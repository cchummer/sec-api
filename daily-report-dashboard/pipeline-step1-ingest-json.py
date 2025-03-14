#%pip install aiohttp
#%pip install azure-storage-blob

import logging
import sys
import os
import requests
import re
import math
import json
from time import time
from datetime import date, datetime
from zoneinfo import ZoneInfo
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient
from logging.handlers import TimedRotatingFileHandler
from collections import deque
import aiohttp
import asyncio
import config
from filing_parsers import parse_raw_filing

# Configure logging directory and filename
script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
log_directory = os.path.join(script_dir, 'logs')  
os.makedirs(log_directory, exist_ok=True)  # Ensure the directory exists

# Use the current date from sys.argv[1] if provided, or default to today
current_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')

log_filename = os.path.join(log_directory, f"pipeline1_{current_date}.log")
log_handler = TimedRotatingFileHandler(
    filename=log_filename,
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8'
)

# Set log formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# Set up the root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        log_handler,  # File handler for logs
        logging.StreamHandler(sys.stdout)  # Console handler for logs
    ]
)

logging.info(f"Logging initialized with TimedRotatingFileHandler with date {current_date}.")

# NOTE: We sanitize filing type names for use as folders before creating the CSV; namely, slashes+spaces are replaced with underscores: S-1/A -> S-1_A 
filing_types_targets = config.FILING_TYPES_TARGETS

filers_targets = config.FILERS_TARGETS # Potential list of CIKs 

# Custom user agent for SEC requests
sec_req_headers = config.EDGAR_REQ_HEADERS

# Initialize BlobServiceClient
connection_string = config.AZURE_BLOB_CONN_STR
container_name = config.AZURE_BLOB_CONT_NAME
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Helper functions

# Support method used below. Given a date object, determines the quarter number.
# Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec (month 1-3, 4-6, 7-9, 10-12)
def get_quarter_from_date(date_obj):
    if date_obj.month >= 1 and date_obj.month <= 3:
        return 1
    elif date_obj.month >= 4 and date_obj.month <= 6:
        return 2
    elif date_obj.month >= 7 and date_obj.month <= 9:
        return 3
    elif date_obj.month >= 10 and date_obj.month <= 12:
        return 4
    else:
        logging.error('Failed to determine quarter when building index file path.')
        raise Exception('Failed to determine quarter when building index file path.')

# Given a date/datetime object, returns the contents of the SEC daily index file
def pull_filtered_daily_index(target_date):
  
    # Verify that target_date is of the right type
    if type(target_date) not in [date, datetime]:
        logging.error('Invalid target date format when building index file path (not a date object).')
        raise Exception('Invalid target date format when building index file path (not a date object).')

    # Build path
    daily_index_url = r"https://www.sec.gov/Archives/edgar/daily-index/{}/QTR{}/master.{}{}{}.idx".format(
        target_date.year,
        get_quarter_from_date(target_date),
        target_date.year,
        str(target_date.month).zfill(2),
        str(target_date.day).zfill(2),
    )  # master.YYYYMMDD.idx

    # Get the file
    resp = None
    try:
        resp = requests.get(url=daily_index_url, headers=sec_req_headers)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logging.error(f'{str(e)}, Failed GET request for daily index: {daily_index_url}')
        raise Exception(f'{str(e)}, Failed GET request for daily index: {daily_index_url}')
    
# Sanitize filing type before use as a folder (avoid /A filings created subdirs etc)
def sanitize_filing_type(filing_type):
    # Replace unsafe characters
    safe_filing_type = filing_type.replace('/', '_')  # Replace '/' with '_'
    safe_filing_type = safe_filing_type.replace(' ', '_')  # Replace spaces with '_'
    safe_filing_type = re.sub(r'[<>:"\\|?*]', '_', safe_filing_type)  # Replace other unsafe characters
    return safe_filing_type
    
''' 
Parses a given index file.
Returns a list of dictionaries of the following structure:
{
  "cik" : "CIK_NUM",
  "company" : "COMPANY_NAME",
  "type" : "SANI_FILING_TYPE",
  "date" : "YYYYMMDD",
  "fulltext_path" : ".../edgar/data/CIK/ETC"
}
'''
def parse_idx(index_text):

    filings_list = []
    base_archives_url = "https://www.sec.gov/Archives/"
    split_idx = index_text.split("--------------------\n")

    # Loop through lines of data
    try:
        for line in split_idx[1].splitlines():

            # CIK|Company Name|Form Type|Date Filed|Filename
            # We expect 5 columns
            columns = line.split("|")
            if len(columns) == 5:

                # Build a dictionary for the filing if we find match
                found_filing = {}
                found_filing["cik"] = columns[0].zfill(10)
                found_filing["company"] = columns[1]
                found_filing["type"] = sanitize_filing_type(columns[2])
                found_filing["date"] = columns[3]
                found_filing["fulltext_path"] = base_archives_url + columns[4]

                # Append it
                filings_list.append(found_filing)
    
    except Exception as e:
        logging.error(f'IDX file was in unexpected format, error parsing: {e}.')
        raise Exception(f'IDX file was in unexpected format, error parsing: {e}.')

    return filings_list

# Method for converting the list of dictionaries returned by parse_idx to CSV format
def convert_json_list_csv(json_list):
    try:
        return pd.DataFrame(json_list).to_csv(index=False)
    
    except Exception as e:
        logging.error(f'Failed to convert parsed index to CSV format. Error: {str(e)}.')
        raise Exception(f'Failed to convert parsed index to CSV format. Error: {str(e)}.')
        
# Rate limiter using a token bucket algorithm
class RateLimiter:
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit  # Requests per second
        self.tokens = deque()  # Timestamps of recent requests
        self.request_count = 0 # For logging

    async def acquire(self):
        while True:
            now = time()
            # Remove tokens older than 1 second
            while self.tokens and now - self.tokens[0] >= 1:
                self.tokens.popleft()
            # If we have space for a new token, add it and proceed
            if len(self.tokens) < self.rate_limit:
                self.tokens.append(now)
                self.request_count += 1
                logging.info(f"Request {self.request_count} at {now}")
                break
            # Otherwise, wait until the next token is available
            await asyncio.sleep(1 / self.rate_limit)
# Global rate limiter instance
rate_limiter = RateLimiter(config.SEC_RATE_LIMIT)

async def fetch_filing_contents(filing_url, session, max_retries=3, base_delay=1.0):
    """Fetch filing contents while respecting SEC rate limit and implementing retry logic."""
    logging.info(f'Attempting to fetch filing contents: {filing_url}.')
    
    await rate_limiter.acquire()  # Ensure we're within the rate limit

    for attempt in range(max_retries):
        try:
            async with session.get(filing_url, headers=sec_req_headers) as response:
                if response.status == 200:
                    logging.info(f'Successfully fetched filing contents: {filing_url}.')
                    return await response.text()
                else:
                    logging.warning(f"Attempt {attempt+1}: Failed to fetch {filing_url} - Status {response.status}. Retrying...")
        
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as e:
            logging.warning(f"Attempt {attempt+1}: Network error fetching {filing_url}: {e}. Retrying...")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(base_delay * (2 ** attempt))  # Exponential backoff

    logging.error(f"Failed to fetch {filing_url} after {max_retries} attempts.")
    return None


def parse_and_save_filing(filing_contents, blob_name, container_client):
    '''
    Parses and saves the given filing to the given blob name in the passed container.
    '''
    logging.info(f'Attempting to parse filing and upload to blob: {blob_name}.')

    parsed_filing = parse_raw_filing(filing_contents)

    # Serialize the filing_data to JSON
    json_data = json.dumps(parsed_filing)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json_data, overwrite=True)

async def process_filings_in_batches(target_filings, container_client, batch_size=100):
    '''
    Grabs the contents of, parses into JSON objects, and saves to blob storage the given list of 
    filings, each in a dict with keys:
        'blob_name': the blob name in the container to save the parsed results into
        'filing_url': the URL of the filing to request + parse

        Returns a tuple of lists of dicts, ([{...},...], [{...},...]) first being successfully parsed target_filings and second being failed.
    '''
    total_batches = math.ceil(len(target_filings) / batch_size)
    logging.info(f'Attempting to process {len(target_filings)} filings in batches of {batch_size} ({total_batches} batches).')
    success_parsed = []
    failed_parsed = []

    async with aiohttp.ClientSession() as session:
        with ThreadPoolExecutor(max_workers=5) as executor:
            logging.info('aiohttp session and ThreadPoolExecutor secured.')

            for i in range(0, len(target_filings), batch_size):
                logging.info(f'Processing batch {(i // batch_size) + 1}/{total_batches}.')

                batch = target_filings[i:i+batch_size]
                fetch_tasks = []
                save_tasks = []

                # Fetch/download the batch
                success_fetched = []
                for filing in batch:
                    fetch_tasks.append(fetch_filing_contents(filing.get('filing_url'), session))# Nothing is launched yet
                fetch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True) # Launch tasks and wait before continuing

                logging.info(f'Downloaded contents of batch {(i // batch_size) + 1}/{total_batches}.')
                
                # Parse and save it
                for res_i, result in enumerate(fetch_results):
                    if isinstance(result, Exception):
                        logging.error(f"Failed to fetch filing: {batch[res_i].get('filing_url')}, unexpected error: {result}.")
                        failed_parsed.append(batch[res_i].get('filing_url'))
                    elif isinstance(result, str):
                        logging.info(f"Successfully fetched filing: {batch[res_i].get('filing_url')}.")
                        success_fetched.append(batch[res_i])
                        save_tasks.append(
                            asyncio.get_event_loop().run_in_executor( # Tasks are launched at this point when using ThreadPoolExecutor
                                executor, parse_and_save_filing, result, batch[res_i].get('blob_name'), container_client
                            )
                        )
                    else:
                        logging.error(f"Unexpected return value from fetch_filing_contents for filing: {batch[res_i].get('filing_url')}. Cannot parse.")
                        failed_parsed.append(batch[res_i].get('filing_url'))
                parse_results = await asyncio.gather(*save_tasks, return_exceptions=True) # Wait before continuing

                for res_i, result in enumerate(parse_results):
                    if isinstance(result, Exception):
                        logging.error(f"Failed to parse and save filing: {success_fetched[res_i].get('filing_url')}, unexpected error: {result}.")
                        failed_parsed.append(success_fetched[res_i].get('filing_url'))
                    else:
                        logging.info(f"Successfully parsed filing: {success_fetched[res_i].get('filing_url')}.")
                        success_parsed.append(success_fetched[res_i].get('filing_url'))

                logging.info(f'Finished processing batch {(i // batch_size) + 1}/{total_batches}.')

            logging.info(f'Finished processing filing batches.')

    return success_parsed, failed_parsed

"""Main workflow / entrypoint"""
async def main():
    logging.info('Starting pipeline step 1 (raw filing ingestion) workflow.')
    t0 = time()

    # Get the target date if passed
    target_date_str = sys.argv[1] if len(sys.argv) > 1 else None

    # Set target date accordingly
    if target_date_str:
        try:
            # Attempt to parse the target_date from the string
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
            logging.info(f'Target date read from parameter: {target_date_str}.')
        except ValueError:
            raise ValueError('Invalid date format for target_date. Please use YYYY-MM-DD.')           
    else:
        # Default to today's date if no target_date is provided
        target_date = datetime.now(ZoneInfo("America/Phoenix")).date()
        logging.info(f'Target date set to today: {target_date}.')

    # Skip weekends
    if target_date.weekday() in (5, 6):
        logging.info('Target date is a Saturday or Sunday, no filings to grab.')
        logging.info('Pipeline step 1 (raw filing ingestion) completed successfully.')
        return

    # Step 1: Fetch the daily index file
    index_text = pull_filtered_daily_index(target_date)
    if not index_text:
        raise ValueError(f"Failed to fetch daily index file for date: {target_date}.")

    # Step 2: Upload the raw index file to blob storage
    blob_name = f"idxs/{target_date.year}/{target_date.strftime('%m')}/master.{target_date.strftime('%Y%m%d')}.idx"
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    blob_client.upload_blob(index_text, overwrite=True)
    logging.info(f"Raw index file uploaded: {blob_name}")

    # Step 3: Parse the index file
    found_filings = parse_idx(index_text)
    logging.info('Successfully parsed daily idx file.')

    # Step 4: Upload parsed CSV
    csv_data = convert_json_list_csv(found_filings)
    if csv_data:
        csv_blob_name = f"idxs/{target_date.year}/{target_date.strftime('%m')}/{target_date.strftime('%Y%m%d')}.csv"
        blob_client = blob_service_client.get_blob_client(container_name, csv_blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f'Successfully parsed idx to csv and uploaded to {csv_blob_name}.')

    # Step 5: Download + parse filings
    container_client = blob_service_client.get_container_client(container_name)
    if container_client:
        logging.info(f'Got container handle for filing downloads.')

        # Will hold a list of filings to parse and the blobs the save them in
        filings_to_parse = []
        for filing_idx, filing in enumerate(found_filings):
            # Filter by filing type. Could also filter by filer here
            if filing['type'].lower() in filing_types_targets:

                # Save unique blob name + URL for each filing
                target_filing = {
                    'blob_name': f"filings/{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}/{filing['type']}/{filing['cik']}/{filing['fulltext_path'].split('/')[-1].replace('.txt','.json')}",
                    'filing_url': filing['fulltext_path']
                }
                filings_to_parse.append(target_filing)
                logging.info(f'Identified filing to parse (#{len(filings_to_parse)}): {target_filing["filing_url"]}.')

        # Process (get contents -> parse -> save) in batches of 100 filings
        if filings_to_parse:
            success_filings, failed_filings = await process_filings_in_batches(filings_to_parse, container_client)
            logging.info(f'Finished processing filings. Successfully processed: {len(success_filings)}.')
            if failed_filings:
                logging.error(f'Failed to process: {len(failed_filings)} filings:\n{failed_filings}')
            else:
                logging.info('No failed filings.')
        else:
            logging.warning(f'Identified no filings to parse for day {target_date} via filtering by filing type. Suspicious... Check daily index file.')

    else:
        raise ValueError(f'Failed to get container client handle for container: {container_name}.')

    logging.info(f'Pipeline step 1 (raw filing ingestion) completed successfully. Time elapsed: {(time() - t0):.3f}s.')

if __name__ == "__main__":
    asyncio.run(main())
