#%pip install aiohttp
#%pip install azure-storage-blob

import logging
import sys
import requests
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo
import pandas as pd
from azure.storage.blob import BlobServiceClient
from logging.handlers import TimedRotatingFileHandler
import aiohttp
import asyncio
import config

# Configure logging
log_filename = f"pipeline1_{datetime.now().strftime('%Y-%m-%d')}.log"
log_handler = TimedRotatingFileHandler(
    filename=log_filename,
    when='midnight',   # Rotate logs daily at midnight
    interval=1,        # Interval is 1 day
    backupCount=7,     # Keep the last 7 log files
    encoding='utf-8'   # Encoding for the log file
)

# Set log formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# Set up the root logger
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level
    handlers=[
        log_handler,      # File handler for logs
        logging.StreamHandler(sys.stdout)  # Console handler for logs
    ]
)

logging.info("Logging initialized with TimedRotatingFileHandler.")

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

# Asynchronous function to download filings
async def download_filing(session, url, blob_name, semaphore):
    logging.info(f'Attempting to save filing: {url}.')
    async with semaphore:
        try:
            await asyncio.sleep(0.5) # Throttle. SEC says 10 req's / sec. We are running 5 'threads' of this function at once, thus 2 batches / sec max...
            async with session.get(url, headers=sec_req_headers) as response:

                response.raise_for_status()
                content = await response.read()

                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob_name)

                blob_client.upload_blob(content, overwrite=True)
                logging.info(f"Filing saved: {blob_name}.")
        
        except Exception as e:
            logging.error(f"Failed to download or save filing: {url}, Error: {str(e)}")
            raise Exception(f"Failed to download or save filing: {url}, Error: {str(e)}")

"""Main workflow / entrypoint"""
async def main():
    logging.info('Starting pipeline step 1 (raw filing ingestion) workflow.')

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
        raise ValueError("Failed to fetch daily index file.")

    # Step 2: Upload the raw index file to blob storage
    blob_name = f"idxs/{target_date.year}/{target_date.strftime('%m')}/master.{target_date.strftime('%Y%m%d')}.idx"
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    blob_client.upload_blob(index_text, overwrite=True)
    logging.info(f"Raw index file uploaded: {blob_name}")

    # Step 3: Parse the index file
    parsed_filings = parse_idx(index_text)
    logging.info('Successfully parsed daily idx file.')

    # Step 4: Upload parsed CSV
    csv_data = convert_json_list_csv(parsed_filings)
    if csv_data:
        csv_blob_name = f"idxs/{target_date.year}/{target_date.strftime('%m')}/{target_date.strftime('%Y%m%d')}.csv"
        blob_client = blob_service_client.get_blob_client(container_name, csv_blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f'Successfully parsed idx to csv and uploaded to {csv_blob_name}.')

    # Step 5: Download filings
    semaphore = asyncio.Semaphore(5)  # 5 downloads concurrently
    async with aiohttp.ClientSession() as session:
        tasks = []
        for filing in parsed_filings:
            # Filter by filing type. Could also filter by filer here
            if filing['type'].lower() in filing_types_targets:
                # Create a unique blob name for each filing
                blob_name = f"filings/{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}/{filing['type']}/{filing['cik']}/{filing['fulltext_path'].split('/')[-1]}"
                tasks.append(download_filing(session, filing['fulltext_path'], blob_name, semaphore))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.warning(f"Failed to download/save filing: {result}.")

    logging.info('Pipeline step 1 (raw filing ingestion) completed successfully.')

if __name__ == "__main__":
    asyncio.run(main())
