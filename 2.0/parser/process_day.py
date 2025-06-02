
import re
import os
import logging
import pandas as pd
from datetime import date, datetime
from prefect import flow

import config.settings as settings
from config.log_config import config_logging
import conn.sec_http as sec_http
from conn.db_engine import engine
from conn.setup_db import create_db_tables
import parser.filing_parser as filing_parser
from ingest.ingest_logic import ingest_dataframe
from ingest.text_embedding import embed_new_text_sections

def get_quarter_from_date(date_obj):
    '''
    Support method used below. Given a date object, determines the quarter number.
    Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec (month 1-3, 4-6, 7-9, 10-12)
    '''
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

def parse_idx_to_dicts(index_text):
    ''' 
    Parses a given index file.
    Returns a list of dictionaries of the following structure:
    {
    "cik" : "CIK_NUM",
    "company" : "COMPANY_NAME",
    "type" : "FILING_TYPE",
    "date" : "YYYYMMDD",
    "fulltext_path" : ".../edgar/data/CIK/ETC"
    }
    '''
    logging.info(f'Attempting to build list of dictionaries from daily index raw text.')

    filings_list = []
    base_archives_url = "https://www.sec.gov/Archives/"
    split_idx = index_text.split("--------------------\n")

    # Loop through lines of data
    try:
        for i, line in enumerate(split_idx[1].splitlines()):
            logging.info(f'Processing line {i+1} of index file.')

            # CIK|Company Name|Form Type|Date Filed|Filename
            # We expect 5 columns
            columns = line.split("|")
            if len(columns) == 5:

                # Build a dictionary for the filing if we find match
                found_filing = {}
                found_filing["cik"] = columns[0].zfill(10)
                found_filing["company"] = columns[1]
                found_filing["type"] = columns[2]
                found_filing["date"] = columns[3]
                found_filing["fulltext_path"] = base_archives_url + columns[4]

                # Append it
                filings_list.append(found_filing)
                logging.info(f'Successfully created a dictionary from line {i+1} of index file.')
            else:
                logging.error(f'Index file line did not have 5 columns as expected. Cannot parse it to a dictionary.\nLine in question: {line}')
    
    except Exception as e:
        logging.error(f'IDX file was in unexpected format, error parsing: {e}.')
        raise Exception(f'IDX file was in unexpected format, error parsing: {e}.')

    logging.info(f'Parsed all lines of index file.')
    return filings_list

def get_daily_index_df(target_date, rate_limiter):
    '''
    Parses and returns the specified daily as a pandas dataframe, building full URLs to filings and creating an accession_number column extracted from the URL.
    '''
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
    logging.info(f'Built path to daily index targetting {target_date}\n{daily_index_url}')

    # Get the raw file
    raw_idx_text = sec_http.download_file_from_sec(daily_index_url, rate_limiter)
    if raw_idx_text:
        logging.info(f'Successfully downloaded daily index for {target_date}')
        # Convert it to a dataframe (intermediately, a list of dictionaries)
        idx_dicts = parse_idx_to_dicts(raw_idx_text)
        if idx_dicts:
            logging.info(f'Parsed index into dictionaries, attempting to convert to a dataframe.')
            idx_df = pd.DataFrame(idx_dicts)
            idx_df['accession_number'] = idx_df['fulltext_path'].str.extract(r'/(\d{10}-\d{2}-\d{6})\.txt')
            logging.info(f'Extracted accession number column from fulltext_url of index dataframe.')
            return idx_df
        else:
            logging.error(f'Failed to convert daily index to dictionary for final conversation to a dataframe.')
            return pd.DataFrame()
    else:
        logging.error(f'Failed to download daily index for {target_date} based on URL: {daily_index_url}.')
        return pd.DataFrame()

def process_filing(fulltext_url, type, rate_limiter):
    '''
    Processes an SEC filing of the specified type from its fulltext URL.

    Returns: a dictionary with one key/value pair per dataframe generated by processing the filing. Values/structure depends on filing type
    '''
    logging.info(f'Processing filing of type {type}.')
    raw_contents = sec_http.download_file_from_sec(fulltext_url, rate_limiter)
    if raw_contents:
        logging.info(f'Downloaded filing contents.')

        parser = filing_parser.MasterParserClass(raw_contents, type)
        logging.info(f"Parsing {type} filing {parser.filing_info['accession_number'].iloc[0]}.")

        return parser.output_dfs() 
    else:
        logging.error(f'Failed to download filing contents, unable to process.')
        return None

def aggregate_parsed_dfs(list_of_parsed, filing_type):
    '''
    Builds the appropriate aggregated dataframes based on filing type.

    Takes a list of dictionaries, each representing a parsed filing, all of the specified type.
    Returns a single dictionary containing multiple dataframes depending on filing type as described above 
        (For exampe, for 10-Q filings, the returned dictionary would have 4 dataframes: filing info, named sections, exhibits, and pdfs.)
    '''
    logging.info(f'Attempting to aggregate dataframes of {filing_type} filings.')
    concatted_dfs = {}

    # Create lists of each type of dataframe present 
    for i, filing_dfs in enumerate(list_of_parsed):
        logging.info(f'Parsing {i+1}/{len(list_of_parsed)} dictionary of dataframes for {filing_type}.')
        for df_type, indiv_filing_df in filing_dfs.items():
            logging.info(f'Appending {df_type} dataframe to appropriate list in concatted_dfs.')
            concatted_dfs.setdefault(df_type, []).append(indiv_filing_df)

    # Now actually concat the DF's 
    agged_dfs = {}
    for df_type, df_list in concatted_dfs.items():
        logging.info(f'Concatenating {df_type} dataframes for {filing_type} filings.')
        combined_df = pd.concat(df_list, ignore_index=True)
        agged_dfs[df_type] = combined_df
        logging.info(f'Built aggregated dataframe of type {df_type} for filings of type {filing_type}.')

    return agged_dfs

def sanitize_filing_type(filing_type):
    '''
    Sanitize filing type before use as a folder (avoid /A filings created subdirs etc).
    '''
    
    # Replace unsafe characters
    safe_filing_type = filing_type.replace('/', '_')  # Replace '/' with '_'
    safe_filing_type = safe_filing_type.replace(' ', '_')  # Replace spaces with '_'
    safe_filing_type = re.sub(r'[<>:"\\|?*]', '_', safe_filing_type)  # Replace other unsafe characters
    return safe_filing_type

def save_parsed_type(agged_dfs, date, form_type):
    '''
    Takes a dictionary of aggregated dataframes, together representing the parsed data from all filings of the given type, 
    and saves them appropriately. 
    '''
    # For now, quick and dirty
    for df_type, df in agged_dfs.items():
        df_save_path = f'{settings.STORAGE_DIR}/{date}/{sanitize_filing_type(form_type)}/{df_type}.csv'
        dir = os.path.dirname(df_save_path)
        if dir:
            os.makedirs(dir, exist_ok=True) 
        df.to_csv(df_save_path)
        logging.info(f'Saved {df_type} for {form_type} filings to: {df_save_path}')

def full_process_day(target_date: date | datetime):
    '''
    Run the full pipeline. 
    '''
    logging.info(f'Running full processing pipeline for date: {target_date}.')

    # Create SEC rate limiter object
    rate_limiter = sec_http.create_sec_rate_limiter()

    # Enable pgvector and create tables as needed
    create_db_tables()

    # Get index file
    idx_df = get_daily_index_df(target_date, rate_limiter)
    if idx_df is not None and not idx_df.empty:
        logging.info(f'Successfully parsed daily index file to a dataframe. Number of entries: {len(idx_df)}.')
        idx_df = idx_df.drop_duplicates(subset='accession_number')
        logging.info(f'Removed duplicate filings based on accession number. New number of entries: {len(idx_df)}.')

        idx_save_path = f'{settings.STORAGE_DIR}/{target_date}/idx.csv'
        idx_dir = os.path.dirname(idx_save_path)
        if idx_dir:
            os.makedirs(idx_dir, exist_ok=True)
        idx_df.to_csv(idx_save_path) # TODO: Maybe save IDX info in SQL?

        # Process filings one target type at a time
        target_filing_types = settings.TARGET_FILING_TYPES
        for type in target_filing_types:
            type_filtered_idx = idx_df[idx_df['type'].str.lower() == type.lower()]
            logging.info(f'Parsing {len(type_filtered_idx)} {type} filings.')

            # TODO: Implement parallel processing, maybe 5 filings at a time...

            parsed_of_current_type = []
            for i, row in type_filtered_idx.iterrows():

                logging.info(f'Processing idx row {i}: {row.fulltext_path}')
                parsed_filing_data = process_filing(row.fulltext_path, type.lower(), rate_limiter)
                if parsed_filing_data:
                    logging.info(f'Successfully processed {type} filing.')
                    parsed_of_current_type.append(parsed_filing_data)
                else:
                    logging.error(f'Failed to process {type} filing.')

            logging.info(f'Finished parsing {len(type_filtered_idx)} {type} filings. Attempting to ingest into SQL DB.')
            # Organize and concatenate our dataframes for the current filing type
            type_specific_dfs = aggregate_parsed_dfs(parsed_of_current_type, type)

            #save_parsed_type(type_specific_dfs, target_date, type) # For testing

            for df_name, df in type_specific_dfs.items():
                if not df.empty:
                    logging.info(f"Ingesting {df_name} from {type} filings.")
                    ingest_dataframe(df, df_name, engine)
                else:
                    logging.warning(f'Was no data in {df_name} from {type} filings, nothing to insert.')
            logging.info(f'Ingested parsed data from all filings of type: {type}.')

        logging.info(f'Processed all {target_date} filings of target types: {target_filing_types}. Now generating embeddings of text sections')

        embed_new_text_sections(engine)
        logging.info(f'Finished embedding new text sections and rebuilding indexes for vector search.')

        logging.info(f'Finished processing {target_date}.')
    else:
        logging.error(f'Failed to get dataframe of daily index for date: {target_date}\nUnable to process any filings.')

@flow(name=settings.PREFECT_FLOW_NAME)
def full_process_day_flow(target_date: str):
    config_logging('ingestion_flow')
    date = datetime.strptime(target_date, "%Y-%m-%d").date()
    return full_process_day(date)