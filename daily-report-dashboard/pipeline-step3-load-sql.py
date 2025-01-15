#%pip install azure-storage-blob

import sys
import logging
import json
import pyodbc
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient
from logging.handlers import TimedRotatingFileHandler
import config

# Configure logging
log_filename = f"pipeline3_{datetime.now().strftime('%Y-%m-%d')}.log"
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

# Initialize BlobServiceClient
blob_connection_string = config.AZURE_BLOB_CONN_STR
container_name = config.AZURE_BLOB_CONT_NAME
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

# SQL connection string
sql_connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};{config.SQL_SERVER_DB_UID_PWD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout={config.SQL_CONN_TIMEOUT}'

################################################################################
#### Utility methods

# Function to concatenate name changes
def concatenate_name_changes(name_changes):
    changes_str = ''

    if name_changes:
        try:
            changes_str = ";; ".join(f"{change['former_name']} ({change['date_of_change']})" for change in name_changes)
        except Exception as e:
            logging.error(f'Invalid name_changes dict passed to concat method. Dict: {name_changes}. Error: {e}.')
    
    return changes_str

# Format parsed section text objects
def format_section_parsed_text(section):
    section_type = section['section_type']
    
    if section_type == 'xbrl_note':
        # For xbrl_note, combine header_vals and text_vals into a long string
        headers = section['section_parsed_text'].get('header_vals', [])
        texts = section['section_parsed_text'].get('text_vals', [])
        
        # Format the output string
        formatted_text = []
        
        # Include headers if they exist
        if headers:
            formatted_text.append("Headers:")
            formatted_text.extend(headers)
        
        # Include texts if they exist, joining with newlines
        if texts:
            formatted_text.append("Text:")
            formatted_text.append("\n\n".join(texts))  # Separate each text item with double newlines for clarity
        
        # Join all parts with line breaks for clarity
        return "\n\n".join(formatted_text)
    
    else:
        return section['section_parsed_text']

#### / Utility methods
################################################################################

################################################################################
#### Insertion handlers

def financial_statement_inserter(filing_id, cursor, financial_statements):
    
    for statement in financial_statements:
        cursor.execute('''
            INSERT INTO FinancialReport (filing_id, report_doc, report_name, report_title_read)
            VALUES (?, ?, ?, ?)
        ''', (
            filing_id,
            statement['report_doc'],
            statement['report_name'],
            statement['report_title_read']
        ))

        logging.info('FinancialReport table insert made.')

        # Get the last inserted financial_report_id
        financial_report_id = cursor.execute('SELECT @@IDENTITY').fetchone()[0]

        # Batch financial report facts from report and insert together
        financial_facts_batch = []

        # Load and parse the JSON string
        try:
            report_df_dict = json.loads(statement['report_df'])
            
            # Flatten the dict
            for account_name, metrics in report_df_dict.items():
                for date, value in metrics.items():
                    financial_facts_batch.append((financial_report_id, account_name, date, value))
        except Exception as e:
            logging.warning(f'Empty or invalid data frame for {statement["report_name"]} ({statement["report_doc"]}). Error: {e}.')


        if financial_facts_batch:
            cursor.executemany('''
                INSERT INTO FinancialReportFacts (financial_report_id, account_name, time_period, value)
                VALUES (?, ?, ?, ?)
            ''', financial_facts_batch)

            logging.info('FinancialReportFacts table insert(s) made.')
    return True

def text_section_inserter(filing_id, cursor, text_sections):
    for section in text_sections:

        # Check if the document already exists
        cursor.execute('''
            SELECT text_document_id FROM TextDocument
            WHERE filing_id = ? AND section_doc = ?
        ''', (filing_id, section['section_doc']))
        
        result = cursor.fetchone()

        if result:
            text_document_id = result[0]
        else:
            # Insert if not
            cursor.execute('''
                INSERT INTO TextDocument (filing_id, section_doc)
                VALUES (?, ?)
            ''', (filing_id, section['section_doc']))

            logging.info('TextDocument table insert made.')
            
            text_document_id = cursor.execute('SELECT @@IDENTITY').fetchone()[0]

        # Ensure text is in right format
        parsed_text_str = format_section_parsed_text(section)
        
        # Insert the text section facts
        cursor.execute('''
            INSERT INTO TextSectionFacts (text_document_id, section_name, section_type, section_text)
            VALUES (?, ?, ?, ?)
        ''', (
            text_document_id,
            section['section_name'],
            section['section_type'],
            parsed_text_str
        ))

        logging.info('TextSectionFacts table insert made.')
    return True

def event_info_inserter(filing_id, cursor, event_info):
    # Insert main event info into Event8K table
    cursor.execute('''
        INSERT INTO Event8K (filing_id, event_info)
        VALUES (?, ?)
    ''', (
        filing_id,
        json.dumps({k: v for k, v in event_info.items() if k != 'items_listed'})  # Exclude 'items_listed' for JSON storage
    ))
    
    logging.info('Event8K table insert made.')

    # Get the last inserted event_id
    event_id = cursor.execute('SELECT @@IDENTITY').fetchone()[0]

    # Insert each item in items_listed into Event8KItems table
    if 'items_listed' in event_info:
        items_batch = [(event_id, item) for item in event_info['items_listed']]
        
        cursor.executemany('''
            INSERT INTO Event8KItems (event_id, item_name)
            VALUES (?, ?)
        ''', items_batch)

        logging.info('Event8KItems table insert(s) made.')
    
    return True

def insider_trans_inserter(filing_id, cursor, insider_trans):
    # Insert into Form4IssuerInfo table
    issuer_info = insider_trans['issuer_info']
    cursor.execute('''
        INSERT INTO Form4IssuerInfo (filing_id, issuer_cik, issuer_name, issuer_trading_symbol)
        VALUES (?, ?, ?, ?)
    ''', (
        filing_id,
        issuer_info.get('issuerCik'),
        issuer_info.get('issuerName'),
        issuer_info.get('issuerTradingSymbol')
    ))
    logging.info('Form4IssuerInfo table insert made.')

    # Insert into Form4OwnerInfo table
    for owner in insider_trans['owner_info']:
        cursor.execute('''
            INSERT INTO Form4OwnerInfo (filing_id, owner_cik, owner_name, owner_city, owner_state, is_officer, officer_title)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            filing_id,
            owner.get('ownerCik'),
            owner.get('ownerName'),
            owner.get('ownerCity'),
            owner.get('ownerState'),
            1 if owner.get('isOfficer') else 0,  # Assuming BIT representation for is_officer TODO: CHECK THIS
            owner.get('officerTitle')
        ))
    logging.info('Form4OwnerInfo table insert(s) made.')

    # Insert into Form4NonDerivTransactionInfo table
    for transaction in insider_trans['trans']:
        logging.info(f'Inserting Form4 non derivative transaction: {transaction}')
        cursor.execute('''
            INSERT INTO Form4NonDerivTransactionInfo (filing_id, security_title, transaction_date, transaction_code,
                                                      transaction_shares, transaction_price_per_share,
                                                      transaction_acquired_disposed_code, shares_owned_following_transaction,
                                                      direct_or_indirect_ownership)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            filing_id,
            transaction.get('securityTitle'),
            transaction.get('transactionDate'),
            transaction.get('transactionCode'),
            transaction.get('transactionShares') or None,
            transaction.get('transactionPricePerShare') or None,
            transaction.get('transactionAcquiredDisposedCode'),
            transaction.get('sharesOwnedFollowingTransaction') or None,
            transaction.get('directOrIndirectOwnership')
        ))
    logging.info('Form4NonDerivTransactionInfo table insert(s) made.')

    # Insert into Form4Footnotes table
    for footnote_id, footnote_text in insider_trans['footnotes'].items():
        cursor.execute('''
            INSERT INTO Form4Footnotes (filing_id, footnote_ref_id, footnote_text)
            VALUES (?, ?, ?)
        ''', (
            filing_id,
            footnote_id,
            footnote_text
        ))
    logging.info('Form4Footnotes table insert(s) made.')

    # Insert into Form4SignatureInfo table
    signature = insider_trans['sigs']
    cursor.execute('''
        INSERT INTO Form4SignatureInfo (filing_id, signature_name, signature_date)
        VALUES (?, ?, ?)
    ''', (
        filing_id,
        signature.get('signatureName'),
        signature.get('signatureDate')
    ))
    logging.info('Form4SignatureInfo table insert made.')

    return True

def pdf_data_inserter(filing_id, cursor, pdfs):
    # Insert each PDF document into the PDFDocument table
    for pdf in pdfs:
        # Insert metadata into PDFDocument table
        cursor.execute('''
            INSERT INTO PDFDocument (filing_id, pdf_name, doc_type, metadata)
            VALUES (?, ?, ?, ?)
        ''', (
            filing_id,
            pdf.get('pdf_name'),
            pdf.get('doc_type'),
            str(pdf.get('metadata'))  # Convert metadata dict to string format for storage
        ))
        logging.info(f"PDFDocument insert made for {pdf.get('pdf_name')}.")

        # Retrieve the last inserted PDF ID to link pages in PDFPageText
        pdf_id = cursor.execute("SELECT @@IDENTITY AS pdf_id").fetchval()

        # Insert each page's content into PDFPageText table
        for page in pdf.get('page_content', []):
            cursor.execute('''
                INSERT INTO PDFPageText (pdf_id, page_num, page_text)
                VALUES (?, ?, ?)
            ''', (
                pdf_id,
                page.get('page_num'),
                page.get('page_text')
            ))
        logging.info(f"PDFPageText inserts made for PDF {pdf.get('pdf_name')}.")

    logging.info("All PDF data inserted.")
    return True

def holdings_report_inserter(filing_id, cursor, holdings_report):
    # Insert metadata into HoldingsReport table
    cursor.execute('''
        INSERT INTO HoldingsReport (filing_id, report_yr_quarter, is_amendment, 
                                    amendment_no, amendment_type, filing_mgr_name, 
                                    filing_mgr_addr, report_type, form13f_filenum, 
                                    sec_filenum, info_instruction5, sig_name, 
                                    sig_title, sig_phone, sic_loc, sig_date, 
                                    other_mgrs_count, it_entries_count, it_value_total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        filing_id,
        holdings_report.get('report_yr_quarter'),
        holdings_report.get('amendment', {}).get('is_amendment'),
        holdings_report.get('amendment', {}).get('amendment_no'),
        holdings_report.get('amendment', {}).get('amendment_type'),
        holdings_report.get('filing_mgr_name'),
        holdings_report.get('filing_mgr_addr'),
        holdings_report.get('report_type'),
        holdings_report.get('form13f_filenum'),
        holdings_report.get('sec_filenum'),
        holdings_report.get('info_instruction5'),
        holdings_report.get('sig_name'),
        holdings_report.get('sig_title'),
        holdings_report.get('sig_phone'),
        holdings_report.get('sic_loc'),
        holdings_report.get('sig_date'),
        holdings_report.get('other_mgrs_count') or None,
        holdings_report.get('it_entries_count') or None,
        holdings_report.get('it_value_total') or None
    ))
    logging.info("HoldingsReport insert made.")

    # Retrieve the last inserted HoldingsReport ID
    holdings_report_id = cursor.execute("SELECT @@IDENTITY AS holdings_report_id").fetchval()

    # Insert Other Managers data using executemany
    other_managers_batch = [
        (
            holdings_report_id,
            manager.get('mgr_seq') or None,
            manager.get('mgr_cik'),
            manager.get('mgr_13f_filenum'),
            manager.get('mgr_sec_filenum'),
            manager.get('mgr_crd_num'),
            manager.get('mgr_name')
        )
        for manager in holdings_report.get('other_mgrs', [])
    ]

    if other_managers_batch:
        cursor.executemany('''
            INSERT INTO OtherManagers (holdings_report_id, mgr_seq, mgr_cik, 
                                       mgr_13f_filenum, mgr_sec_filenum, 
                                       mgr_crd_num, mgr_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', other_managers_batch)
        logging.info(f"{len(other_managers_batch)} OtherManagers insert(s) made.")

    # Insert Holdings Entries data using executemany
    holdings_entries_batch = [
        (
            holdings_report_id,
            entry.get('issuer'),
            entry.get('class'),
            entry.get('cusip'),
            entry.get('value') or None,
            entry.get('amount'),
            entry.get('amt_type'),
            entry.get('discretion'),
            entry.get('sole_vote'),  
            entry.get('shared_vote'),
            entry.get('no_vote'),
            entry.get('figi'),
            entry.get('other_mgr'),
            entry.get('option_type')
        )
        for entry in holdings_report.get('it_entries', [])
    ]

    if holdings_entries_batch:
        cursor.executemany('''
            INSERT INTO HoldingsEntries (holdings_report_id, issuer, class, 
                                         cusip, value, amount, amt_type, 
                                         discretion, sole_vote, shared_vote, 
                                         no_vote, figi, other_manager, 
                                         option_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', holdings_entries_batch)
        logging.info(f"{len(holdings_entries_batch)} HoldingsEntries insert(s) made.")

    logging.info("All holdings report data inserted.")
    return True

#### / Insertion handlers
################################################################################

# Chooses correct insertion handler
def insert_parsed_filing_data(filing_id, cursor, parsed_data_type, parsed_object):
    match parsed_data_type:
        case 'financial_statements':
            return financial_statement_inserter(filing_id, cursor, parsed_object)
        case 'text_sections':
            return text_section_inserter(filing_id, cursor, parsed_object)
        case 'event_info':
            return event_info_inserter(filing_id, cursor, parsed_object)
        case 'insider_trans':
            return insider_trans_inserter(filing_id, cursor, parsed_object)
        case 'pdfs':
            return pdf_data_inserter(filing_id, cursor, parsed_object)
        case 'holdings_report':
            return holdings_report_inserter(filing_id, cursor, parsed_object)
        case _:
            logging.warning(f'Unknown data type...: {parsed_data_type}. Don\'t know how to parse + insert.')
            return True

# Orchestrates insertion of filing data to the DB
def insert_filing_data(conn, filing_json):
    try:
        conn.autocommit = False
        filing_info = filing_json['filing_info']
    except Exception as e:
        logging.error(f'Met in invalid filing_json object in insert_filing_data(). Error: {e}.')
        return False
    
    # Concat name changes
    name_changes_str = concatenate_name_changes(filing_info['name_changes'])
    
    # Get a list of parsed data type keys
    parsed_data_types = [key for key in filing_json.keys() if key != 'filing_info']

    # Retry logic
    for attempt in range(config.MAX_SQL_INSERT_RETRIES):
        
        logging.info(f'Insert attempt {attempt + 1} of {config.MAX_SQL_INSERT_RETRIES} beginning.')

        try:
            # Create a cursor from the connection
            with conn.cursor() as cursor:

                # Check if the filing already exists in MasterFiling table
                cursor.execute('''
                    SELECT filing_id FROM MasterFiling
                    WHERE cik = ? AND accession_number = ?
                ''', (filing_info['cik'], filing_info['accession_number']))
                
                result = cursor.fetchone()

                if result:
                    # Skip if already exists
                    logging.warning(f'Filing has already been loaded to the DB, according the MasterFiling. Skipping, CIK: {filing_info["cik"]}, ASCN: {filing_info["accession_number"]}.')
                    return True
                else:
                    # Insert filing info if not, and handle other data in specialized methods
                    cursor.execute('''
                        INSERT INTO MasterFiling (cik, type, date, accession_number, company_name, sic_code, sic_desc,
                                                    report_period, state_of_incorp, fiscal_yr_end, business_address,
                                                    business_phone, name_changes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        filing_info['cik'],
                        filing_info['type'],
                        datetime.strptime(filing_info['date'], '%Y%m%d').date(),
                        filing_info['accession_number'],
                        filing_info['company_name'],
                        filing_info['sic_code'],
                        filing_info['sic_desc'],
                        filing_info['report_period'],
                        filing_info['state_of_incorp'],
                        filing_info['fiscal_yr_end'],
                        filing_info['business_address'],
                        filing_info['business_phone'],
                        name_changes_str,
                    ))

                    logging.info('MasterFiling table insert made.')

                    # Get the last inserted filing_id
                    filing_id = cursor.execute('SELECT @@IDENTITY').fetchone()[0]

                # Loop through other parsed data, inserting into appropriate tables
                for key in parsed_data_types:
                    if not insert_parsed_filing_data(filing_id, cursor, key, filing_json[key]):
                        logging.error(f'Failed to insert parsed filing data type: {key}.')
                        # Kick back to our retry logic
                        raise Exception(f'Failed to insert parsed filing data type: {key}.') 
                    
                # Commit the transaction
                conn.commit()
                logging.info(f'Transaction committed on attempt {attempt + 1}.')
                return True
                
        except Exception as e:
            logging.error(f'Insert attempt {attempt + 1} failed. Error: {e}.')
            conn.rollback()
            
            if attempt < config.MAX_SQL_INSERT_RETRIES - 1:
                logging.warning('Retrying insert after sleep.')
                time.sleep(2)
            else:
                logging.error('Insert retries exhausted.')
                return False

    return False

def load_parsed_day_for_analysis(target_date):
    """Main logic here. Processes a parsed filing JSON object and inserts its data into our DB"""

    if type(target_date) not in [date, datetime]:
        logging.error('Invalid target_date type. Must be a date or datetime object.')
        raise Exception('Invalid target_date type. Must be a date or datetime object.')

    # Build our subfolder path for the day's filings
    parsed_filings_folder = f'parsed_filings/{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}/'

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # List blobs in the specified folder
    blob_list = list(container_client.list_blobs(name_starts_with=parsed_filings_folder))

    # Filter blobs to only count JSON files
    json_blobs = [blob for blob in blob_list if blob.name.endswith('.json')]

    # Count the number of blobs
    total_blob_count = len(json_blobs)
    logging.info(f'Number of blobs found in {parsed_filings_folder} is: {total_blob_count}.')

    # Connect to SQL db
    for connect_attempt in range(config.MAX_SQL_LOGIN_RETRIES):
        try:
            conn = pyodbc.connect(sql_connection_string)
            logging.info('Connected to SQL db.')

        # Retry if appropriate. Azure SQL db connection can be finicky, so worth implementing retry logic
        except Exception as e:
            logging.error(f'Failed connecting to SQL db. Error: {e}')

            if connect_attempt < config.MAX_SQL_LOGIN_RETRIES - 1:
                logging.warning('Retrying connection after sleep.')
                time.sleep(2)
            else:
                logging.error('Connection retries exhausted.')
                raise Exception('Connection retries exhausted.')

        else:
                if not conn:
                    logging.error('Connection to SQL DB was not properly established.')
                    raise Exception('Connection to SQL DB was not properly established.')

                # TODO: Clear tables of previous day's data.... decide whether to archive etc
                
                # Iterate parsed filings
                inserted_filings_count = 0
                for count, blob in enumerate(json_blobs):

                    # Download the blob content
                    blob_client = container_client.get_blob_client(blob.name)
                    content = blob_client.download_blob().readall().decode('utf-8')
                    content_dict = json.loads(content)

                    logging.info(f'Attempting to load filing data to SQL db: {blob.name}\nBlob number {inserted_filings_count + 1} out of {total_blob_count}.')

                    if (insert_filing_data(conn, content_dict)):
                        inserted_filings_count += 1
                        logging.info(f'Inserted filing data #{inserted_filings_count + 1} to DB. {blob.name}.')
                    else:
                        logging.error(f'Failed to load filing data to database. #{inserted_filings_count + 1}.')

                logging.info('Finished iterating parsed filings.')
                conn.close()
                break
                # TODO: Possibly parse daily index file and insert basic info of unparsed filings to MasterFiling for stats. For now am just reading from CSV in flask app

"""Real entrypoint, after config stuff at top"""
logging.info('Starting pipeline step 3 (JSON -> SQL loading) workflow.')

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
    # NOTE: Adjust timezone settings as needed (possible disrepancy between timezone your function app is provisioned in and other resources such as ADF timers)
    target_date = datetime.now(ZoneInfo("America/Phoenix")).date()
    logging.info('Target date set to today: {target_date}.')

# TODO: Finish handling weekends and system holidays
if target_date.weekday() in (5, 6):
    logging.info('Target date is a Saturday or Sunday, no parsed filings to load.')
    logging.info('Pipeline step 3 (JSON -> SQL loading) workflow completed.')
    exit()

# Call main worker method
load_parsed_day_for_analysis(target_date)

logging.info('Pipeline step 3 (JSON -> SQL loading) workflow completed.')
