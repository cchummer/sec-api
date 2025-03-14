#%pip install azure-storage-blob
#%pip install sqlalchemy

import sys
import os
import logging
import json
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ServiceRequestError, ClientAuthenticationError
from logging.handlers import TimedRotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import config

# Configure logging directory and filename
script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
log_directory = os.path.join(script_dir, 'logs')  
os.makedirs(log_directory, exist_ok=True)  # Ensure the directory exists

# Use the current date from sys.argv[1] if provided, or default to today
current_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')

log_filename = os.path.join(log_directory, f"pipeline2_{current_date}.log")
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

################################################################################
#### Blob storage methods

# Blob storage account config
def setup_blob_client(retries=5, delay=2):
    """Initialize the BlobServiceClient with retry logic."""
    for attempt in range(retries):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_BLOB_CONN_STR)
            # Test connection with a simple operation
            _ = blob_service_client.get_container_client(config.AZURE_BLOB_CONT_NAME).exists()
            logging.info("Successfully connected to Azure Blob Storage.")
            return blob_service_client
        except (ServiceRequestError, ClientAuthenticationError) as e:
            logging.warning(f"Blob storage connection attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
            else:
                logging.error("Max retries reached. Could not connect to Azure Blob Storage.")
                raise

def download_blob_with_retry(blob_client, max_retries=3):
    """Download blob content with retry logic and chunked download"""
    for attempt in range(max_retries):
        try:
            return blob_client.download_blob().readall()
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logging.warning(f'Blob download attempt {attempt + 1} failed: {str(e)}. Retrying...')
            time.sleep(2 ** attempt)  # Exponential backoff
    
    raise Exception(f'Failed to download blob after {max_retries} attempts')

#### / Blob storage methods
################################################################################

################################################################################
#### SQL methods

# SQLAlchemy connection setup, create the engine with timeout settings
engine = create_engine(
    config.SQL_CONN_STR,
    connect_args={
        'timeout': config.SQL_QUERY_TIMEOUT,  
        'connection_timeout': config.SQL_CONN_TIMEOUT
    },
    pool_size=5,  # Number of connections to keep in the pool
    max_overflow=10,  # Maximum number of connections to create beyond pool_size
    pool_recycle=3600, # Recycle connections after 1 hour
    pool_pre_ping=True,
    #echo=True
)
SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_sql_session():
    """Get an SQLAlchemy session."""
    return SessionFactory()

def is_session_healthy(session):
    """Check if the session is healthy by executing a simple query."""
    try:
        session.execute(text("SELECT 1")).fetchone()
        return True
    except Exception as e:
        logging.error(f"Session health check failed: {e}")
        return False

def ensure_sql_connection(session):
    """Ensure/reconnect SQL function."""
    try:
        if not session or not is_session_healthy(session):
            if session and session.connection():
                session.close()  # Explicitly close the unhealthy session
                logging.info("Closed unhealthy session.")
            logging.warning("SQL session is invalid or unhealthy. Reconnecting to the database...")
            session = get_sql_session()
            logging.info("Reconnected to the database.")
        return session
    except Exception as reconnect_error:
        logging.error(f"Failed to reconnect: {reconnect_error}")
        raise  # Raise the error instead of returning None

def clear_sql_table(table_name):
    '''
    Clears the given table. Used to clear the MasterFiling table for new day, cascading through rest of DB.
    '''
    logging.info(f'Attempting to clear SQL DB table: {table_name}.')

    with get_sql_session() as session:
        # Retry logic
        for attempt in range(config.MAX_SQL_INSERT_RETRIES):

            try:
                logging.info(f'Clear attempt {attempt + 1} of {config.MAX_SQL_INSERT_RETRIES} beginning.')
                
                with ensure_sql_connection(session) as session:
                    logging.info(f"SQL connection secured for clear attempt {attempt + 1}.")

                    clear_table_cmd = f'DELETE FROM {table_name}'
                    session.execute(text(clear_table_cmd))
                    session.commit()
                    logging.info(f'Successfully cleared existing data from SQL DB table: {table_name}.')
                    return True

            except Exception as e:
                logging.error(f"Clear attempt {attempt + 1} failed. Error: {e}")
                
                if session:
                    session.rollback()
                    logging.info('Transaction rolled back.')
                
                if attempt < config.MAX_SQL_INSERT_RETRIES - 1:
                    if "40613" in str(e):  # Check for transient error code
                        backoff_time = min(2 ** attempt, 30)  # Exponential backoff with a cap
                        logging.warning(f"Database unavailable. Retrying after {backoff_time} seconds.")
                        time.sleep(backoff_time)
                    else:
                        backoff_time = 2 ** attempt  # Exponential backoff
                        logging.warning(f"Retrying clear after {backoff_time} seconds.")
                        time.sleep(backoff_time)
                else:
                    logging.error("Clear retries exhausted.")
                    return False
    
#### / SQL Methods
################################################################################

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

def financial_statement_inserter(filing_id, session, financial_statements):
    
    for statement in financial_statements:
        result = session.execute(text('''
            INSERT INTO FinancialReport (filing_id, report_doc, report_name, report_title_read)
            OUTPUT INSERTED.financial_report_id
            VALUES (:filing_id, :report_doc, :report_name, :report_title_read)
        '''), {
            'filing_id': filing_id,
            'report_doc': statement['report_doc'],
            'report_name': statement['report_name'],
            'report_title_read': statement['report_title_read']
        }).fetchone()

        # Get the last inserted financial_report_id
        financial_report_id = result[0]

        logging.info(f"FinancialReport table insert made for report: {statement['report_name']}.")

        # Batch financial report facts from report and insert together
        financial_facts_batch = []

        # Load and parse the JSON string
        try:
            report_df_dict = json.loads(statement['report_df'])
            
            # Flatten the dict
            for account_name, metrics in report_df_dict.items():
                for date, value in metrics.items():
                    financial_facts_batch.append({
                        'financial_report_id': financial_report_id,
                        'account_name': account_name,
                        'time_period': date,
                        'value': value
                    })
        except Exception as e:
            logging.warning(f'Empty or invalid data frame for {statement["report_name"]} ({statement["report_doc"]}). Error: {e}.')


        if financial_facts_batch:
            session.execute(text('''
                INSERT INTO FinancialReportFacts (financial_report_id, account_name, time_period, value)
                VALUES (:financial_report_id, :account_name, :time_period, :value)
            '''), financial_facts_batch)

            logging.info(f"FinancialReportFacts table insert(s) made for report: {statement['report_name']}.")
    return True

def text_section_inserter(filing_id, session, text_sections):
    for section in text_sections:

        # Check if the document already exists
        result = session.execute(text('''
            SELECT text_document_id FROM TextDocument
            WHERE filing_id = :filing_id AND section_doc = :section_doc
        '''), {
            'filing_id': filing_id,
            'section_doc': section['section_doc']
        }).fetchone()

        if result:
            text_document_id = result[0]
            logging.info(f"Home document of current section ({section['section_doc']}) has already been saved, skipping TextDocument insert.")
        else:
            # Insert if not
            result = session.execute(text('''
                INSERT INTO TextDocument (filing_id, section_doc)
                OUTPUT INSERTED.text_document_id
                VALUES (:filing_id, :section_doc)
            '''), {
                'filing_id': filing_id,
                'section_doc': section['section_doc']
            }).fetchone()

            text_document_id = result[0]

            logging.info(f"TextDocument table insert made for document: {section['section_doc']}.")

        # Ensure text is in right format
        parsed_text_str = format_section_parsed_text(section)
        
        # Insert the text section facts
        session.execute(text('''
            INSERT INTO TextSectionFacts (text_document_id, section_name, section_type, section_text)
            VALUES (:text_document_id, :section_name, :section_type, :section_text)
        '''), {
            'text_document_id': text_document_id,
            'section_name': section['section_name'],
            'section_type': section['section_type'],
            'section_text': parsed_text_str
        })

        logging.info(f"TextSectionFacts table insert made for section: {section['section_name']}.")
    return True

def event_info_inserter(filing_id, session, event_info):
    # Insert main event info into Event8K table
    result = session.execute(text('''
        INSERT INTO Event8K (filing_id, event_info)
        OUTPUT INSERTED.event_id
        VALUES (:filing_id, :event_info)
    '''), {
        'filing_id': filing_id,
        'event_info': json.dumps({k: v for k, v in event_info.items() if k != 'items_listed'})  # Exclude 'items_listed' for JSON storage
    }).fetchone()

    event_id = result[0]
    
    logging.info('Event8K table insert made.')

    # Insert each item in items_listed into Event8KItems table
    if 'items_listed' in event_info:
        items_batch = [{
            'event_id': event_id,
            'item_name': item
        } for item in event_info['items_listed']]
        
        session.execute(text('''
            INSERT INTO Event8KItems (event_id, item_name)
            VALUES (:event_id, :item_name)
        '''), items_batch)

        logging.info('Event8KItems table insert(s) made.')
    
    return True

def insider_trans_inserter(filing_id, session, insider_trans):
    # Insert into Form4IssuerInfo table
    issuer_info = insider_trans['issuer_info']
    session.execute(text('''
        INSERT INTO Form4IssuerInfo (filing_id, issuer_cik, issuer_name, issuer_trading_symbol)
        VALUES (:filing_id, :issuer_cik, :issuer_name, :issuer_trading_symbol)
    '''), {
        'filing_id': filing_id,
        'issuer_cik': issuer_info.get('issuerCik'),
        'issuer_name': issuer_info.get('issuerName'),
        'issuer_trading_symbol': issuer_info.get('issuerTradingSymbol')
    })
    logging.info('Form4IssuerInfo table insert made.')

    # Insert into Form4OwnerInfo table
    for owner in insider_trans['owner_info']:
        session.execute(text('''
            INSERT INTO Form4OwnerInfo (filing_id, owner_cik, owner_name, owner_city, owner_state, is_officer, officer_title)
            VALUES (:filing_id, :owner_cik, :owner_name, :owner_city, :owner_state, :is_officer, :officer_title)
        '''), {
            'filing_id': filing_id,
            'owner_cik': owner.get('ownerCik'),
            'owner_name': owner.get('ownerName'),
            'owner_city': owner.get('ownerCity'),
            'owner_state': owner.get('ownerState'),
            'is_officer': 1 if owner.get('isOfficer') else 0,  # Assuming BIT representation for is_officer TODO: CHECK THIS
            'officer_title': owner.get('officerTitle')
        })
    logging.info('Form4OwnerInfo table insert(s) made.')

    # Insert into Form4NonDerivTransactionInfo table (batch insert)
    if insider_trans['trans']:
        transaction_batch = [{
            'filing_id': filing_id,
            'security_title': transaction.get('securityTitle'),
            'transaction_date': transaction.get('transactionDate'),
            'transaction_code': transaction.get('transactionCode'),
            'transaction_shares': transaction.get('transactionShares') or None,
            'transaction_price_per_share': transaction.get('transactionPricePerShare') or None,
            'transaction_acquired_disposed_code': transaction.get('transactionAcquiredDisposedCode'),
            'shares_owned_following_transaction': transaction.get('sharesOwnedFollowingTransaction') or None,
            'direct_or_indirect_ownership': transaction.get('directOrIndirectOwnership')
        } for transaction in insider_trans['trans']]

        ######### TESTING
        #logging.info(f'Form4 trans batch:\n{transaction_batch}')

        session.execute(text('''
            INSERT INTO Form4NonDerivTransactionInfo (filing_id, security_title, transaction_date, transaction_code,
                                                      transaction_shares, transaction_price_per_share,
                                                      transaction_acquired_disposed_code, shares_owned_following_transaction,
                                                      direct_or_indirect_ownership)
            VALUES (:filing_id, :security_title, :transaction_date, :transaction_code, :transaction_shares, :transaction_price_per_share,
                    :transaction_acquired_disposed_code, :shares_owned_following_transaction, :direct_or_indirect_ownership)
        '''), transaction_batch)

        logging.info('Form4NonDerivTransactionInfo table insert(s) made.')

    # Insert into Form4Footnotes table
    for footnote_id, footnote_text in insider_trans['footnotes'].items():
        session.execute(text('''
            INSERT INTO Form4Footnotes (filing_id, footnote_ref_id, footnote_text)
            VALUES (:filing_id, :footnote_ref_id, :footnote_text)
        '''), {
            'filing_id': filing_id,
            'footnote_ref_id': footnote_id,
            'footnote_text': footnote_text
        })
    logging.info('Form4Footnotes table insert(s) made.')

    # Insert into Form4SignatureInfo table
    signature = insider_trans['sigs']
    session.execute(text('''
        INSERT INTO Form4SignatureInfo (filing_id, signature_name, signature_date)
        VALUES (:filing_id, :signature_name, :signature_date)
    '''), {
        'filing_id': filing_id,
        'signature_name': signature.get('signatureName'),
        'signature_date': signature.get('signatureDate')
    })
    logging.info('Form4SignatureInfo table insert made.')

    return True

def pdf_data_inserter(filing_id, session, pdfs):
    # Insert each PDF document into the PDFDocument table
    for pdf in pdfs:
        # Insert metadata into PDFDocument table
        result = session.execute(text('''
            INSERT INTO PDFDocument (filing_id, pdf_name, doc_type, metadata)
            OUTPUT INSERTED.pdf_id
            VALUES (:filing_id, :pdf_name, :doc_type, :metadata)
        '''), {
            'filing_id': filing_id,
            'pdf_name': pdf.get('pdf_name'),
            'doc_type': pdf.get('doc_type'),
            'metadata': str(pdf.get('metadata'))  # Convert metadata dict to string format for storage
        }).fetchone()
        
        pdf_id = result[0]

        logging.info(f"PDFDocument insert made for {pdf.get('pdf_name')}.")

        # Insert each page's content into PDFPageText table
        for page in pdf.get('page_content', []):
            session.execute(text('''
                INSERT INTO PDFPageText (pdf_id, page_num, page_text)
                VALUES (:pdf_id, :page_num, :page_text)
            '''), {
                'pdf_id': pdf_id,
                'page_num': page.get('page_num'),
                'page_text': page.get('page_text')
            })
        logging.info(f"PDFPageText inserts made for PDF {pdf.get('pdf_name')}.")

    logging.info("All PDF data inserted.")
    return True

def holdings_report_inserter(filing_id, session, holdings_report):
    # Insert metadata into HoldingsReport table
    result = session.execute(text('''
        INSERT INTO HoldingsReport (filing_id, report_yr_quarter, is_amendment, 
                                    amendment_no, amendment_type, filing_mgr_name, 
                                    filing_mgr_addr, report_type, form13f_filenum, 
                                    sec_filenum, info_instruction5, sig_name, 
                                    sig_title, sig_phone, sic_loc, sig_date, 
                                    other_mgrs_count, it_entries_count, it_value_total)
        OUTPUT INSERTED.holdings_report_id
        VALUES (:filing_id, :report_yr_quarter, :is_amendment, :amendment_no, :amendment_type, :filing_mgr_name, 
                :filing_mgr_addr, :report_type, :form13f_filenum, :sec_filenum, :info_instruction5, :sig_name, 
                :sig_title, :sig_phone, :sic_loc, :sig_date, :other_mgrs_count, :it_entries_count, :it_value_total)
    '''), {
        'filing_id': filing_id,
        'report_yr_quarter': holdings_report.get('report_yr_quarter'),
        'is_amendment': holdings_report.get('amendment', {}).get('is_amendment'),
        'amendment_no': holdings_report.get('amendment', {}).get('amendment_no'),
        'amendment_type': holdings_report.get('amendment', {}).get('amendment_type'),
        'filing_mgr_name': holdings_report.get('filing_mgr_name'),
        'filing_mgr_addr': holdings_report.get('filing_mgr_addr'),
        'report_type': holdings_report.get('report_type'),
        'form13f_filenum': holdings_report.get('form13f_filenum'),
        'sec_filenum': holdings_report.get('sec_filenum'),
        'info_instruction5': holdings_report.get('info_instruction5'),
        'sig_name': holdings_report.get('sig_name'),
        'sig_title': holdings_report.get('sig_title'),
        'sig_phone': holdings_report.get('sig_phone'),
        'sic_loc': holdings_report.get('sic_loc'),
        'sig_date': holdings_report.get('sig_date'),
        'other_mgrs_count': holdings_report.get('other_mgrs_count') or None,
        'it_entries_count': holdings_report.get('it_entries_count') or None,
        'it_value_total': holdings_report.get('it_value_total') or None
    }).fetchone()

    holdings_report_id = result[0]

    logging.info("HoldingsReport insert made.")

    # Insert Other Managers data 
    other_managers_batch = [{
        'holdings_report_id': holdings_report_id,
        'mgr_seq': manager.get('mgr_seq') or None,
        'mgr_cik': manager.get('mgr_cik'),
        'mgr_13f_filenum': manager.get('mgr_13f_filenum'),
        'mgr_sec_filenum': manager.get('mgr_sec_filenum'),
        'mgr_crd_num': manager.get('mgr_crd_num'),
        'mgr_name': manager.get('mgr_name')
    } for manager in holdings_report.get('other_mgrs', [])]

    if other_managers_batch:
        session.execute(text('''
            INSERT INTO OtherManagers (holdings_report_id, mgr_seq, mgr_cik, 
                                       mgr_13f_filenum, mgr_sec_filenum, 
                                       mgr_crd_num, mgr_name)
            VALUES (:holdings_report_id, :mgr_seq, :mgr_cik, :mgr_13f_filenum, :mgr_sec_filenum, :mgr_crd_num, :mgr_name)
        '''), other_managers_batch)
        logging.info(f"{len(other_managers_batch)} OtherManagers insert(s) made.")

    # Insert Holdings Entries data in smaller batches
    holdings_entries_batch = [{
        'holdings_report_id': holdings_report_id,
        'issuer': entry.get('issuer'),
        'class': entry.get('class'),
        'cusip': entry.get('cusip'),
        'value': entry.get('value') or None,
        'amount': entry.get('amount'),
        'amt_type': entry.get('amt_type'),
        'discretion': entry.get('discretion'),
        'sole_vote': entry.get('sole_vote'),  
        'shared_vote': entry.get('shared_vote'),
        'no_vote': entry.get('no_vote'),
        'figi': entry.get('figi'),
        'other_manager': entry.get('other_mgr'),
        'option_type': entry.get('option_type')
    } for entry in holdings_report.get('it_entries', [])]

    batch_size = 100  # Adjust based on your needs
    for i in range(0, len(holdings_entries_batch), batch_size):
        batch = holdings_entries_batch[i:i + batch_size]
        session.execute(text('''
            INSERT INTO HoldingsEntries (holdings_report_id, issuer, class, 
                                         cusip, value, amount, amt_type, 
                                         discretion, sole_vote, shared_vote, 
                                         no_vote, figi, other_manager, 
                                         option_type)
            VALUES (:holdings_report_id, :issuer, :class, :cusip, :value, :amount, :amt_type, 
                    :discretion, :sole_vote, :shared_vote, :no_vote, :figi, :other_manager, :option_type)
        '''), batch)
        logging.info(f"Inserted batch of {len(batch)} HoldingsEntries.")

    logging.info("All holdings report data inserted.")
    return True

#### / Insertion handlers
################################################################################

################################################################################
#### Orchestrators

# Chooses correct insertion handler and hands over to them
def insert_parsed_filing_data(filing_id, session, parsed_data_type, parsed_object):
    match parsed_data_type:
        case 'financial_statements':
            return financial_statement_inserter(filing_id, session, parsed_object)
        case 'text_sections':
            return text_section_inserter(filing_id, session, parsed_object)
        case 'event_info':
            return event_info_inserter(filing_id, session, parsed_object)
        case 'insider_trans':
            return insider_trans_inserter(filing_id, session, parsed_object)
        case 'pdfs':
            return pdf_data_inserter(filing_id, session, parsed_object)
        case 'holdings_report':
            return holdings_report_inserter(filing_id, session, parsed_object)
        case _:
            logging.warning(f'Unknown data type...: {parsed_data_type}. Don\'t know how to parse + insert.')
            return True

# Orchestrates insertion of filing data to the DB
def insert_filing_data(session, filing_json):
    filing_info = filing_json.get('filing_info', None)
    if not filing_info:
        logging.error(f'Met in invalid filing_json object in insert_filing_data(). Error: {e}.')
        return False
    
    # Concat name changes
    name_changes_str = concatenate_name_changes(filing_info['name_changes'])
    
    # Get a list of parsed data type keys
    parsed_data_types = [key for key in filing_json.keys() if key != 'filing_info']

    # Retry logic
    for attempt in range(config.MAX_SQL_INSERT_RETRIES):

        try:
            logging.info(f'Insert attempt {attempt + 1} of {config.MAX_SQL_INSERT_RETRIES} beginning.')
            
            with ensure_sql_connection(session) as session:
                logging.info(f"SQL connection secured for attempt {attempt + 1}.")

                # Check if the filing already exists in MasterFiling table
                result = session.execute(text('''
                    SELECT filing_id FROM MasterFiling
                    WHERE cik = :cik AND accession_number = :accession_number
                '''), {
                    'cik': filing_info['cik'],
                    'accession_number': filing_info['accession_number']
                }).fetchone()

                if result:
                    # Skip if already exists
                    logging.warning(f'Filing has already been loaded to the DB, according to MasterFiling. Skipping, CIK: {filing_info["cik"]}, ASCN: {filing_info["accession_number"]}.')
                    return True
                else:
                    # Insert filing info if not, and handle other data in specialized methods
                    result = session.execute(text('''
                        INSERT INTO MasterFiling (cik, type, date, accession_number, company_name, sic_code, sic_desc,
                                                report_period, state_of_incorp, fiscal_yr_end, business_address,
                                                business_phone, name_changes)
                        OUTPUT INSERTED.filing_id
                        VALUES (:cik, :type, :date, :accession_number, :company_name, :sic_code, :sic_desc,
                                :report_period, :state_of_incorp, :fiscal_yr_end, :business_address,
                                :business_phone, :name_changes)
                    '''), {
                        'cik': filing_info['cik'],
                        'type': filing_info['type'],
                        'date': datetime.strptime(filing_info['date'], '%Y%m%d').date(),
                        'accession_number': filing_info['accession_number'],
                        'company_name': filing_info['company_name'],
                        'sic_code': filing_info['sic_code'],
                        'sic_desc': filing_info['sic_desc'],
                        'report_period': filing_info['report_period'],
                        'state_of_incorp': filing_info['state_of_incorp'],
                        'fiscal_yr_end': filing_info['fiscal_yr_end'],
                        'business_address': filing_info['business_address'],
                        'business_phone': filing_info['business_phone'],
                        'name_changes': name_changes_str,
                    }).fetchone()

                    filing_id = result[0]
                    logging.info('MasterFiling table insert made.')

                # Loop through other parsed data, inserting into appropriate tables
                for key in parsed_data_types:
                    if not insert_parsed_filing_data(filing_id, session, key, filing_json[key]):
                        logging.error(f'Failed to insert parsed filing data type: {key}.')
                        # Kick back to our retry logic
                        raise Exception(f'Failed to insert parsed filing data type: {key}.') 
                    
                # Commit the transaction
                session.commit()
                logging.info(f'Transaction committed on attempt {attempt + 1}.')
                return True
                
        except Exception as e:
            logging.error(f"Insert attempt {attempt + 1} failed. Error: {e}")
            
            if session:
                session.rollback()
                logging.info('Transaction rolled back.')
            
            if attempt < config.MAX_SQL_INSERT_RETRIES - 1:
                if "40613" in str(e):  # Check for transient error code
                    backoff_time = min(2 ** attempt, 30)  # Exponential backoff with a cap
                    logging.warning(f"Database unavailable. Retrying after {backoff_time} seconds.")
                    time.sleep(backoff_time)
                else:
                    backoff_time = 2 ** attempt  # Exponential backoff
                    logging.warning(f"Retrying insert after {backoff_time} seconds.")
                    time.sleep(backoff_time)
            else:
                logging.error("Insert retries exhausted.")
                return False

    return False

def process_blob(blob, container_client):
    '''
    Handles processing (downloading and inserting into SQL db) of a single blob.
    Returns: a tuple (blob.name, True/False)
    '''
    logging.info(f'Processing blob: {blob.name}')

    try:
        blob_client = container_client.get_blob_client(blob.name)
        content = download_blob_with_retry(blob_client)
        content_dict = json.loads(content)
        logging.info(f'Downloaded blob content: {blob.name}.')

        # Create a session per thread
        with get_sql_session() as session:
            if insert_filing_data(session, content_dict):
                logging.info(f'Successfully inserted SQL data for blob: {blob.name}')
                return (blob.name, True)  # Indicate success
            else:
                logging.error(f'Failed to insert SQL data for blob: {blob.name}')
                return (blob.name, False)  # Indicate failure
    except Exception as e:
        logging.error(f'Error processing blob {blob.name}: {str(e)}')
        return (blob.name, False)

def process_blobs_in_parallel(container_client, json_blobs, max_workers=5):
    """Process blobs in parallel using ThreadPoolExecutor."""
    successful_inserts = []
    failed_inserts = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_blob = {executor.submit(process_blob, blob, container_client): blob for blob in json_blobs}

        for future in as_completed(future_to_blob):
            blob_name, success = future.result()
            if success:
                successful_inserts.append(blob_name)
                logging.info(f'Successfully inserted blob contents: {blob_name}.')
            else:
                failed_inserts.append(blob_name)
                logging.error(f'Failed to insert blob contents: {blob_name}.')
    
    return successful_inserts, failed_inserts

def load_parsed_day_for_analysis(target_date):
    """Main logic here. Processes a parsed filing JSON object and inserts its data into our DB"""

    if type(target_date) not in [date, datetime]:
        logging.error('Invalid target_date type. Must be a date or datetime object.')
        raise Exception('Invalid target_date type. Must be a date or datetime object.')

    # Build our subfolder path for the day's filings
    parsed_filings_folder = f'filings/{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}/'

    # Get the container client
    blob_service_client = setup_blob_client()
    container_client = blob_service_client.get_container_client(config.AZURE_BLOB_CONT_NAME)

    # List blobs in the specified folder
    blob_list = list(container_client.list_blobs(name_starts_with=parsed_filings_folder))
    json_blobs = [blob for blob in blob_list if blob.name.endswith('.json')]
    total_blob_count = len(json_blobs)
    logging.info(f'Number of blobs found in {parsed_filings_folder}: {total_blob_count}.')

    successful_inserts = [] # Will hold lists of successful and failed inserts/filings, respectively
    failed_inserts = []

    '''
    session = None  # Ensure session is defined outside the loop
    for connect_attempt in range(config.MAX_SQL_LOGIN_RETRIES):
        try:
            session = get_sql_session()
            logging.info('Connected to SQL db.')
            break  # Exit the retry loop on success
        except Exception as e:
            logging.error(f'Failed connecting to SQL db. Error: {e}')
            if connect_attempt < config.MAX_SQL_LOGIN_RETRIES - 1:
                logging.warning('Retrying connection after sleep.')
                time.sleep(2)
            else:
                logging.error('Connection retries exhausted.')
                raise
    else:
        raise Exception('Unable to establish connection to the database.')
    '''

    try:
        # Clear database
        if clear_sql_table(config.SQL_MASTER_TABLE):

            # Process blobs
            successful_inserts, failed_inserts = process_blobs_in_parallel(
                container_client, 
                json_blobs,
            )
            
            total_count = len(successful_inserts) + len(failed_inserts)
            logging.info(f'Finished processing. Successfully inserted {len(successful_inserts)}/{total_count} filings.')
            
            if failed_inserts:
                logging.warning(f'Failed to insert {len(failed_inserts)}/{total_count} filings:')
                logging.warning('\n'.join(failed_inserts))
        else:
            logging.error(f'Failed to clear table: {config.SQL_MASTER_TABLE}, cannot proceed.')
    
    except Exception as e:
        logging.error(f"Unexpected error encountered during blob download or SQL insertion: {e}")
    
    '''
    finally:
        if session and session.connection():
            session.close()
            logging.info('Connection to SQL db closed.')
    '''

    logging.info(f'Finished iterating parsed filings. Successfully inserted {len(successful_inserts)}/{total_blob_count} filings.')
    # TODO: Possibly parse daily index file and insert basic info of unparsed filings to MasterFiling/other table for stats. For now am just reading from CSV in flask app

#### / Orchestrators
################################################################################

"""Real entrypoint, after config stuff at top"""
logging.info('Starting pipeline step 3 (JSON -> SQL loading) workflow.')
t0 = time.time()

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
    # NOTE: Adjust timezone settings as needed 
    target_date = datetime.now(ZoneInfo("America/Phoenix")).date()
    logging.info(f'Target date set to today: {target_date}.')

# TODO: Finish handling weekends and system holidays
if target_date.weekday() in (5, 6):
    logging.info('Target date is a Saturday or Sunday, no parsed filings to load.')
    logging.info('Pipeline step 3 (JSON -> SQL loading) workflow completed.')
    exit()

# Call main worker method
load_parsed_day_for_analysis(target_date)

logging.info(f'Pipeline step 3 (JSON -> SQL loading) workflow completed. Time elapsed:  {(time.time() - t0):.3f}s.')