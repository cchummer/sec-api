from azure.storage.blob import BlobServiceClient
from collections import defaultdict
import pandas as pd
import json
import pyodbc
import config

# Prevent unnecessary queries etc...
class DataCache:
    csv_summary = {} # Will hold summary data regarding filers and filing types of all filings of the day, parsed or not 
    sql_summary = {} # Will hold summary data regarding filers, filing types, and industries 
    sic_summary = {} # Will hold summary data regarding filers and filing types, organized by industry
    fin_summary = {} # Will hold summary data regarding financial report type filings, organized by specific filing type
    prospectus_summary = {} # Will hold summary data regarding prospectus summary type filings, organized by specific filing type
    events_summary = {} # Will hold summary data regarding current event filings, organized by specific filing type
    sec_staff_summary = {} # Will hold summary data regarding SEC staff action or letter filings, organized by specific filing type (either letter or action)
    proxy_summary = {} # Will hold summary data regarding proxy filings, organized by specific filing type
    indiv_filings = {} # Will hold data on specific filings, such as their text sections 
    industry_text_sections = {} # Will hold a list of text sections for each industry / SIC code

data_cache = DataCache()

def get_db_connection():
    """
    Establishes a connection to the database using the configuration details
    from the config.py file.
    """
    connection_string = (
        f"DRIVER={config.SQL_DRIVER};"
        f"SERVER={config.SQL_SERVER};"
        f"DATABASE={config.SQL_DATABASE};"
        f"UID={config.SQL_USERNAME};"
        f"PWD={config.SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        f"Connection Timeout={config.SQL_CONN_TIMEOUT};"
    )
    
    try:
        conn = pyodbc.connect(connection_string)
        print('Connected to SQL DB.')
        return conn
    except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        return None

def refresh_csv_summary():
    """Fetch and cache CSV summary counts."""
    print('Refreshing csv_summary dictionary.')
    if not data_cache.csv_summary:  # Only query if csv_summary is empty
        try:
            blob_service_client = BlobServiceClient(
                account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
                credential=config.STORAGE_ACCOUNT_KEY,
            )
            blob_client = blob_service_client.get_blob_client(container=config.CONTAINER_NAME, blob=config.CSV_FILE_NAME)
            csv_data = blob_client.download_blob().readall()
            df = pd.read_csv(pd.io.common.BytesIO(csv_data))
            
            # Compute statistics
            data_cache.csv_summary = {
                "total_filings": len(df),
                "all_by_type": list(df["type"].value_counts().to_dict().items()),
                "all_by_company": list(df["company"].value_counts().to_dict().items()),
            }
            print('Refreshed csv_summary dictionary.')
        except Exception as e:
            print(f"Error fetching CSV: {e}")
            data_cache.csv_summary = {}
    else:
        print('csv_summary already populated, skipping refresh.')

def refresh_sql_summary():
    """Fetch and cache SQL summary counts."""
    print('Refreshing sql_summary dictionary.')
    if not data_cache.sql_summary:  # Only query if sql_summary is empty
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Perform queries
                cursor.execute("SELECT COUNT(*) FROM MasterFiling;")
                total_parsed_filings = cursor.fetchone()[0]

                cursor.execute("SELECT MIN(date) AS current_filings_date FROM MasterFiling;")
                current_filings_date = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT type, COUNT(*) AS count
                    FROM MasterFiling
                    GROUP BY type
                    ORDER BY count DESC;
                """)
                parsed_by_type = cursor.fetchall()

                cursor.execute("""
                    SELECT company_name, COUNT(*) AS count
                    FROM MasterFiling
                    GROUP BY company_name
                    ORDER BY count DESC;
                """)
                parsed_by_company = cursor.fetchall()

                cursor.execute("""
                    SELECT sic_code, sic_desc, COUNT(*) as count
                    FROM MasterFiling
                    GROUP BY sic_code, sic_desc
                    ORDER BY count DESC;
                """)
                parsed_by_industry = cursor.fetchall()

                # Convert results to dictionaries
                data_cache.sql_summary = {
                    'total_filings_parsed': total_parsed_filings,
                    'current_filings_date': current_filings_date,
                    'parsed_by_type': [{'type': row[0], 'count': row[1]} for row in parsed_by_type],
                    'parsed_by_company': [{'company_name': row[0], 'count': row[1]} for row in parsed_by_company],
                    'parsed_by_industry': [{'sic_code': row[0], 'sic_desc': row[1], 'count': row[2]} for row in parsed_by_industry]
                }

                conn.close()
                print('Refreshed sql_summary dictionary.')
        except Exception as e:
            print(f"Error fetching SQL summary: {e}.")
            data_cache.sql_summary = {}

    else:
        print('sql_summary already populated, skipping refresh.')

def refresh_sic_summary(sic):
    """Fetch and cache SIC summary counts."""
    print(f'Refreshing sic_summary dictionary for SIC: {sic}.')
    if sic not in data_cache.sic_summary:  # Only query if this SIC is not in sic_summary
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query for specific SIC code
                cursor.execute("""
                    SELECT sic_desc, sic_code, COUNT(*) as total_filings
                    FROM MasterFiling
                    WHERE sic_code = ?
                    GROUP BY sic_desc, sic_code;
                """, (sic,))
                result = cursor.fetchone()

                # Populate the summary if a result is found
                if result:
                    filings_by_type = cursor.execute("""
                        SELECT type, COUNT(*) as count
                        FROM MasterFiling
                        WHERE sic_code = ?
                        GROUP BY type;
                    """, (sic,)).fetchall()

                    filings_by_company = cursor.execute("""
                        SELECT company_name, COUNT(*) as count
                        FROM MasterFiling
                        WHERE sic_code = ?
                        GROUP BY company_name;
                    """, (sic,)).fetchall()

                    data_cache.sic_summary[sic] = {
                        'sic_desc': result[0],
                        'sic_code': result[1],
                        'total_filings': result[2],
                        'by_type': [{'type': row[0], 'count': row[1]} for row in filings_by_type],
                        'by_company': [{'company_name': row[0], 'count': row[1]} for row in filings_by_company]
                    }
                else:
                    data_cache.sic_summary[sic] = {
                        'sic_desc': 'Unknown',
                        'sic_code': sic,
                        'total_filings': 0,
                        'by_type': [],
                        'by_company': []
                    }

                conn.close()
                print(f'Refreshed sic_summary for {sic}.')
        except Exception as e:
            print(f"Error fetching SIC summary for {sic}: {e}.")
            data_cache.sic_summary[sic] = {}
    else:
        print(f'sic_summary already populated for {sic}, skipping refresh.')

def refresh_fin_reports_summary(filing_type):
    """Fetch summary information on financial report filing types, filtered to the specific type given."""
    print(f'Refreshing fin_summary dictionary for filing type: {filing_type}.')
    
    if filing_type not in data_cache.fin_summary:  # Only query if this filing type is not in fin_summary
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query for Text Sections
                cursor.execute("""
                    SELECT 
                        mf.sic_desc AS Industry,
                        mf.company_name AS CompanyName,
                        mf.filing_id AS FilingID,
                        td.section_doc AS TextDocumentName,
                        tsf.section_name AS TextSectionName,
                        tsf.section_type AS SectionType
                    FROM MasterFiling mf
                    LEFT JOIN TextDocument td ON mf.filing_id = td.filing_id
                    LEFT JOIN TextSectionFacts tsf ON td.text_document_id = tsf.text_document_id
                    WHERE mf.type = ?
                    ORDER BY mf.sic_desc, mf.company_name;
                """, (filing_type,))
                text_section_results = cursor.fetchall()

                # Query for Financial Reports
                cursor.execute("""
                    SELECT 
                        mf.sic_desc AS Industry,
                        mf.company_name AS CompanyName,
                        mf.filing_id AS FilingID,
                        fr.report_doc AS FinancialReportDoc, 
                        fr.report_name AS FinancialReportName
                    FROM MasterFiling mf
                    LEFT JOIN FinancialReport fr ON mf.filing_id = fr.filing_id
                    WHERE mf.type = ?
                    ORDER BY mf.sic_desc, mf.company_name;
                """, (filing_type,))
                financial_report_results = cursor.fetchall()

                # Prepare data for the summary cache
                fin_summary_data = {}

                # Organize text sections by filing_id
                for row in text_section_results:
                    filing_id = row[2]
                    if filing_id not in fin_summary_data:
                        fin_summary_data[filing_id] = {
                            'industry': row[0] if row[0] else "Unknown",
                            'company_name': row[1],
                            'filing_id': filing_id,
                            'text_sections': [],
                            'financial_reports': []
                        }
                    if row[4]:
                        fin_summary_data[filing_id]['text_sections'].append({
                            'section_name': row[4],
                            'section_type': row[5],
                            'doc_name': row[3]
                        })

                # Organize financial reports by filing_id
                for row in financial_report_results:
                    filing_id = row[2]
                    if filing_id not in fin_summary_data:
                        fin_summary_data[filing_id] = {
                            'industry': row[0] if row[0] else "Unknown",
                            'company_name': row[1],
                            'filing_id': filing_id,
                            'text_sections': [],
                            'financial_reports': []
                        }
                    if row[4]:
                        fin_summary_data[filing_id]['financial_reports'].append({
                            'rep_name': row[4],
                            'doc_name': row[3]
                        })

                # Convert the dictionary to a list for the final summary
                fin_summary_data_list = list(fin_summary_data.values())

                # Store the fetched data in the cache
                data_cache.fin_summary[filing_type] = fin_summary_data_list

                conn.close()
                print(f'Refreshed fin_summary for filing type {filing_type}.')
        
        except Exception as e:
            print(f"Error fetching financial reports summary for {filing_type}: {e}.")
            data_cache.fin_summary[filing_type] = []  # Empty list if error occurs

    else:
        print(f'fin_summary already populated for type {filing_type}, skipping refresh.')

def refresh_prospectus_reports_summary(filing_type):
    """Fetch summary information on prospectus filings, filtered by specific filing type."""
    print(f'Refreshing prospectus_summary dictionary for filing type: {filing_type}.')
    
    if filing_type not in data_cache.prospectus_summary:  # Only query if this filing type is not in prospectus_summary
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query for Text Sections related to prospectus filings
                cursor.execute("""
                    SELECT 
                        mf.sic_desc AS Industry,
                        mf.company_name AS CompanyName,
                        mf.filing_id AS FilingID,
                        td.section_doc AS TextDocumentName,
                        tsf.section_name AS TextSectionName,
                        tsf.section_type AS SectionType
                    FROM MasterFiling mf
                    LEFT JOIN TextDocument td ON mf.filing_id = td.filing_id
                    LEFT JOIN TextSectionFacts tsf ON td.text_document_id = tsf.text_document_id
                    WHERE mf.type = ?
                    ORDER BY mf.sic_desc, mf.company_name;
                """, (filing_type,))
                text_section_results = cursor.fetchall()

                # Prepare data for the summary cache
                prospectus_summary_data = {}

                # Organize text sections by filing_id
                for row in text_section_results:
                    filing_id = row[2]
                    if filing_id not in prospectus_summary_data:
                        prospectus_summary_data[filing_id] = {
                            'industry': row[0] if row[0] else "Unknown",
                            'company_name': row[1],
                            'filing_id': filing_id,
                            'text_sections': []
                        }
                    if row[4]:  # Add text section details if available
                        prospectus_summary_data[filing_id]['text_sections'].append({
                            'section_name': row[4],
                            'section_type': row[5],
                            'doc_name': row[3]
                        })

                # Convert the dictionary to a list for the final summary
                prospectus_summary_data_list = list(prospectus_summary_data.values())

                # Store the fetched data in the cache
                data_cache.prospectus_summary[filing_type] = prospectus_summary_data_list

                conn.close()
                print(f'Refreshed prospectus_summary for filing type {filing_type}.')
        
        except Exception as e:
            print(f"Error fetching prospectus summary for {filing_type}: {e}.")
            data_cache.prospectus_summary[filing_type] = []  # Empty list if error occurs

    else:
        print(f'prospectus_summary already populated for type {filing_type}, skipping refresh.')

def refresh_events_summary(filing_type):
    """Fetch and cache 8K/current event filing summaries."""
    print(f'Refreshing events_summary dictionary for type {filing_type}.')
    if filing_type not in data_cache.events_summary:  # Only query if events_summary is empty for the given filing type 
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Initialize the events_data structure
                events_data = {
                    'by_industry': {},
                    'by_item': {}
                }
            
                # Query to fetch all event filings and their details, including filer_cik
                cursor.execute('''
                    SELECT mf.filing_id, mf.company_name, mf.date, mf.business_address, mf.accession_number,
                        mf.sic_code, mf.sic_desc, mf.cik, e.event_id
                    FROM MasterFiling mf
                    JOIN Event8K e ON mf.filing_id = e.filing_id
                    WHERE mf.type = ?
                ''', (filing_type))
                
                filings = cursor.fetchall()

                for (filing_id, company_name, date, business_address, accession_number,
                    sic_code, sic_desc, cik, event_id) in filings:
                    # Query to fetch items listed for this event
                    cursor.execute('''
                        SELECT item_name
                        FROM Event8KItems
                        WHERE event_id = ?
                    ''', (event_id,))
                    
                    items_listed = [row[0] for row in cursor.fetchall()]

                    # Create the filing record
                    filing_record = {
                        'filing_id': filing_id,
                        'company_name': company_name,
                        'date': date,
                        'items_listed': items_listed,
                        'business_address': business_address,
                        'accession_number': accession_number,
                        'cik': cik,
                        'sic_code': sic_code,
                        'sic_desc': sic_desc
                    }

                    # Populate by_industry with count
                    if f'{sic_code} - {sic_desc}' not in events_data['by_industry']:
                        events_data['by_industry'][f'{sic_code} - {sic_desc}'] = {
                            'filings': [],
                            'count': 0
                        }
                    events_data['by_industry'][f'{sic_code} - {sic_desc}']['filings'].append(filing_record)
                    events_data['by_industry'][f'{sic_code} - {sic_desc}']['count'] += 1


                    # Populate by_item with count
                    for item in items_listed:
                        if item not in events_data['by_item']:
                            events_data['by_item'][item] = {
                                'filings': [],
                                'count': 0
                            }
                        events_data['by_item'][item]['filings'].append(filing_record)
                        events_data['by_item'][item]['count'] += 1

                # Sort by count
                events_data['by_industry'] = dict(sorted(events_data['by_industry'].items(), key=lambda item: item[1]['count'], reverse=True))
                events_data['by_item'] = dict(sorted(events_data['by_item'].items(), key=lambda item: item[1]['count'], reverse=True))

                # Store in the cache
                data_cache.events_summary[filing_type] = events_data

                conn.close()
                print(f'Refreshed events_summary dictionary for type {filing_type}.')
        
        except Exception as e:
            print(f"Error fetching current events filings summary for type {filing_type}: {e}.")
            data_cache.events_summary[filing_type] = []  # Empty list if error occurs

    else:
        print(f'events_summary already populated for type {filing_type}, skipping refresh.')

def refresh_sec_staff_summary(filing_type):
    '''
    Fetch and cache SEC staff action/letter filing info summary.
    '''

    if filing_type not in data_cache.sec_staff_summary:  # Only query if this filing type is not in cache
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query for PDF documents related to SEC staff filings
                cursor.execute("""
                    SELECT mf.sic_desc, mf.company_name, mf.filing_id, pd.pdf_name, pd.doc_type, pd.metadata
                    FROM MasterFiling mf
                    JOIN PDFDocument pd ON mf.filing_id = pd.filing_id
                    WHERE mf.type = ?
                    ORDER BY mf.sic_desc, mf.company_name;
                """, (filing_type,))
                pdf_doc_results = cursor.fetchall()

                # Prepare data for the summary cache
                sec_staff_summary_data = {}

                # Organize PDF's by filing_id
                for row in pdf_doc_results:
                    filing_id = row[2]
                    if filing_id not in sec_staff_summary_data:
                        sec_staff_summary_data[filing_id] = {
                            'industry': row[0] if row[0] else "Unknown",
                            'company_name': row[1],
                            'filing_id': filing_id,
                            'pdf_docs': []
                        }
                    if row[3]:  # Add PDF details if available
                        sec_staff_summary_data[filing_id]['pdf_docs'].append({
                            'doc_name': row[3],
                            'doc_type': row[4],
                            'metadata': row[5]
                        })

                # Convert the dictionary to a list for the final summary
                sec_staff_summary_data_list = list(sec_staff_summary_data.values())

                # Store the fetched data in the cache
                data_cache.sec_staff_summary[filing_type] = sec_staff_summary_data_list

                conn.close()
                print(f'Refreshed sec_staff_summary for filing type {filing_type}.')

        except Exception as e:
            print(f"Error fetching SEC staff filings summary for type {filing_type}: {e}.")
            data_cache.sec_staff_summary[filing_type] = []  # Empty list if error occurs

def refresh_proxy_summary(filing_type):
    '''
    Fetch and cache proxy filing summary data.
    '''

    if filing_type not in data_cache.proxy_summary:  # Only query if this filing type is not in cache
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query for Text Sections
                cursor.execute("""
                    SELECT 
                        mf.sic_desc AS Industry,
                        mf.company_name AS CompanyName,
                        mf.filing_id AS FilingID,
                        td.section_doc AS TextDocumentName,
                        tsf.section_name AS TextSectionName,
                        tsf.section_type AS SectionType
                    FROM MasterFiling mf
                    LEFT JOIN TextDocument td ON mf.filing_id = td.filing_id
                    LEFT JOIN TextSectionFacts tsf ON td.text_document_id = tsf.text_document_id
                    WHERE mf.type = ?
                    ORDER BY mf.sic_desc, mf.company_name;
                """, (filing_type,))
                text_section_results = cursor.fetchall()

                # Prepare data for the summary cache
                proxy_summary_data = {}

                # Organize text sections by filing_id
                for row in text_section_results:
                    filing_id = row[2]
                    if filing_id not in proxy_summary_data:
                        proxy_summary_data[filing_id] = {
                            'industry': row[0] if row[0] else "Unknown",
                            'company_name': row[1],
                            'filing_id': filing_id,
                            'text_sections': []
                        }
                    if row[4]:  # Add text section details if available
                        proxy_summary_data[filing_id]['text_sections'].append({
                            'section_name': row[4],
                            'section_type': row[5],
                            'doc_name': row[3]
                        })

                # Convert the dictionary to a list for the final summary
                proxy_summary_data_list = list(proxy_summary_data.values())

                # Store the fetched data in the cache
                data_cache.proxy_summary[filing_type] = proxy_summary_data_list

                conn.close()
                print(f'Refreshed proxy_summary for filing type {filing_type}.')

        except Exception as e:
            print(f"Error fetching proxy filings summary for type {filing_type}: {e}.")
            data_cache.proxy_summary[filing_type] = []  # Empty list if error occurs

def load_financial_filing_data(filing_id, conn):
    '''
    Loads data for an individual financial report filing (10-Q, etc) into the data_cache.
    '''
    print(f'Attempting to load financial report filing data for filing_id {filing_id}.')
    cursor = conn.cursor()

    # Load TextDocument data
    cursor.execute("SELECT * FROM TextDocument WHERE filing_id = ?", (filing_id,))
    text_documents = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextDocument'] = text_documents

    # Load TextSectionFacts data
    cursor.execute("""
        SELECT tsf.* 
        FROM TextSectionFacts tsf
        JOIN TextDocument td ON tsf.text_document_id = td.text_document_id
        WHERE td.filing_id = ?
    """, (filing_id,))
    text_section_facts = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextSectionFacts'] = text_section_facts

    print(f'Got filing_id {filing_id} text documents and sections.')

    # And now financial reports and facts
    cursor.execute("SELECT * FROM FinancialReport WHERE filing_id = ?", (filing_id,))
    fin_reports = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['FinancialReport'] = fin_reports

    cursor.execute("""
        SELECT frf.*
        FROM FinancialReportFacts frf
        JOIN FinancialReport fr on frf.financial_report_id = fr.financial_report_id
        WHERE fr.filing_id = ?
    """, (filing_id,))
    fin_report_facts = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['FinancialReportFacts'] = fin_report_facts

    print(f'Got filing_id {filing_id} financial reports and facts.')
    print(f'Loaded financial report filing data for filing_id {filing_id}.')

def load_event_filing_data(filing_id, conn):
    """
    Loads data for an individual event filing (8-K, etc.) into the data_cache.
    """
    print(f'Attempting to load 8-K/event filing data for filing_id {filing_id}.')
    cursor = conn.cursor()

    # Load TextDocument data
    cursor.execute("SELECT * FROM TextDocument WHERE filing_id = ?", (filing_id,))
    text_documents = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextDocument'] = text_documents

    # Load TextSectionFacts data
    cursor.execute("""
        SELECT tsf.* 
        FROM TextSectionFacts tsf
        JOIN TextDocument td ON tsf.text_document_id = td.text_document_id
        WHERE td.filing_id = ?
    """, (filing_id,))
    text_section_facts = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextSectionFacts'] = text_section_facts

    print(f'Got filing_id {filing_id} 8-K text documents and sections.')

    # Load Event8K data and parse event_info as JSON
    cursor.execute("SELECT event_id, filing_id, event_info FROM Event8K WHERE filing_id = ?", (filing_id))
    event_8k_row = cursor.fetchone()
    if event_8k_row:
        event_id, int_filing_id, event_info_str = event_8k_row
        #try:
        if event_info_str:
            #print(f"event_info_str: {event_info_str}.")
            event_info = json.loads(event_info_str)
        else:
            print(f"Failed to find event_info_str.")
            event_info = {}
        #except Exception as e:
        #    print(f"JSON decoding error for filing_id {filing_id}: {e}")
        #    print(f"Raw event_info string: {event_info_str}")
        #    event_info = {}
        data_cache.indiv_filings[filing_id]['Event8K'] = {
            'event_id': event_id,
            'filing_id': int_filing_id,
            'event_info': event_info
        }
    else:
        print(f'No Event8K entry found for filing_id {filing_id}.')
        data_cache.indiv_filings[filing_id]['Event8K'] = None

    # Load Event8KItems data
    cursor.execute("""
        SELECT e8ki.* 
        FROM Event8KItems e8ki
        JOIN Event8K e8k ON e8ki.event_id = e8k.event_id
        WHERE e8k.filing_id = ?
    """, (filing_id))
    event_8k_items = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['Event8KItems'] = event_8k_items

    print(f'Loaded 8-K/event filing data for filing_id {filing_id}.')

def load_text_filing_data(filing_id, conn):
    """
    Loads data for an individual filing comprised of only TextDocument + TextSectionFacts dat (S-1, DEF 14A, etc) into the data_cache.
    """
    print(f'Attempting to load filing text data for filing_id {filing_id}.')
    cursor = conn.cursor()

    # Load TextDocument data
    cursor.execute("SELECT * FROM TextDocument WHERE filing_id = ?", (filing_id,))
    text_documents = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextDocument'] = text_documents

    # Load TextSectionFacts data
    cursor.execute("""
        SELECT tsf.* 
        FROM TextSectionFacts tsf
        JOIN TextDocument td ON tsf.text_document_id = td.text_document_id
        WHERE td.filing_id = ?
    """, (filing_id,))
    text_section_facts = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['TextSectionFacts'] = text_section_facts

    print(f'Got filing_id {filing_id} text documents and sections.')
    print(f'Loaded text data for filing_id {filing_id}.')


def load_sec_staff_filing_data(filing_id, conn):
    '''
    Loads data for an individual SEC staff action or SEC staff letter filing type. At the moment, is just text from PDFs. 
    '''
    print(f'Attempting to load SEC staff action/letter filing data for filing_id {filing_id}.')
    cursor = conn.cursor()

    # Load PDFDocument and PDFPageText 
    cursor.execute("SELECT * FROM PDFDocument WHERE filing_id = ?", (filing_id,))
    pdf_docs = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['PDFDocument'] = pdf_docs

    cursor.execute("""
        SELECT *
        FROM PDFPageText ppt
        JOIN PDFDocument pd on ppt.pdf_id = pd.pdf_id
        WHERE pd.filing_id = ?
    """, (filing_id,))
    pdf_pages = cursor.fetchall()
    data_cache.indiv_filings[filing_id]['PDFPageText'] = pdf_pages

    print(f'Got filing_id {filing_id} PDF documents and pages.')
    print(f'Loaded SEC staff action/letter filing data for filing_id {filing_id}.')

def get_masterfiling_info(filing_id):
    """Fetch and cache MasterFiling info for an individual filing."""
    print(f'Refreshing indiv_filings dictionary MasterFiling summary for filing_id: {filing_id}.')
    if filing_id not in data_cache.indiv_filings: # Check filing info is not already cached
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Query the MasterFiling table for the filing details
                cursor.execute("""
                    SELECT filing_id, cik, type, date, accession_number, company_name, 
                        sic_code, sic_desc, report_period, state_of_incorp, 
                        fiscal_yr_end, business_address, business_phone, name_changes
                    FROM MasterFiling
                    WHERE filing_id = ?;
                """, (filing_id,))
                result = cursor.fetchone()

                # If the filing exists, return a dictionary
                if result:
                    data_cache.indiv_filings[filing_id] = {
                        'filing_id': result[0],
                        'cik': result[1],
                        'type': result[2],
                        'date': result[3],
                        'accession_number': result[4],
                        'company_name': result[5],
                        'sic_code': result[6],
                        'sic_desc': result[7],
                        'report_period': result[8],
                        'state_of_incorp': result[9],
                        'fiscal_yr_end': result[10],
                        'business_address': result[11],
                        'business_phone': result[12],
                        'name_changes': result[13]
                    }
                    print(f'Fetched filing summary for filing_id: {filing_id}.')
                else:
                    data_cache.indiv_filings[filing_id] = []
                    print(f'MasterFiling query returned empty for filing_id: {filing_id}.')

                conn.close()
                print(f'Refreshed indiv_filings dictionary MasterFiling summary for filing id {filing_id}.')

        except Exception as e:
            print(f"Error fetching individual filing_id {filing_id} MasterFiling info: {e}.")
            data_cache.indiv_filings[filing_id] = []  # Empty list if error occurs

    else:
        print(f'indiv_filings dictionary MasterFiling summary already populated for filing_id {filing_id}, skipping refresh.')

def get_filing_data(filing_id):
    """
    Dispatches to the appropriate data loading function based on filing type, caching results.
    """
    print(f'Refreshing full indiv_filings dictionary for filing_id {filing_id}.')
    get_masterfiling_info(filing_id) # Refresh incase somehow hasn't been called
    filing_type = data_cache.indiv_filings[filing_id].get('type')

    # Only query if this filing's data has not been previously successfully cached
    if not data_cache.indiv_filings[filing_id].get('full_cached'):
        try:
            conn = get_db_connection()
            if conn:

                # Map filing types to loader functions
                filing_type_to_loader = defaultdict(
                    lambda: None,  # Default to None for unsupported types
                    {
                        '10-Q': load_financial_filing_data,
                        '10-Q/A': load_financial_filing_data,
                        '10-K': load_financial_filing_data,
                        '10-K/A': load_financial_filing_data,
                        '6-K': load_financial_filing_data,
                        '6-K/A': load_financial_filing_data,
                        #'13F-HR': load_holdings_filing_data,
                        #'13F-NT': load_holdings_filing_data,
                        '8-K': load_event_filing_data,
                        '8-K/A': load_event_filing_data,
                        'S-1': load_text_filing_data,
                        'S-1/A': load_text_filing_data,
                        'S-3': load_text_filing_data,
                        'S-3/A': load_text_filing_data,
                        'DEF 14A': load_text_filing_data,
                        'DEFA14A': load_text_filing_data,
                        'DEF 14A/A': load_text_filing_data,
                        'SEC STAFF ACTION': load_sec_staff_filing_data,
                        'SEC STAFF LETTER': load_sec_staff_filing_data
                    }
                )

                loader_function = filing_type_to_loader[filing_type]
                if loader_function:
                    print(f'Chose loader for filing type {filing_type}.')
                    loader_function(filing_id, conn)
                    print(f'Fetched filing data for filing_id {filing_id}.')
                    data_cache.indiv_filings[filing_id]['full_cached'] = True
                else:
                    print(f"Unsupported filing type in get_filing_data: {filing_type}, filing_id {filing_id}.")
                
                conn.close()
                
        except Exception as e:
            print(f'Error refreshing full indiv_filings dictionary for filing_id {filing_id}: {e}.')

    else:
        print(f'indiv_filings dictionary already fully populated for filing_id {filing_id}, skipping refresh.')

def get_industry_text_sections(sic):
    '''
    Attempts to retrieve and cache, in a list, all text sections of filings belonging to the specified industry.
    '''
    if sic not in data_cache.industry_text_sections:  # Only query if this SIC is not in cache
        print(f'Attempting to grab + cache text sections for new industry: {sic}.')
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT tsf.*
                    FROM TextSectionFacts tsf
                    LEFT JOIN TextDocument td on tsf.text_document_id = td.text_document_id
                    LEFT JOIN MasterFiling mf on td.filing_id = mf.filing_id
                    WHERE mf.sic_code = ?
                """, (sic,))
                text_section_results = cursor.fetchall()

                # Store fetched results
                data_cache.industry_text_sections[sic] = text_section_results
                conn.close()

                print(f'Cached text sections for SIC {sic}.')

        except Exception as e:
            print(f"Error fetching text sections for SIC {sic}: {e}.")
            data_cache.industry_text_sections[sic] = []  # Empty list if error occurs
