from azure.storage.blob import BlobServiceClient
import pandas as pd
import pyodbc
import config

# Prevent unnecessary queries etc...
class DataCache:
    csv_summary = {} # Will hold summary data regarding filers and filing types of all filings of the day, parsed or not 
    sql_summary = {} # Will hold summary data regarding filers, filing types, and industries 
    sic_summary = {} # Will hold summary data regarding filers and filing types, organized by industry
    fin_summary = {} # Will hold summary data regarding financial report type filings, organized by specific filing type
    prospectus_summary = {} # Will hold summary data regarding prospectus summary type filings, organized by specific filing type
    events_summary = {} # Will hold summary data regarding 8K/current event filings

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
                        'by_company': [{'company_name': row[0], 'count': row[1]} for row in filings_by_company],
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
                        tsf.section_name AS TextSectionName
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

def refresh_events_summary():
    """Fetch and cache 8K/current event filing summaries."""
    print('Refreshing events_summary dictionary.')
    if not data_cache.events_summary:  # Only query if events_summary is empty
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()

                # Initialize the events_data structure
                events_data = {
                    'by_industry': {},
                    'by_item': {}
                }
            
                # Query to fetch all 8-K filings and their details, including filer_cik
                cursor.execute('''
                    SELECT mf.company_name, mf.date, mf.business_address, mf.accession_number,
                        mf.sic_desc, mf.cik, e.event_id
                    FROM MasterFiling mf
                    JOIN Event8K e ON mf.filing_id = e.filing_id
                ''')
                
                filings = cursor.fetchall()

                for (company_name, date, business_address, accession_number,
                    sic_desc, cik, event_id) in filings:
                    # Query to fetch items listed for this event
                    cursor.execute('''
                        SELECT item_name
                        FROM Event8KItems
                        WHERE event_id = ?
                    ''', (event_id,))
                    
                    items_listed = [row[0] for row in cursor.fetchall()]

                    # Create the filing record
                    filing_record = {
                        'company_name': company_name,
                        'date': date,
                        'items_listed': items_listed,
                        'business_address': business_address,
                        'accession_number': accession_number,
                        'cik': cik
                    }

                    # Populate by_industry with count
                    if sic_desc not in events_data['by_industry']:
                        events_data['by_industry'][sic_desc] = {
                            'filings': [],
                            'count': 0
                        }
                    events_data['by_industry'][sic_desc]['filings'].append(filing_record)
                    events_data['by_industry'][sic_desc]['count'] += 1


                    # Populate by_item with count
                    for item in items_listed:
                        if item not in events_data['by_item']:
                            events_data['by_item'][item] = {
                                'filings': [],
                                'count': 0
                            }
                        events_data['by_item'][item]['filings'].append({
                            'company_name': company_name,
                            'date': date,
                            'items_listed': items_listed,
                            'accession_number': accession_number,
                            'cik': cik,
                            'sic_desc': sic_desc
                        })
                        events_data['by_item'][item]['count'] += 1

                # Sort by count
                events_data['by_industry'] = dict(sorted(events_data['by_industry'].items(), key=lambda item: item[1]['count'], reverse=True))
                events_data['by_item'] = dict(sorted(events_data['by_item'].items(), key=lambda item: item[1]['count'], reverse=True))

                # Store in the cache
                data_cache.events_summary = events_data

                conn.close()
                print('Refreshed events_summary dictionary.')
        
        except Exception as e:
            print(f"Error fetching current events filings summary: {e}.")
            data_cache.events_summary = []  # Empty list if error occurs

    else:
        print(f'events_summary already populated, skipping refresh.')
