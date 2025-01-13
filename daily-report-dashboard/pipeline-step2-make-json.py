#%pip install --upgrade pymupdf beautifulsoup4 lxml
#%pip install azure-storage-blob

from datetime import date, datetime
from zoneinfo import ZoneInfo
import logging
import json
import os
import re
import sys
from azure.storage.blob import BlobServiceClient
from logging.handlers import TimedRotatingFileHandler
import unicodedata
import uu
import pymupdf
import io
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
import config

# Configure logging
log_filename = f"pipeline2_{datetime.now().strftime('%Y-%m-%d')}.log"
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

'''
11/28/24

Parser objects, written per filing type. Currently targetting:
    - 10-Q + 10-K + 6-K
    - 13F
    - 13G + 13D
    - S-1 + S-3
    - 8K
    - Form 4
    - Proxy statements
    - SEC actions+letters
    - TODO: 20-F, 40-F, 11-K, N-CSR, N-Q, N-PORT
'''

'''
Base class. Is able to read the SEC header (standard across filing types) and search for filing documents
Returns a dictionary of structure:
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    }
}
'''
class SECFulltextParser:

    def __init__(self, fulltext):
        self.filing_info = self.parse_sec_header(fulltext)

    ''' 
    Break filing into documents. Returns a list of dictionaries, one per document, of the following format:
    {
        'doc_type': '...',
        'doc_sequence': '...',
        'doc_filename': '...',
        'doc_desc': '...',
        'doc_text': '...'
    }
    '''
    def split_filing_documents(self):

        docs_found = []
        document_pattern = r'<document>(.*?)</document>'
             
        doc_info_patterns = {
            'doc_type': r'\s*<type>(?P<doc_type>.*?)\s*<',
            'doc_sequence': r'\s*<sequence>(?P<doc_sequence>\d+)\s*<',
            'doc_filename': r'\s*<filename>(?P<doc_filename>.*?)\s*<',
            'doc_desc': r'\s*<description>(?P<doc_desc>.*?)\s*<',  
            'doc_text': r'\s*<text>(?P<doc_text>.*?)</text>\s*'
        }   

        # Find all filing documents
        try:
            documents = re.findall(document_pattern, self.filing_info['filing_raw_text'], re.DOTALL | re.IGNORECASE)
        except Exception as e:
            logging.error(f'Failed to find <DOCUMENT> tags in the current filing. Error: {e}')
            documents = []

        # Iterate through each document
        for doc in documents:
            
            doc_info = {}
            doc_info['doc_desc'] = '' # May not be present, just initialize for uniformity

            for key, pattern in doc_info_patterns.items():
                match = re.search(pattern, doc, re.DOTALL | re.IGNORECASE)

                if match:
                    doc_info[key] = match.group(key).strip()
                else:
                    if key != 'doc_desc':
                        logging.warning(f'Failed to find SEC <document> section: {key}\nDocument contents: {doc[:100]}')

            docs_found.append(doc_info)

        return docs_found
    
    # Helper for below, traversing filing. Returns the first match found.
    def search_filing_for_doc(self, doc_name):

        logging.info(f'Searching for document in filing: {doc_name}')

        doc_info = {}
        documents = self.split_filing_documents()
        if not documents:
            logging.error('Failed to split filing into documents to search for {doc_name}')
            return doc_info
        
        try:
            for doc in documents:

                if doc['doc_filename'].lower() == doc_name.lower():
                    doc_info = doc
                    break
        except Exception as e:
            logging.error(f'Failed iterating list of documents returned by split_filing_documents(). Error: {e}.')

        return doc_info
    
    # Helps traverse filing full text. Search documents by filename. 
    # (Returns contents within <TEXT>...</TEXT> tags for the found document)
    def search_filing_for_doc_text(self, doc_name):

        doc_text = ''
        doc_info = self.search_filing_for_doc(doc_name)
        if not doc_info:
            return doc_text
    
        try:
            doc_text = doc_info['doc_text']
        except Exception as e:
            logging.error(f'Failed parsing doc_info from search_filing_for_doc(). doc_name: {doc_name}, Error: {e}.')

        return doc_text

    # Extracts the SEC header (<SEC-HEADER>...</SEC-HEADER>) from the full text of a filing. Helper method
    def extract_sec_header(self, fulltext_contents):
        # Regex pattern to capture the SEC header
        pattern = r'(<sec-header>.*?</sec-header>)'
        
        # Search for the SEC header in the full text
        try:
            match = re.search(pattern, fulltext_contents, re.DOTALL | re.IGNORECASE) 
        except Exception as e:
            logging.error(f'Failed regex search for SEC header. Error: {e}.')
            return None

        if match:
            return match.group(0)  # Return the entire match, which includes the tags
        else:
            return None  # Return None if no SEC header is found
    
    '''
    Parses the filing's SEC header into a dictionary filing_info
    {  
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_number': '...',
        # Optional fields from here on (method won't fail if it can't fill them)
        'cik': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'company_name': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...],
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    }
    '''
    def parse_sec_header(self, fulltext_contents):

        filing_info = {
            'type': None,
            'date': None,
            'accession_number': None,
            'cik': None,
            'sic_code': None,
            'sic_desc': None,
            'company_name': None,
            'report_period': None,
            'state_of_incorp': None,
            'fiscal_yr_end': None,
            'business_address': None,
            'business_phone': None,
            'name_changes': [],
            'header_raw_text': None,
            'filing_raw_text': None
        }
        header_text = self.extract_sec_header(fulltext_contents)

        if not header_text:
            logging.error('Failed to extract SEC header from filing content.')
            return filing_info
        filing_info['header_raw_text'] = header_text
        filing_info['filing_raw_text'] = fulltext_contents

        # Extract required fields
        required_patterns = {
            'accession_number': r'accession number:\s+([^\n]+)',
            'type': r'form type:\s+([^\n]+)',
            'date': r'filed as of date:\s+(\d{8})'
            # (CIK + SIC will be grabbed later on. If they cannot be found, xxxxxxxxxx and xxxx will be used, respectively, for unknowns)
        }

        for key, pattern in required_patterns.items():
            match = re.search(pattern, header_text, re.IGNORECASE)
            if match:
                filing_info[key] = match.group(1).strip()
            else:
                logging.error(f'Failed to parse SEC header required field: {key}.')
                return filing_info  # Early exit. Something is wrong lol
        
        # Report period if applicable
        report_period_match = re.search(r'\s*conformed period of report:\s*(.+?)', header_text, re.IGNORECASE)
        if report_period_match:
            filing_info['report_period'] = report_period_match.group(1).strip()
        else:
            logging.warning('Header field not found: report_period')  
            filing_info['report_period'] = None
        
        # We try to ensure we only grab the filer / filed by / issuer company, not a subject company.
        # Filings such as 13D/G, hold company data on both subject companies and filer companies.
        filed_by_match = re.search(r'filed by:', header_text, re.IGNORECASE)
        filer_match = re.search(r'filer:', header_text, re.IGNORECASE)
        issuer_match = re.search(r'issuer:', header_text, re.IGNORECASE)

        if filed_by_match:
            remaining_text = header_text[filed_by_match.end():]  # Extract text after 'filed by:'
        elif filer_match:
            remaining_text = header_text[filer_match.end():]  # Extract text after 'filer:'
        elif issuer_match:
            remaining_text = header_text[issuer_match.end():]
        else:
            remaining_text = header_text  # Fallback in case neither section is present
        
        # Some company data patterns
        company_data_patterns = {
            'company_name': r'\s*COMPANY CONFORMED NAME:\s*(?P<company_name>.+?)\s*(?:\n|$)',
            'cik' : r'\s*CENTRAL INDEX KEY:\s*(?P<cik>\d+)\s*(?:\n|$)',
            'sic_whole' : r'\s*STANDARD INDUSTRIAL CLASSIFICATION:\s*',
            'state_of_incorp': r'\s*STATE OF INCORPORATION:\s*(?P<state_of_incorp>\w+)\s*(?:\n|$)',
            'fiscal_yr_end': r'\s*FISCAL YEAR END:\s*(?P<fiscal_yr_end>\d{4})\s*(?:\n|$)',
            'business_address': r'\s*BUSINESS ADDRESS:\s*',
            'business_phone': r'\s*BUSINESS PHONE:\s*(?P<business_phone>.+?)\s*(?:\n|$)'
        }

        for key, pattern in company_data_patterns.items():

            match = re.search(pattern, remaining_text, re.IGNORECASE)

            if match:

                # Address field takes some additional processing
                if key == 'business_address':
                    
                    business_address_patterns = {
                        'street1': r"\s*STREET\s+1:\s*(?P<street1>.*?)\s*(?:\n|$)",
                        'street2': r"\s*STREET\s+2:\s*(?P<street2>.*?)\s*(?:\n|$)",
                        'city': r"\s*CITY:\s*(?P<city>.*?)\s*(?:\n|$)",
                        'state': r"\s*STATE:\s*(?P<state>\w+)\s*(?:\n|$)",
                        'zip': r"\s*ZIP:\s*(?P<zip>\d+)"
                    }

                    business_address_info = {}

                    for key, pattern in business_address_patterns.items():
                        match = re.search(pattern, remaining_text, re.IGNORECASE | re.DOTALL)
                        if match:
                            business_address_info[key] = match.group(key).strip()
                        else:
                            logging.warning(f'Business address component not found: {key}')

                    # Construct the address
                    components = [component for component in business_address_info.values() if component]
                    filing_info['business_address'] = ', '.join(components)

                # SIC needs to be split into code and description
                elif key == 'sic_whole':
                    
                    sic_pattern = r"\s*STANDARD INDUSTRIAL CLASSIFICATION:\s*(?P<sic_description>.+?)\s*\[(?P<sic_code>\d{4})\]"

                    # Search for the SIC
                    match = re.search(sic_pattern, remaining_text, re.IGNORECASE)

                    if match:
                        filing_info['sic_desc'] = match.group('sic_description').strip()
                        filing_info['sic_code'] = match.group('sic_code').strip()
                    else:
                        filing_info['sic_desc'] = None
                        filing_info['sic_code'] = 'XXXX'
                        logging.warning('Failed to parse sic_code or sic_desc')
                
                else:
                    filing_info[key] = match.group(1).strip()
            else:
                logging.warning(f'Header field not found: {key}')
                if key == 'cik':
                    filing_info[key] = 'XXXXXXXXXX'
                elif key == 'sic_whole':
                    filing_info['sic_desc'] = None
                    filing_info['sic_code'] = 'XXXX'

        # Lastly, grab any former company names
        former_name_pattern = r"\s*FORMER COMPANY:\s*FORMER CONFORMED NAME:\s*(?P<former_name>.+?)\s*DATE OF NAME CHANGE:\s*(?P<date_of_change>\d{8})"

        # Find all matches
        matches = re.finditer(former_name_pattern, remaining_text, re.IGNORECASE | re.DOTALL)

        for match in matches:
            filing_info['name_changes'].append({
                'former_name': match.group('former_name').strip(),
                'date_of_change': match.group('date_of_change').strip()
            })

        logging.info('Finished parsing filing SEC header')
        return filing_info

    def full_parse(self):
        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
        }
    
'''
10-Q, 10-K, 6-K parser
Adds financial_statements and text_section fields to returned dict:
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'financial_statements': [
        {
            'report_doc': '...',
            'report_name': '...',
            'report_title_read': '...',
            'report_raw_text': '...',
            'report_parsed_data': {...},
            'report_df': ...
        },
        ...
    ],
    'text_sections': [
        {
            'section_doc': '...',
            'section_name': '...',
            'section_type': '...',
            'section_raw_text': '...',
            'section_parsed_text': {...} / '...'
        },
        ...
    ],
}
'''
class FinancialFilingParser(SECFulltextParser):

    def __init__(self, fulltext):
        # Init self.filing_info
        super().__init__(fulltext) 

        self.financial_statements = []
        self.text_sections = []

    '''
    Returns a list of dictionaries about each of the reports found in the given FilingSummary.xml contents
    Structure of each dict:
    {
        'menucategory': '...',
        'shortname': '...',
        'longname': '...',
        'doc_name': '...',
        'role': '...',
        'position': '...'
    }
    '''
    def list_xbrl_reports(self, filingsummary, report_category=None):

        reports_found = []
        
        # Find MyReports and loop through them
        summary_soup = BeautifulSoup(filingsummary, 'xml')

        reports = summary_soup.find(re.compile('myreports', flags=re.IGNORECASE))
        if not reports:
            logging.warning('Failed to find <myreports> tag for filing.')
            return reports_found

        for report in reports.find_all(re.compile(r'^(?:\w+:)?report$', flags=re.IGNORECASE)):

            tag_regex = lambda tag: re.compile(rf'^(?:\w+:)?{tag}$', flags=re.IGNORECASE)

            report_info = {}    
            report_info['menu_category'] = report.find(tag_regex('MenuCategory')).get_text() if report.find(tag_regex('MenuCategory')) else None
            if report_category:
                if not report_info['menu_category']:
                    logging.info(f'Passing on report, has no MenuCategory set but filter is set to {report_category}.')
                    continue
                if report_category.lower() != report_info['menu_category'].lower():
                    logging.info(f'Passing on report, non-target menu category. Desired: {report_category.lower()}, found: {report_info["menu_category"].lower()}.')
                    continue

            report_info['short_name'] = report.find(tag_regex('ShortName')).get_text() if report.find(tag_regex('ShortName')) else None
            report_info['long_name'] = report.find(tag_regex('LongName')).get_text() if report.find(tag_regex('LongName')) else None
            report_info['doc_name'] = report.find(tag_regex('HtmlFileName')).get_text() if report.find(tag_regex('HtmlFileName')) else None
            report_info['role'] = report.find(tag_regex('Role')).get_text() if report.find(tag_regex('Role')) else None
            report_info['position'] = report.find(tag_regex('Position')).get_text() if report.find(tag_regex('Position')) else None

            reports_found.append(report_info)
            logging.info(f'Found report of interest: {report_info["short_name"]}.')

        return reports_found

    # Parse the given table, expected to holding financial statement data
    def parse_report_table(self, table_soup):
        
        report_data = {}
        report_data['headers'] = []
        report_data['sections'] = [] 
        report_data['data'] = []
        
        # Loop through table's rows 
        for row_index, current_row in enumerate(table_soup.find_all('tr')):

            ######## TESTING
            logging.info(f'Parsing row #{row_index + 1} of report table. Row text: {current_row.text}')
            ########
            
            # If there is a sub-table, skip for now. TODO: Possibly parse/deal with
            if current_row.find('table'):
                logging.warning(f'Sub-table encountered on row #{row_index + 1}, skipping.')
                continue
            
            # If we come across a row of class "rh" (signaling an end of the aggregated/totals and beginning of member/source breakdowns), end here. TODO: Parse
            if "rh" in current_row.get('class', []):
                logging.warning(f"Member/source-specific information encountered on row #{row_index + 1}. Ending table parsing here.")
                break

            # Skip rows that contain "note" in any of their class values (substring search). These rows are generally footnotes. TODO: Optimize/store somewhere
            skip_row = False
            for class_value in current_row.get('class', []):
                if "note" in class_value.lower():
                    skip_row = True
                    break

            if skip_row:
                logging.info(f"Skipping row #{row_index + 1} due to string 'note' in class.")
                continue

            # Grab all the elements / columns of the row. Keep in mind column headers don't have <td> tags, thus we list <th> objects within the elif upon finding them
            line_columns = current_row.find_all('td')

            # Decide if row is: data, section (sub) header, or column header according to logic above (th and strong tags) 
            # and append it to the proper list (headers, sections, or data) of the statement_data dictionary.

            # Data row. May be a footnote or superscript, which we will filter out
            if not current_row.find('th') and not current_row.find('strong'):
                data_row = [] 

                ####### TESTING
                logging.info(f'Parsing data row (row #{row_index + 1}) of report table. line_columns: {line_columns}')
                #######
        
                # Skip superscripted values/columns (usually link to footnote) and those with class of "fn", again link to footnote
                for column_index, current_column in enumerate(line_columns):

                    ####### TESTING
                    logging.info(f'Parsing column #{column_index + 1} of #{row_index + 1}. Text: {current_column.text}')
                    #######

                    if len(current_column.find_all('sup')) == 0:
                        if "fn" not in current_column.get('class', []):
                            data_column = current_column.text.strip()
                            data_row.append(data_column)

                            ######## TESTING
                            logging.info(f'Appended column text to row list. Text appended: {data_column}')
                            ########

                report_data['data'].append(data_row)

            # Section/sub header row
            elif (len(current_row.find_all('th')) == 0 and len(current_row.find_all('strong')) != 0):
                section_row = line_columns[0].text.strip() # Only the first element in this row will have the section label, the others are blank so no point
                report_data['sections'].append(section_row)

            # Header row
            elif len(current_row.find_all('th')) != 0:
                header_row = []

                # Again, skip superscripted columns. TODO Potentially record these somewhere
                for current_column in current_row.find_all("th"):
                    if len(current_column.find_all("sup")) == 0:
                        
                        header_column = current_column.text.strip()
                        header_row.append(header_column)

                report_data['headers'].append(header_row)

            # Unable to identify
            else:
                logging.warning(r"Unable to identify row #{} of table found in table of financial report.".format(row_index + 1))

        # TODO remove any newline characters in the columns or section headers
        
        return report_data

    # Attempts to extract data from the given XBRL financial/statement report
    def parse_xbrl_fin_report(self, report_content):

        # Dictionary we will return
        report_data = {}
        report_data['headers'] = []
        report_data['sections'] = []
        report_data['data'] = []

        # Soupify report
        report_soup = BeautifulSoup(report_content, 'html.parser')

        # In case there are multiple tables in the document, loop through all of those labeled with the "report" class, saving the last. TODO: Further test and optimize if needed
        for table_index, current_table in enumerate(report_soup.find_all('table', class_ = "report")):
            logging.info(f'Parsing financial report table #{table_index + 1}.')
            report_data = self.parse_report_table(current_table)
        
        # Return the filled dictionary
        return report_data
    
    # Makes the given list of strings unique, appending _X as they increase in count
    def make_unique_string_list(self, list_of_strings):

        unique_list = []
        existing_counts = {}

        if list_of_strings:
            for current_string in list_of_strings:
                if current_string in existing_counts:
                    existing_counts[current_string] += 1
                    unique_list.append(f"{current_string}_{existing_counts[current_string]}")
                else:
                    existing_counts[current_string] = 0
                    unique_list.append(current_string)
        else:
            logging.warning(f'Empty list of strings passed to make_unique method. Object passed: {list_of_strings}.')
        return unique_list
    
    # Attempts to store the data from the parsed financial report dictionary into a pandas dataframe
    def parsed_fin_report_to_df(self, parsed_fin_report):

        # Create two lists: headers (column headings), and data values. Ignore section headers for now
        try:
            report_headers_list = parsed_fin_report['headers']
            report_data_list = parsed_fin_report['data']
        except Exception as e:
            logging.error(f'Passed invalid parsed_fin_report dict. Error: {e}.')
            return None

        # Check that data has been passed. Can be empty if the report starts off with an "rh" row, usually seen in parenthetical statements regarding member entities etc.
        if not report_data_list:
            logging.warning('An empty report was passed to parsed_fin_report_to_df(). DF will be None.')
            return None

        # Create the dataframe around the report data list
        report_df = pd.DataFrame(report_data_list)

        # Create a dictionary to count occurrences of each line item / financial account
        name_count = {}

        # Iterate through the first column to rename duplicates
        for index in range(len(report_df)):
            item_name = report_df.iloc[index, 0]  # Get the name in the first column
            if item_name in name_count:
                name_count[item_name] += 1
                # Rename the item with an index suffix
                report_df.iloc[index, 0] = f"{item_name}_{name_count[item_name]}"
            else:
                name_count[item_name] = 1  # Initialize count for the first occurrence

        # Set the DF index
        report_df.index = report_df[0]
        report_df.index.name = 'account_name'
        report_df = report_df.drop(0, axis=1)

        # Sanitize it of illegal characters
        report_df = report_df.replace('[\[\]\$,)]', '', regex = True)\
            .replace('[(]', '-', regex = True)\
            .replace('', 'NaN', regex = True)

        # Convert data values to floats. "Unlimited" and other text may be present, so ignore for now. Could convert them to NaNs also
        report_df = report_df.astype(dtype = float, errors = 'ignore')

        # Drop rows with all NaN's
        #report_df = report_df.dropna(how="all")
        
        # Set column names to the headers we stored. Remember we have a list of lists. Do some cleaning
        # If there is only one list/row of column headers, we want to drop the first element (which basically holds the table name). Otherwise rely on the last row to be the dates / headings we want.
        # TD-DO: Better optimization for multi-line headers etc, also integrate section headers 

        try:
            headers_list = []

            if len(report_headers_list) == 1:
                headers_list = self.make_unique_string_list(report_headers_list[0][1:])

            elif len(report_headers_list) > 1:
                headers_list = self.make_unique_string_list(report_headers_list[-1])

            else:
                logging.warning(f'Unexpected fin report header structure. Possibly empty. Object: {report_headers_list}.\nDataframe structure will be incomplete.')

            report_df = report_df.set_axis(headers_list, axis='columns')

        except Exception as e:
            logging.error(f"Failed to read/set column headers for dataframe of financial report. Error: {e}.")

        return report_df
    
    # At the moment am relying on XBRL enabled filings which contain 'reports' organizing financial statements (see github/cchummer/sec-api)
    def parse_financial_statements(self):
        
        reports_list = []

        logging.info('Attempting to parse financial statements')
        
        # Check that a FilingSummary.xml exists. It will contain information on reports present
        reports_summary = self.search_filing_for_doc_text('filingsummary.xml')
        if not reports_summary:
            logging.info('No FilingSummary.xml was found.')
            # TODO implement manual HTML table parsing (or pd.read_html())
            return reports_list
        
        # Find reports
        found_reports = self.list_xbrl_reports(reports_summary, report_category='statements')
        if not found_reports:
            logging.info('No financial statement reports found.')
            return reports_list
        
        # Parse/scrape them
        for fin_report in found_reports:

            logging.info(f'Parsing report: {fin_report["short_name"]}.')

            report_text = self.search_filing_for_doc_text(fin_report['doc_name'])
            if report_text:
                scraped_report = self.parse_xbrl_fin_report(report_text.lower())
            else:
                logging.warning('Failed to find report document in filing full text.')
                scraped_report = {}

            # Save report info so far
            report_dict = {}
            report_dict['report_doc'] = fin_report['doc_name']
            report_dict['report_name'] = fin_report['short_name'] 
            report_dict['report_raw_text'] = None
            report_dict['report_parsed_data'] = scraped_report
            try:
                report_dict['report_title_read'] = scraped_report['headers'][0][0]
            except:
                logging.warning(f'Unable to read report title from parsed contents. Report name: {fin_report["short_name"]}.')
            
            # Save to dataframe
            report_dict['report_df'] = None
            report_df = self.parsed_fin_report_to_df(scraped_report)
            if report_df is not None and not report_df.empty:
                try:
                    report_dict['report_df'] = report_df.to_json(orient='index')
                except Exception as e:
                    logging.error(f'Exception thrown writing {fin_report["short_name"]} to dictionary. Error: {e}.')
            else:
                logging.warning(f'Parsed report {fin_report["short_name"]} DF was returned empty. No data being saved.')

            reports_list.append(report_dict)
            logging.info(f'Finished parsing report {fin_report["short_name"]}.')

        return reports_list
    
    # Helper functions for cleaning filing text
    # unicode.normalize leaves behind a couple of not technically whitespace control-characters. See https://www.geeksforgeeks.org/python-program-to-remove-all-control-characters/ and http://www.unicode.org/reports/tr44/#GC_Values_Table
    def remove_control_characters(self, s):
        return "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    
    # Cleans the given text (specifically: unicode normalize and turn newlines/whitespace into a single space)
    def clean_filing_text(self, text_to_clean):

        clean_text = unicodedata.normalize('NFKD', text_to_clean)
        clean_text = self.remove_control_characters(clean_text)
        clean_text = clean_text.replace('\n', ' ') # Split doesn't catch newlines from my testing
        clean_text = " ".join(clean_text.split()) # Split string along tabs and spaces, then rejoin the parts with single spaces instead

        return clean_text
    
    # Attempts to extract data from the given XBRL 'notes' report, mainly targetting text data
    # TODO better handling of tables in these reports
    def parse_xbrl_note_report(self, note_text):
        
        # Returned structure
        table_data = {
            "header_vals" : [],
            "text_vals" : []
        }

        report_soup = BeautifulSoup(note_text, 'html.parser')

        # In case there are multiple tables in the document, loop through all of those labeled with the "report" class. Tables of other classes (i.e. of type "authRefData") are ignored.
        for table_index, current_table in enumerate(report_soup.find_all('table', class_ = "report")):

            # Loop through rows
            for row_index, current_row in enumerate(current_table.find_all('tr')):

                # Header row if <th> element is found
                header_columns = current_row.find_all('th')
                if header_columns:

                    # Strip the text from each column and append it to headers master list
                    for hdr_column in header_columns:
                        table_data["header_vals"].append(hdr_column.text.strip())
                
                # Not a header row, look for columns of class "text"
                else:

                    # Strip the text from each column and append it to text_vals master list
                    # TODO: Optimize for tables within note. Formatting is a bit janky / unpreserved right now
                    for txt_column in current_row.find_all('td', class_ = "text"):

                        # Loop through the children of the text column
                        for child in txt_column.children:

                            # Ignore empty paragraphs/spacers
                            child_text = self.clean_filing_text(child.text.strip())
                            if len(child_text):
                                table_data["text_vals"].append(child_text)

        return table_data
    
    # Helper function to below
    # Attempts to locate a table of contents by looking for a <table> element containing one or more <href> elements
    # Returns the bs4.Element.Tag object of that table if it exists, or None
    def linked_toc_exists(self, document_soup):

        # Find all <table> tags
        all_tables = document_soup.find_all('table')
        for cur_table in all_tables:

            # Look for an <a href=...>
            links = cur_table.find_all('a', attrs = { 'href' : True })
            if len(links):
                return cur_table

        return None
    
    # Helper method to find_section_with_toc, extracts the text found inbetween 2 bs4 Tags/elements
    def text_between_tags(self, start, end):

        cur = start
        found_text = ""

        # Loop through all elements inbetween the two
        while cur and cur != end:
            if isinstance(cur, NavigableString):

                text = cur.strip()
                if len(text):
                    found_text += "{} ".format(text)

            cur = cur.next_element

        return self.clean_filing_text(found_text.strip()) # Strip trailing space that the above pattern will result in
    
    # Helper method to find_section_with_toc, extracts the text found starting at a given tag through the end of the soup
    def text_starting_at_tag(self, start):

        cur = start
        found_text = ""

        # Loop through all elements
        while cur:
            if isinstance(cur, NavigableString):

                text = cur.strip()
                if len(text):
                    found_text += "{} ".format(text)

            cur = cur.next_element

        return self.clean_filing_text(found_text.strip())
    
    # Support method for find_section_with_toc, attempt to determine if the given text is simple a page number (duplicate link in my observations)
    def is_text_page_number(self, question_text):

        # Check argument
        if type(question_text) != str:
            logging.warning("Non-string passed to is_text_page_number. Returning True (will result in href being skipped)")
            return True

        # Strip just to be sure
        stripped_question_text = question_text.strip()

        # Check if text is only digits
        if stripped_question_text.isnumeric():
            return True

        # Check if only roman numerals
        valid_romans = ["M", "D", "C", "L", "X", "V", "I", "(", ")"]
        is_roman = True
        for letter in stripped_question_text.upper():
            if letter not in valid_romans:
                is_roman = False
                break

        return is_roman
        
    """
    Use the hyperlinked TOC to find the given text section. Provide a bs4 Tag object for the located TOC. Returns a list of dictionaries:
    [
        {
        "section_name": "...",
        "section_raw_text": "...",
        "section_parsed_text": "..."
        },
        ...
    ]
    """
    def find_sections_with_toc(self, document_soup, toc_soup):

        # Returned list
        section_list = []

        # First, loop through the <a> tags of the TOC and build a dictionary of href anchor values and text (sections) values
        link_dict = {}
        link_tags = toc_soup.find_all('a', attrs = { 'href' : True })
        for link_tag in link_tags:

            # From some TOC's I have examined, there may be a second <a href...> for each section, labeled instead by the page number. This page number may be a digit or a roman numeral
            # If I come across a filing with a different TOC strcture, I will find a more nuanced way to handle it. For now simply check if the text is only digits or roman numerals
            # Some TOC's also look to have a third link to each section, on the far left of the table and with the text "Item 1, Item 2, ...". Again will update if these appear after the properly labeled links and thus
            # over-write that spot in the href dict defined below. As of now we are relying on the properly/fully labeled links being the last non-page-number reference to each href in order to be recorded.
            if self.is_text_page_number(link_tag.text.strip()):
                continue

            link_dict[link_tag.get('href').replace('#', '')] = self.clean_filing_text(link_tag.text.strip())

        # Grab a list of destination anchors (<a> or <div> tags with "id" or "name" attribute)
        link_dests = document_soup.find_all('a', attrs = { 'id' : True }) + document_soup.find_all('a', attrs = { 'name' : True })\
        + document_soup.find_all('div', attrs = { 'id' : True }) + document_soup.find_all('div', attrs = { 'name' : True })

        # Filter out those which are never linked to, they will obstruct our logic in text_between_tags as we rely on the next anchor to be the beginning of the next section
        # I have run into filings with such "phantom" anchors that are never linked to and can prematurely signal the end of a section
        # (i.e: https://www.sec.gov/Archives/edgar/data/1331451/000133145118000076/0001331451-18-000076.txt)
        link_dests = [anchor for anchor in link_dests if (anchor.get('id') in link_dict.keys() or anchor.get('name') in link_dict.keys())]

        # Now loop through the target sections that we just found links to. We will try to locate the destination of each
        for target_href, target_name in link_dict.items():

            # The href values are used at their destination in <a> tags with an id/name attribute of the same href value (minus the leading #, why we got rid of it)
            # Loop through the link_dests list of all destination tags, and find the one with id/name=target_href
            num_destinations = len(link_dests)
            for dest_index, link_dest in enumerate(link_dests):

                if (link_dest.get('id') == target_href or link_dest.get('name') == target_href): # Can be either id or name according to HTML spec (see https://stackoverflow.com/questions/484719/should-i-make-html-anchors-with-name-or-id)

                    # Grab the text inbetween the current destination tag and the next occuring destination in link_dests
                    # If we are on the last destination, grab all the text left
                    section_text = ""

                    if dest_index + 1 < num_destinations:
                        section_text = self.text_between_tags(link_dest, link_dests[dest_index + 1])
                    else:
                        section_text = self.text_starting_at_tag(link_dest)

                    if section_text:

                        section_info = {}
                        section_info['section_name'] = target_name
                        section_info['section_raw_text'] = None
                        section_info['section_parsed_text'] = section_text

                        # Add to master list
                        section_list.append(section_info)

        return section_list
    
    # Attempts to grab any existent notes to the financial statements, and all text sections
    def parse_text_sections(self):

        sections_list = []
        
        logging.info('Attempting to parse filing text sections.')

        # Look for notes accompanying financials, in XBRL enabled filings will be their own reports
        reports_summary = self.search_filing_for_doc_text('filingsummary.xml')
        if reports_summary:
            
            found_notes = self.list_xbrl_reports(reports_summary, report_category='notes')
            if found_notes:

                for note in found_notes:

                    logging.info(f'Parsing note to financial statement: {note["short_name"]}.')

                    note_text = self.search_filing_for_doc_text(note['doc_name'])
                    if note_text:
                        scraped_note = self.parse_xbrl_note_report(note_text.lower())
                    else:
                        logging.warning('Failed to find note document in filing full text.')
                        scraped_note = {}

                    text_section_info = {}
                    text_section_info['section_doc'] = note['doc_name']
                    text_section_info['section_name'] = note['short_name']
                    text_section_info['section_type'] = 'xbrl_note'         # Denotes difference in scraped_note format
                    text_section_info['section_raw_text'] = None
                    text_section_info['section_parsed_text'] = scraped_note # Will be a dictionary

                    sections_list.append(text_section_info)
                    logging.info(f'Finished parsing note: {note["short_name"]}.')

                logging.info('Finished parsing notes to financial statements.')
        
        # Now look for more traditional text sections
        # TODO: ATM, relying on a linked TOC to navigate filing sections. Develop more robust method
        
        # Loop through all documents in the filing
        docs_list = self.split_filing_documents()
        if not docs_list:
            logging.warning('Failed to split filing into documents')

        for doc_info in docs_list:

            # Only parse HTM/HTML files
            if not doc_info['doc_filename'].lower().endswith('.htm'):
                continue
            
            doc_html = BeautifulSoup(doc_info['doc_text'].lower(), "html.parser")

            # Will hold results from current document
            doc_sections = []

            logging.info(f'Parsing filing HTML document ({doc_info["doc_filename"]}) for TOC.')

            # Parse using TOC if it exists
            # TODO: BETTER PARSING / STORING OF TABLES FOUND IN TEXT SECTIONS
            toc_tag = self.linked_toc_exists(doc_html)
            if toc_tag:
                doc_sections = self.find_sections_with_toc(doc_html, toc_tag)
            else:
                logging.warning(f'Could not find a hyperlinked TOC to crawl for text sections. Parsing whole file {doc_info["doc_filename"]} as one section.')
                sections_list.append({
                    'section_doc': doc_info['doc_filename'],
                    'section_name': f'doc-{doc_info["doc_filename"]}',
                    'section_type': 'html_whole_doc',
                    'section_raw_text': None,
                    'section_parsed_text': doc_html.get_text(separator='\n', strip=True)
                })

            # We will enforce section name uniqueness on a document level
            existing_section_names = set()
            
            # Loop through results, add to the master dict
            if doc_sections:
                for result_section_data in doc_sections:

                    logging.info(f'Parsing {doc_info["doc_filename"]} section: {result_section_data["section_name"]}')

                    section_key_name = result_section_data['section_name']
                    i = 1
                    # Check for duplicates within the document using the set
                    while section_key_name in existing_section_names:
                        section_key_name = f"{section_key_name}_{i}"
                        i += 1
                    
                    # Add to dict after finding unused key
                    text_section_info = {}
                    text_section_info['section_doc'] = doc_info['doc_filename']
                    text_section_info['section_name'] = section_key_name
                    text_section_info['section_type'] = 'linked_toc_section'
                    text_section_info['section_raw_text'] = result_section_data['section_raw_text']
                    text_section_info['section_parsed_text'] = result_section_data['section_parsed_text'] # Will be a string in this case

                    sections_list.append(text_section_info)
                    existing_section_names.add(section_key_name)  # Update the set with the new name

                    logging.info(f'Finished parsing section: {result_section_data["section_name"]}')

        return sections_list

    def full_parse(self):

        self.financial_statements = self.parse_financial_statements()
        self.text_sections = self.parse_text_sections()

        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'financial_statements': self.financial_statements,
            'text_sections': self.text_sections
        }
    
    def __repr__(self):
        return 'FinancialFilingParser'
    
'''
13F parser
Returns the filing_info and a holdings report object for a 13F filing. Dict structure:
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'holdings_report': {
        'report_yr_quarter': '...',
        'amendment': {
            'is_amendment': '...',
            'amendment_no': '...',
            'amendment_type': '...'
        },
        'filing_mgr_name': '...',
        'filing_mgr_addr': '...',
        'report_type': '...',
        'form13f_filenum': '...',
        'sec_filenum': '...',
        'info_instruction5': '...',
        'sig_name': '...',
        'sig_title': '...',
        'sig_phone': '...',
        'sic_loc': '...',
        'sig_date': '...',
        'other_mgrs_count': '...',
        'it_entries_count': '...'
        'it_value_total': '...',
        'other_mgrs': [
            {
                'mgr_seq': '...',
                'mgr_cik': '...',
                'mgr_13f_filenum': '...',
                'mgr_sec_filenum': '...',
                'mgr_crd_num': '...',
                'mgr_name': '...'
            },
            ...
        ],
        'it_entries': [
            {
                "issuer" : "APPLE INC",
                "class" : "COM", # Or "SHS CLASS A" etc. In some cases will hold CALL/PUT along with optiontype
                "cusip" : "CUSIP",
                "value" : "VALUE_IN_THOUSANDS",
                "amount" : "NUM_OF_SECURITY_OWNED",
                "amt_type" : "SH/PRN",
                "discretion": "",
                "sole_vote": "",
                "shared_vote": "",
                "no_vote": "",
                "figi" : "OPENFIGI_ID",
                "other_mgr": "",
                "option_type" : "" # "CALL/PUT"
            },
            ...
        ]
    }
}
'''
class HR13FParser(FinancialFilingParser):

    def __init__(self, fulltext):
        # Init self.filing_info, financial_statements, and text_sections
        super().__init__(fulltext) 
        
        self.holdings_report_info = {} 
    
    # Helper function to concat address components
    def format_address(self, street1, street2, city, state, zip_code):

        if street2:
            return f'{street1}, {street2}, {city}, {state}, {zip_code}' if all([street1, city, state, zip_code, street2]) else None
        else:
            return f'{street1}, {city}, {state}, {zip_code}' if all([street1, city, state, zip_code]) else None
    
    # Parses the cover and summary page of a 13F filing, holding fund manager information
    def parse_primary_doc_xml(self, doc_content):

        soup = BeautifulSoup(doc_content, 'xml')

        holding_info = {
            'report_yr_quarter': soup.find(re.compile(r'reportCalendarOrQuarter')).text.strip() if soup.find(re.compile(r'reportCalendarOrQuarter')) else None,
            'amendment': {
                'is_amendment': soup.find(re.compile(r'isAmendment')).text.strip() if soup.find(re.compile(r'isAmendment')) else None,
                'amendment_no': soup.find(re.compile(r'amendmentNo')).text.strip() if soup.find(re.compile(r'amendmentNo')) else None,
                'amendment_type': soup.find(re.compile(r'amendmentType')).text.strip() if soup.find(re.compile(r'amendmentType')) else None  
            },
            'filing_mgr_name': soup.find(re.compile(r'filingManager')).find(re.compile(r'name')).text.strip() if soup.find(re.compile(r'filingManager')) else None,
            'filing_mgr_addr': self.format_address(
                street1=soup.find(re.compile(r'street1')).text.strip() if soup.find(re.compile(r'street1')) else None,
                street2=soup.find(re.compile(r'street2')).text.strip() if soup.find(re.compile(r'street2')) else None,
                city=soup.find(re.compile(r'city')).text.strip() if soup.find(re.compile(r'city')) else None,
                state=soup.find(re.compile(r'stateOrCountry')).text.strip() if soup.find(re.compile(r'stateOrCountry')) else None,
                zip_code=soup.find(re.compile(r'zipCode')).text.strip() if soup.find(re.compile(r'zipCode')) else None
            ),
            'report_type': soup.find(re.compile(r'reportType')).text.strip() if soup.find(re.compile(r'reportType')) else None,
            'form13f_filenum': soup.find(re.compile(r'form13FFileNumber')).text.strip() if soup.find(re.compile(r'form13FFileNumber')) else None,
            'sec_filenum': soup.find(re.compile(r'secFileNumber')).text.strip() if soup.find(re.compile(r'secFileNumber')) else None,
            'info_instruction5': soup.find(re.compile(r'provideInfoForInstruction5')).text.strip() if soup.find(re.compile(r'provideInfoForInstruction5')) else None,
            'sig_name': soup.find(re.compile(r'signatureBlock')).find(re.compile(r'name')).text.strip() if soup.find(re.compile(r'signatureBlock')) else None,
            'sig_title': soup.find(re.compile(r'signatureBlock')).find(re.compile(r'title')).text.strip() if soup.find(re.compile(r'signatureBlock')) else None,
            'sig_phone': soup.find(re.compile(r'signatureBlock')).find(re.compile(r'phone')).text.strip() if soup.find(re.compile(r'signatureBlock')) else None,
            'sic_loc': soup.find(re.compile(r'signatureBlock')).find(re.compile(r'stateOrCountry')).text.strip() if soup.find(re.compile(r'signatureBlock')) else None,
            'sig_date': soup.find(re.compile(r'signatureDate')).text.strip() if soup.find(re.compile(r'signatureDate')) else None,
            'other_mgrs_count': soup.find(re.compile(r'otherIncludedManagersCount')).text.strip() if soup.find(re.compile(r'otherIncludedManagersCount')) else None,
            'it_entries_count': soup.find(re.compile(r'tableEntryTotal')).text.strip() if soup.find(re.compile(r'tableEntryTotal')) else None,
            'it_value_total': soup.find(re.compile(r'tableValueTotal')).text.strip() if soup.find(re.compile(r'tableValueTotal')) else None,
            'other_mgrs': []
        }

        # Extract other managers information
        other_mgrs_info = soup.find_all(re.compile(r'otherManager2'))
        for mgr in other_mgrs_info:
            holding_info['other_mgrs'].append({
                'mgr_seq': mgr.find(re.compile(r'sequenceNumber')).text.strip() if mgr.find(re.compile(r'sequenceNumber')) else None,
                'mgr_cik': mgr.find(re.compile(r'cik')).text.strip() if mgr.find(re.compile(r'cik')) else None,
                'mgr_13f_filenum': mgr.find(re.compile(r'form13FFileNumber')).text.strip() if mgr.find(re.compile(r'form13FFileNumber')) else None,
                'mgr_sec_filenum': mgr.find(re.compile(r'secFileNumber')).text.strip() if mgr.find(re.compile(r'secFileNumber')) else None,
                'mgr_crd_num': mgr.find(re.compile(r'crdNumber')).text.strip() if mgr.find(re.compile(r'crdNumber')) else None,
                'mgr_name': mgr.find(re.compile(r'name')).text.strip() if mgr.find(re.compile(r'name')) else None
            })

        return holding_info
    
    # Helper to below function. Extracts the holding information from one infotable element of a 13F-HR information table
    def extract_holding_from_soup(self, position_soup):
        holding = {
        "issuer": "",
        "class": "",
        "cusip": "",
        "value": "",
        "amount": "",
        "amt_type": "",
        "discretion": "",
        "sole_vote": "",
        "shared_vote": "",
        "no_vote": "",
        "figi": "",
        "other_mgr": "",
        "option_type": ""
        }
        try:
            holding["issuer"] = position_soup.find(re.compile('nameofissuer')).text.strip()
            holding["class"] = position_soup.find(re.compile('titleofclass')).text.strip()
            holding["cusip"] = position_soup.find(re.compile('cusip')).text.strip()
            holding["value"] = position_soup.find(re.compile('value')).text.strip()
            holding["amount"] = position_soup.find(re.compile('sshprnamt')).text.strip()
            holding["amt_type"] = position_soup.find(re.compile('sshprnamttype')).text.strip()
            holding["discretion"] = position_soup.find(re.compile('investmentdiscretion')).text.strip()

            vote_auth = position_soup.find(re.compile('votingauthority'))
            if vote_auth:
                holding["sole_vote"] = vote_auth.find(re.compile('sole')).text.strip()
                holding['shared_vote'] = vote_auth.find(re.compile('shared')).text.strip()
                holding['no_vote'] = vote_auth.find(re.compile('none')).text.strip()
            else:
                logging.error('Failed to find voting authority XML element for holding entry, which should be mandatory.')

            # Optional
            holding['figi'] = position_soup.find(re.compile('figi')).text.strip() if position_soup.find(re.compile('figi')) else ''
            holding['other_mgr'] = position_soup.find(re.compile('othermanager')).text.strip() if position_soup.find(re.compile('othermanager')) else ''
            holding['option_type'] = position_soup.find(re.compile('putcall')).text.strip() if position_soup.find(re.compile('putcall')) else ''
        
        except AttributeError as e:
            logging.warning(f"Error extracting holding details: {e}. Holding data may be incomplete.") 
        
        return holding
    
    # Parses the information table XML document of a 13F-HR filing. Returns the same structure as its caller below, pull_holdings_from_fulltext()
    # Takes a document dictionary from split_filing_documents()
    def parse_information_table(self, doc_dict):

        logging.info(f'About to parse IT document {doc_dict["doc_filename"]}.')
        
        parsed_holdings = []
        
        # Prepare content for lxml
        try:
            doc_cont = re.sub(r'</?xml>', '', doc_dict['doc_text'], flags=re.IGNORECASE)
        except Exception as e:
            logging.error(f'Failed to prepare information table document text for parsing. Error: {e}.')
            return parsed_holdings
        
        # Get parent infotable
        it_soup = BeautifulSoup(doc_cont, 'lxml')
        parent_infotable = it_soup.find(re.compile("informationtable"))

        if not parent_infotable:
            logging.error(f'Failed to find XML <informationtable> in {doc_dict["doc_filename"]}. No holdings will be read.')
            return parsed_holdings
        
        # Loop through the children, each one holding/position
        positions_list = parent_infotable.find_all(re.compile("infotable"))
        if not positions_list:
            logging.error(f'Failed to parse the XML <informationtable> structure for <infotable>\'s in {doc_dict["doc_filename"]}. No holdings will be read.')
            return parsed_holdings
        
        # Extract formatted holdings info 
        parsed_holdings = [self.extract_holding_from_soup(position) for position in positions_list]
        #for holding in parsed_holdings:
            #for key in holding:
                #holding[key] = holding[key].lower() # Lowercase values

        return parsed_holdings
    
    # Parses a 13F filing for manager and holdings info, returns a dictionary object
    def pull_holdings_from_fulltext(self):

        # Locate primary document (cover + summary page)
        primary_doc_text = ''
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            try:
                if (doc['doc_type'].lower().startswith('13f-')) and (doc['doc_filename'].lower().endswith('.xml')):
                    logging.info(f'Found primary XML document of holdings report: {doc["doc_filename"]}.')
                    primary_doc_text = re.sub(r'</?xml>', '', doc['doc_text'], flags=re.IGNORECASE)
                    break
            except KeyError as e:
                logging.error(f'Incomplete document dict was returned from split_filing_documents() to pull_holdings_from_fulltext(). Error: {e}.')

        if not primary_doc_text:
            logging.error('Failed to find 13F primary document.')
            return None
        
        # Parse it
        hr_dict = self.parse_primary_doc_xml(primary_doc_text)
        if not hr_dict:
            logging.error('Failed to parse 13F primary document XML. Will still attempt info table.')
        else:
            logging.info('Parsed 13F primary doc, now looking for info table.')

        # Other main piece of 13F's is the information table, which contains holdings information
        hr_dict['it_entries'] = []
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            if (doc['doc_type'].lower() == 'information table') and (doc['doc_filename'].lower().endswith('.xml')):
                
                logging.info(f'Found information table document {doc["doc_filename"]}. Going to attempt to parse.')
                hr_dict['it_entries'] = self.parse_information_table(doc)
                if not hr_dict['it_entries']:
                    logging.warning(f'Failed to extract any holdings from document {doc["doc_filename"]}.')
                break

        return hr_dict
    
    def full_parse(self):

        self.holdings_report_info = self.pull_holdings_from_fulltext()

        logging.info('Returning from full_parse() in HR13FParser.')

        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'holdings_report': self.holdings_report_info
        }
    
    def __repr__(self):
        return 'HR13FParser'
    
'''
13D and 13G parser
'''
class HR13GParser(FinancialFilingParser):

    def __repr__(self):
        return 'HR13GParser'
    
'''
S1 and S3 parser
For now just focusing on text. TODO: Organize any financial data found within
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'text_sections': [
        {
            'section_doc': '...',
            'section_name': '...',
            'section_type': '...',
            'section_raw_text': '...',
            'section_parsed_text': '...'
        },
        ...
    ]
}
'''
class ProspectusParser(FinancialFilingParser):

    def full_parse(self):
        self.text_sections = self.parse_text_sections()
        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'text_sections': self.text_sections
        }
    
    def __repr__(self):
        return 'ProspectusParser'

'''
8-K parser
Returns filing_info, event_info, and text_section fields:
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'event_info': {
        'items_listed': [],
        ...
    }
    'text_sections': [
        {
            'section_doc': '...',
            'section_name': '...',
            'section_type': '...',
            'section_raw_text': '...',
            'section_parsed_text': '...'
        },
        ...
    ]
}
'''
class Event8KParser(FinancialFilingParser):
    
    def __init__(self, fulltext):
        # Init self.filing_info, financial_statements, and text_sections
        super().__init__(fulltext) 

        self.event_info = {}
    
    # Reads information from the HTML of an 8-K filing cover page. Data is XBRL formatted, but I just parse the HTML. Returns a dictionary structure.
    def read_8k_cover_page(self, cover_content):
        
        soup = BeautifulSoup(cover_content, 'html.parser')

        # Find the table with class "report"
        table = soup.find('table', class_='report')
        table_data = {}

        # Iterate rows
        for row in table.find_all('tr'):

            # First cell is field name
            field_name_cell = row.find('td', class_='pl')
            # Second one is the value
            value_cell = row.find('td', class_='text')

            if field_name_cell and value_cell:

                field_name = field_name_cell.get_text(strip=True)
                field_value = value_cell.get_text(strip=True)

                table_data[field_name] = field_value

        return table_data
    
    # Parses the SEC header and XBRL cover report for a couple values of interest
    def parse_8k_header(self):

        logging.info('Parsing 8-K header.')        
        event_info = {}
        
        # Parse SEC header for info about items present in filing
        sec_header = self.filing_info['header_raw_text']
        header_item_pattern = r'ITEM INFORMATION:\s+([^\n]+)'

        event_info['items_listed'] = re.findall(header_item_pattern, sec_header, re.IGNORECASE)

        logging.info('Parsed header for items list, now looking for XBRL cover report.')

        # Find filing summary -> XBRL reports
        reports_summary = self.search_filing_for_doc_text('filingsummary.xml')
        if not reports_summary:
            logging.warning('No FilingSummary.xml was found in 8-K fulltext.')
            return event_info
        
        cover_reports = self.list_xbrl_reports(reports_summary, report_category='cover')
        if not cover_reports:
            logging.warning('No cover reports found in FilingSummary.xml.')
            return event_info
        
        # Parse cover report
        for cover_report in cover_reports:
            logging.info(f'Targetting 8-K cover report: {cover_report["doc_name"]}.')

            report_text = self.search_filing_for_doc_text(cover_report['doc_name'])
            if report_text:
                report_text = re.sub(r'</?html>', '', report_text, flags=re.IGNORECASE)
                parsed_cover = self.read_8k_cover_page(report_text)

                if not parsed_cover:
                    logging.error('Failed to parse 8-K cover page.')
                else:
                    for key in parsed_cover:
                        event_info[key] = parsed_cover[key]
                    logging.info('Parsed 8-K cover report.')
                    break # Should only be one cover page... break after we successfully parse

        return event_info
    
    # Parses the main html file of an 8-K for any present items (see 8-K format / item no. meanings on SEC website)
    def parse_8k_items(self, doc_name, doc_content):

        soup = BeautifulSoup(doc_content, 'html.parser')

        # Regex pattern to match item headers
        item_pattern = re.compile(r'Item\s+\d+\.\d+')

        # List to store the results
        results = []

        # Variables to track the current item and its content
        current_item = None
        current_content = ""

        # Iterate through relevant HTML elements
        for element in soup.find_all(['p', 'span']):
            text = self.clean_filing_text(element.get_text(strip=True))

            # Check if the text matches the item pattern. AKA we have hit a new 'Item'
            if item_pattern.match(text):

                # If we were parsing a previous item, save it
                if current_item:
                    item_section = {
                        'section_doc': doc_name,
                        'section_name': current_item,
                        'section_type': '8k_item_section',
                        'section_raw_text': None,
                        'section_parsed_text': current_content.strip() 
                    }
                    results.append(item_section)
                    logging.info(f'Finished scraping item section: {current_item}. Found another item after.')

                # Update the current item and reset content
                current_item = text
                current_content = ""  # Reset for new item content
                logging.info(f'Found item section: {current_item}. Scraping it.')

            # Check for the keyword "signature(s)" to stop capturing
            elif text.lower() in ['signatures', 'signature']:
                if current_item:
                    item_section = {
                        'section_doc': doc_name,
                        'section_name': current_item,
                        'section_type': '8k_item_section',
                        'section_raw_text': None,
                        'section_parsed_text': current_content.strip() 
                    }
                    results.append(item_section)
                    current_item = None
                    logging.info(f'Finished scraping item section: {current_item}. Ran into signature block.')
                break  # Exit the loop 

            # If it's content, append to current_content
            else:
                current_content += text + " "  # Append with a space for separation

        # Final check to save any leftover item content (didn't trigger signature match)
        if current_item:
            item_section = {
                        'section_doc': doc_name,
                        'section_name': current_item,
                        'section_type': '8k_item_section',
                        'section_raw_text': None,
                        'section_parsed_text': current_content.strip() 
            }
            results.append(item_section)
            logging.info(f'Finished scraping item section: {current_item}. Hit final check at end of document.')

        return results

    # Parses HTM exhibit files of 8-K filings, for now just for text content
    def parse_8k_exhibit(self, doc_name, doc_content):
        
        soup = BeautifulSoup(doc_content, 'html.parser')

        if not soup:
            logging.error(f'Invalid html content passed to 8-K exhibit parser. Document name: {doc_name}.')
            return None
        
        all_text = soup.get_text(separator='\n', strip=True)
        return {
            'section_doc': doc_name,
            'section_name': f'exhibit-{doc_name}',
            'section_type': '8k_exhibit_section',
            'section_raw_text': None,
            'section_parsed_text': all_text
        }
    
    # Parses 8-k filing fulltext for text sections, returning a list for use as 'text_sections' in final output structure (see FinancialFilingParser)
    def pull_8k_items_exs(self):

        logging.info('Parsing 8-K filing for items and exhibits.')
        
        # Locate 8-K html file, grab contents (item numbers and their text)
        target_doc_name = ''
        target_doc_text = ''
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            try:
                if (doc['doc_type'].lower() == '8-k' or doc['doc_type'].lower() == '8-k/a') and (doc['doc_filename'].lower().endswith('.htm')):
                    logging.info(f'Found 8-K htm file: {doc["doc_filename"]}.')
                    target_doc_name = doc['doc_filename']
                    target_doc_text = re.sub(r'</?xbrl>', '', doc['doc_text'], flags=re.IGNORECASE)
            except KeyError as e:
                logging.error(f'Incomplete document dict was returned from split_filing_documents() to pull_8k_items_exs(). Error: {e}.')

        if not target_doc_text:
            logging.error('Failed to find 8-K htm file.')
            return None
        
        text_sections = self.parse_8k_items(target_doc_name, target_doc_text)

        logging.info('Parsed 8-K item sections, now looking for exhibits.')

        # Other documents of interest are exhibits, document type r'EX-\d+\.\d+'
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            try:
                if (re.fullmatch(r'ex-\d+\.\d+', doc['doc_type'].lower()) and (doc['doc_filename'].lower().endswith('.htm'))):
                    logging.info(f'Found 8-K exhibit file: {doc["doc_filename"]}.')
                    
                    ex_doc_text = re.sub(r'</?html>', '', doc['doc_text'], flags=re.IGNORECASE)
                    text_sections.append(self.parse_8k_exhibit(doc['doc_filename'], ex_doc_text))
                    logging.info('Parsed exhibit file.')
            except KeyError as e:
                logging.error(f'Incomplete document was returned from split_filing_documents to 8-K parser looking for exhibits. Error: {e}.')

        return text_sections

    def full_parse(self):
       
       self.event_info = self.parse_8k_header()
       self.text_sections = self.pull_8k_items_exs()

       # TODO: Grab / organize financial data and tables

       logging.info('Returning from full_parse in Event8KParser.')

       return self.construct_parsed_output()
   
    def construct_parsed_output(self):
       return {
           'filing_info': self.filing_info,
           'event_info': self.event_info,
           'text_sections': self.text_sections
       }
   
    def __repr__(self):
        return 'Event8KParser'
    
'''
Form 4 parser. Returns a dictionary object containing insider transaction info
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'insider_trans': {
        'issuer_info': {
        },
        'owner_info': [],
        'trans': [],
        'footnotes': {
        },
        'sigs': {
        }
    }
}
'''
class Form4Parser(FinancialFilingParser):

    def __init__(self, fulltext):
        super().__init__(fulltext)
        self.insider_trans = {}
    
    # Helper to below, testing a new way of handling optional XML namespaces
    def find_element_ext(self, p_soup, ns, tag_name):
        return p_soup.find(f'{ns}{tag_name}') or p_soup.find(tag_name)
    
    # Construct insider_trans dict object
    def parse_form4_xml(self, xml_content):
        
        soup = BeautifulSoup(xml_content, 'xml')

        # Extract namespace dynamically if present
        namespace = ''
        if soup.ownershipDocument and soup.ownershipDocument.attrs:
            for attr, value in soup.ownershipDocument.attrs.items():
                if attr.startswith("xmlns"):
                    namespace = f"{{{value}}}"  # Namespace format for BeautifulSoup with `{}`

        # Extract Issuer information
        issuer = {
            "issuerCik": self.find_element_ext(soup, namespace, "issuerCik").text.strip() if self.find_element_ext(soup, namespace, "issuerCik") else None,
            "issuerName": self.find_element_ext(soup, namespace, "issuerName").text.strip() if self.find_element_ext(soup, namespace, "issuerName") else None,
            "issuerTradingSymbol": self.find_element_ext(soup, namespace, "issuerTradingSymbol").text.strip() if self.find_element_ext(soup, namespace, "issuerTradingSymbol") else None
        }
        logging.info(f'Parsed Form 4 issuer info: {issuer}')

        # Extract Reporting Owner information
        reporting_owners = []
        for owner in soup.find_all(f"{namespace}reportingOwner") or soup.find_all("reportingOwner"):
            owner_info = {
                "ownerCik": self.find_element_ext(owner, namespace, "rptOwnerCik").text.strip() if self.find_element_ext(owner, namespace, "rptOwnerCik") else None,
                "ownerName": self.find_element_ext(owner, namespace, "rptOwnerName").text.strip() if self.find_element_ext(owner, namespace, "rptOwnerName") else None,
                "ownerCity": self.find_element_ext(owner, namespace, "rptOwnerCity").text.strip() if self.find_element_ext(owner, namespace, "rptOwnerCity") else None,
                "ownerState": self.find_element_ext(owner, namespace, "rptOwnerState").text.strip() if self.find_element_ext(owner, namespace, "rptOwnerState") else None,
                "isOfficer": self.find_element_ext(owner, namespace, "isOfficer").text.strip() if self.find_element_ext(owner, namespace, "isOfficer") else None,
                "officerTitle": self.find_element_ext(owner, namespace, "officerTitle").text.strip() if self.find_element_ext(owner, namespace, "officerTitle") else None
            }
            reporting_owners.append(owner_info)
        logging.info(f'Parsed Form 4 reporting owner info: {reporting_owners}')

        # Extract Non-Derivative Transactions
        # (Helper function to get .text, with nested <value> handling)
        def get_value_text(element, default=None):
            if element:
                value_elem = element.find("value")
                return value_elem.text.strip() if value_elem else element.text.strip()
            return default

        # Extract Non-Derivative Transactions
        non_derivative_transactions = []
        for transaction in soup.find_all(f"{namespace}nonDerivativeTransaction") or soup.find_all("nonDerivativeTransaction"):
            trans_info = {
                "securityTitle": get_value_text(self.find_element_ext(transaction, namespace, "securityTitle")),
                "transactionDate": get_value_text(self.find_element_ext(transaction, namespace, "transactionDate")),
                "transactionCode": get_value_text(self.find_element_ext(transaction, namespace, "transactionCode")), #### Doesn't use value. Updated code to use get_value_text since last run, test
                "transactionShares": get_value_text(self.find_element_ext(transaction, namespace, "transactionShares")),
                "transactionPricePerShare": get_value_text(self.find_element_ext(transaction, namespace, "transactionPricePerShare")),
                "transactionAcquiredDisposedCode": get_value_text(self.find_element_ext(transaction, namespace, "transactionAcquiredDisposedCode")),
                "sharesOwnedFollowingTransaction": get_value_text(self.find_element_ext(transaction, namespace, "sharesOwnedFollowingTransaction")),
                "directOrIndirectOwnership": get_value_text(self.find_element_ext(transaction, namespace, "directOrIndirectOwnership"))
            }
            non_derivative_transactions.append(trans_info)
        logging.info(f'Parsed Form 4 non-derivative transactions.')

        # TODO: Non-Deriv Holdings, Deriv Trans + Holdings

        # Extract Footnotes
        footnotes = {}
        for footnote in soup.find_all(f"{namespace}footnote") or soup.find_all("footnote"):
            footnotes[footnote['id']] = footnote.text.strip()
        logging.info('Parsed Form 4 footnotes.')

        # Extract Signature Information
        signature = {
            "signatureName": self.find_element_ext(soup, namespace, "signatureName").text.strip() if self.find_element_ext(soup, namespace, "signatureName") else None,
            "signatureDate": self.find_element_ext(soup, namespace, "signatureDate").text.strip() if self.find_element_ext(soup, namespace, "signatureDate") else None
        }
        logging.info(f'Parsed Form 4 signatures: {signature}')

        return {
            'issuer_info': issuer,
            'owner_info': reporting_owners,
            'trans': non_derivative_transactions,
            'footnotes': footnotes,
            'sigs': signature
        }
   
    def parse_insider_action(self):
        
        # Find primary XML document
        primary_doc_text = ''
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            try:
                if (doc['doc_type'] == '4' or doc['doc_type'].lower() == '4/a') and (doc['doc_filename'].lower().endswith('.xml')):
                    logging.info(f'Found primary XML document of Form 4: {doc["doc_filename"]}.')
                    primary_doc_text = re.sub(r'</?xml>', '', doc['doc_text'], flags=re.IGNORECASE)
                    break
            except KeyError as e:
                logging.error(f'Incomplete document dict was returned from split_filing_documents() to parse_insider_action(). Error: {e}.')

        if not primary_doc_text:
            logging.error('Failed to find Form 4 primary document.')
            return None
        
        insider_trans = self.parse_form4_xml(primary_doc_text)
        if not insider_trans:
            logging.error('Failed to parse Form 4 document XML. insider_trans will be empty.')
        else:
            logging.info('Parsed Form 4 primary document.')

        return insider_trans
    
    def full_parse(self):
        
        self.insider_trans = self.parse_insider_action()
        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'insider_trans': self.insider_trans
        }
    
    def __repr__(self):
        return 'Form4Parser'

'''
Proxy statement (DEF 14A) parser
For now focus on text content from HTML files. 
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'text_sections': [
        {
            'section_doc': '...',
            'section_name': '...',
            'section_type': '...',
            'section_raw_text': '...',
            'section_parsed_text': '...'
        },
        ...
    ]
}
'''
class ProxyParser(FinancialFilingParser):

    def full_parse(self):

        self.text_sections = self.parse_text_sections()
        return self.construct_parsed_output()
    
    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'text_sections': self.text_sections
        }
    
    def __repr__(self):
        return 'ProxyParser'
    
'''
SEC staff action + letter parser
At the moment focusing on PDF content. TODO: Add HTML + other text parsing
{
    'filing_info': {
        'cik': '...',    
        'type': '...',
        'date': 'YYYYMMDD',
        'accession_numer': '...',
        'company_name': '...',
        'sic_code': '...',
        'sic_desc': '...',
        'report_period': 'YYYYMMDD',
        'state_of_incorp': '...',
        'fiscal_yr_end': 'MMDD',
        'business_address': 'ADDRESS, CITY, STATE, ZIP',
        'business_phone': '...',
        'name_changes': [{...},...]
        'header_raw_text': '...',
        'filing_raw_text': '...' 
    },
    'pdfs': [
        {
            'pdf_name': '...',
            'doc_type': '...',
            'metadata': {...},
            'page_content': [
                {
                'page_num': ...,
                'page_text': '...'
                },
                ...
            ]
        },
        ...
    ]
}
'''
class SECStaffParser(FinancialFilingParser):

    def __init__(self, fulltext):
        # Init self.filing_info, financial_statements, and text_sections
        super().__init__(fulltext) 

        self.pdfs = []

    # Utilizes PyMuPDF to parse documents for metadata and text. TODO: Optionally grab images too
    # Returns a dict structure
    def extract_pdf_meta_and_text(self, pdf_bytes: io.BytesIO):
        
        parsed_pdf_dict = {
            'metadata': {},
            'page_content': []
        }
        
        # Type check of argument
        if not isinstance(pdf_bytes, io.BytesIO):
            logging.error('pdf_bytes passed to extract_pdf_meta_and_text() must be a BytesIO object.')
            return None
        
        try:
            pdf_mu = pymupdf.open(stream=pdf_bytes, filetype='pdf')

            logging.info('Parsing PDF with PyMuPDF.')

            # Metadata
            parsed_pdf_dict['metadata'] = pdf_mu.metadata
        except Exception as e:
            logging.error(f'Failed to open PDF with PyMyPDF and grab metadata. Error: {e}.')
            return None

        logging.info(f'Found {len(pdf_mu)} pages to parse.')
        
        # Iterate through pages, grabbing text
        for page_num in range(len(pdf_mu)):

            try:

                parsed_page_dict = {}

                page = pdf_mu.load_page(page_num)
                parsed_page_dict['page_num'] = page_num + 1
                parsed_page_dict['page_text'] = page.get_text()

                parsed_pdf_dict['page_content'].append(parsed_page_dict)

                logging.info(f'Parsed page {page_num + 1} of {len(pdf_mu)}.')
            except Exception as e:
                logging.error(f'Failed to read text content from page {page_num + 1} of {len(pdf_mu)}.')
                continue

        return parsed_pdf_dict
    
    # Looks through the filing documents, parsing PDFs for their metadata and text. See parse_raw_filing() comments below for structure returned
    def parse_filing_pdfs(self):
        
        # Iterate PDF documents
        found_pdfs = []

        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            if not doc['doc_filename'].lower().endswith('.pdf'):
                continue

            logging.info(f'Found a PDF file, going to attempt to parse: {doc["doc_filename"]}.')

            # Contents come UUencoded (represent PDF in ASCII chars)
            try:
                encoded_pdf_content = re.sub(r'</?pdf>', '', doc['doc_text'], flags=re.IGNORECASE)
            except Exception as e:
                logging.error(f'Failed to read PDF document contents from filing raw text. Error: {e}.')
                continue
            
            uu_encoded_bytes = io.BytesIO(encoded_pdf_content.encode('ascii'))
            pdf_bytes = io.BytesIO()

            try:
                # Decode and write PDF content to BytesIO object
                uu.decode(uu_encoded_bytes, pdf_bytes)
                pdf_bytes.seek(0) # Reset pointer
            except Exception as e:
                logging.error(f'Failed to decode UUencoded data. Error: {e}.')
                continue

            # Parse it
            current_pdf = self.extract_pdf_meta_and_text(pdf_bytes)
            if not current_pdf:
                logging.error('Failed to parse PDF for metadata and contents.')
                continue

            # Add document name and type to the returned dict
            current_pdf['pdf_name'] = doc['doc_filename']
            current_pdf['doc_type'] = doc['doc_type']

            found_pdfs.append(current_pdf)

            logging.info(f'Successfully parsed PDF {doc["doc_filename"]}.')

        return found_pdfs
    
    # Mainly, grab PDF contents
    def full_parse(self):
        
        self.pdfs = self.parse_filing_pdfs()
        # TODO: Parse any HTML

        return self.construct_parsed_output()

    def construct_parsed_output(self):
        return {
            'filing_info': self.filing_info,
            'pdfs': self.pdfs
        }
    
    def __repr__(self):
        return 'SECStaffParser'

def parse_raw_filing(filing_contents_str):
    """Assigns and runs the appropriate parser class from above based on filing type."""

    parsed_dict = {}

    # First populate filing_info subdict
    filing_info_parser = SECFulltextParser(filing_contents_str)
    prelim_filing_info = filing_info_parser.full_parse()
    if not prelim_filing_info:
        logging.error(f'Failed to parse SEC header of filing.')
        return parsed_dict
    
    # Pass on to appropriate parser based on filing type
    parser = None
    try:
        filing_type = prelim_filing_info['filing_info']['type'].lower()  # Convert filing type to lowercase
    except:
        logging.error('Failed to read filing type from SECFulltextParser parse.')
        return parsed_dict
    
    if filing_type in ['10-q', '10-q/a', '10-k', '10-k/a', '6-k', '6-k/a']: 
        parser = FinancialFilingParser(filing_contents_str)

    elif filing_type in ['13f-hr', '13f-hr/a', '13f-nt', '13f-nt/a']:
        parser = HR13FParser(filing_contents_str)

    elif filing_type in ['sc 13g', 'sc 13g/a', 'schedule 13g', 'schedule 13g/a', 'sc 13d', 'sc 13d/a', 'schedule 13d', 'schedule 13d/a']:
        parser = HR13GParser(filing_contents_str)

    elif filing_type in ['s-1', 's-1/a', 's-3', 's-3/a']:
        parser = ProspectusParser(filing_contents_str)

    elif filing_type in ['8-k', '8-k/a']:
        parser = Event8KParser(filing_contents_str)

    elif filing_type in ['4', '4/a']:
        parser = Form4Parser(filing_contents_str)

    elif filing_type in ['def 14a', 'defa14a', 'def 14a/a']:
        parser = ProxyParser(filing_contents_str)

    elif filing_type in ['sec staff action', 'sec staff letter']:
        parser = SECStaffParser(filing_contents_str)

    else:
        logging.warning(f'Unsupported filing type encountered: {filing_type}. Unable to parse.')

    if parser:
        try:
            parsed_dict = parser.full_parse()
            logging.info(f'Parsed filing')
        except Exception as e:
            parsed_dict = {}
            logging.error(f'Exception parsing filing. Error: {e}.')

    return parsed_dict

def sanitize_filing_type(filing_type):
    """Sanitize filing type before use as a folder (avoid /A filings created subdirs etc)"""

    # Replace unsafe characters
    safe_filing_type = filing_type.replace('/', '_')  # Replace '/' with '_'
    safe_filing_type = safe_filing_type.replace(' ', '_')  # Replace spaces with '_'
    safe_filing_type = re.sub(r'[<>:"\\|?*]', '_', safe_filing_type)  # Replace other unsafe characters
    return safe_filing_type

def blob_save_parsed_filing(container_client, parsed_filing):
    """
    Saves the parsed filing dict to its own blob, creating a path based on the info in parsed_filing
    Path follows pattern: 'parsed_filings/YYYY/MM/DD/FILING_TYPE/INDUSTRY_SIC_CODE/FILER_CIK/ACCESSION_NUMBER.json'
    """

    try:
        # Extract information from the filing_data dictionary
        filing_info = parsed_filing['filing_info']
        
        # Build the blob name/path
        date_str = filing_info['date']
        filing_type = sanitize_filing_type(filing_info['type']) # Sanitize for use as folder name
        sic_code = filing_info['sic_code']
        cik = filing_info['cik']
        accession_number = filing_info['accession_number']
        
        # Convert the date string 'YYYYMMDD' to the required format
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        # Create the blob name/path
        blob_name = f'parsed_filings/{year}/{month}/{day}/{filing_type}/{sic_code}/{cik}/{accession_number}.json'
        
        # Serialize the filing_data to JSON
        json_data = json.dumps(parsed_filing)
    
    except Exception as e:
        logging.error(f'Failed to parse filing_info and build blob name. Error: {e}')
        return False

    try:
        # Upload the JSON data to Azure Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data, overwrite=True)

        logging.info(f'Successfully uploaded parsed filing to {blob_name}')
        return True
    
    except Exception as e:
        logging.error(f'Failed to upload parsed filing to {blob_name}\nError: {e}')
        return False

def process_filings_for_date(target_date):
    """Main functionality is orchestrated here"""
    
    if type(target_date) not in [date, datetime]:
        logging.error('Invalid target_date type. Must be a date or datetime object.')
        raise Exception('Invalid target_date type. Must be a date or datetime object.')
    
    # Build our subfolder path for the day's filings
    filings_folder = f'filings/{target_date.year}/{str(target_date.month).zfill(2)}/{str(target_date.day).zfill(2)}/'

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # List blobs in the specified folder
    blob_list = container_client.list_blobs(name_starts_with=filings_folder)

    # Filter blobs to only count txt files
    txt_blobs = [blob for blob in blob_list if blob.name.endswith('.txt')]
    total_blob_count = len(txt_blobs)

    for count, blob in enumerate(txt_blobs):
        
        try:
            # Download the blob content
            blob_client = container_client.get_blob_client(blob.name)
            content = blob_client.download_blob().readall().decode('utf-8')

            logging.info(f'Attempting to parse filing data: {blob.name}, #{count + 1} / {total_blob_count}.')

            # Parse 
            parsed_filing = parse_raw_filing(content)

            logging.info(f'Attempting to upload filing data: {blob.name}, #{count + 1} / {total_blob_count}.')

            # Save parsed data to blob storage under its own path (will be built using filing_info in parsed_filing)
            if (blob_save_parsed_filing(container_client, parsed_filing)):
                logging.info(f'Parsed and saved {blob.name}, #{count + 1} / {total_blob_count} to parsed_filings.')
            else:
                logging.error(f'Failed to upload parsed filing data {blob.name}, #{count + 1} / {total_blob_count}.')
        
        except Exception as e:
            logging.error(f'Exception thrown during filing parse + save. {blob.name}, #{count + 1} / {total_blob_count}. Error: {e}.')

"""Real entrypoint, after config stuff at top"""
logging.info('Starting pipeline step 2 (raw filing -> JSON transformation) workflow.')

# Get the target date if passed
target_date_str = sys.argv[1]

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
    logging.info('Target date is a Saturday or Sunday, no filings to parse.')
    logging.info('Pipeline step 2 (raw filing -> JSON transformation) completed successfully.')
    exit()

# Call main worker method
process_filings_for_date(target_date)

logging.info('Pipeline step 2 (raw filing -> JSON transformation) completed successfully.')
