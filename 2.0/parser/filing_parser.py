import logging
import unicodedata
import re
import pymupdf
import uu
import io
import pandas as pd
from bs4 import BeautifulSoup, NavigableString
import config.settings as settings

'''
Breakdown of logic/flow used:
- NOTE: When I refer to 10-Q, 10-K, etc, I am also implicitly referring to their 'amended' versions, 10-Q/A and so on. 
    From my observations, the structure of amended filings remains generally the same to their 'parent' type.
- The main HTML documents of 10-Q, 10-K, 8-K, S-1, S-3, 13D, 13G filings all have very standard section names. 
    Look for these particular known sections. If unable, look for a table of contents and try to parse it. If unable, parse the whole document as one section. 
- 13F-HR and Form 4 filings have unique XML structures for their data, custom but straightforward parsing.
- DEF 14A, 6-K and other targetted filing types are rather varying in structure and section names. 
    Attempt to locate and walk a table of contents on the main HTML document. If unable, parse the whole document as one section.

For all filing types, HTML exhibit files are also parsed for their text, each document stored as a section. 
    Exhibit numbers are pretty standardized in their meanings within filing types, so a simple mapping is used to organize parsed exhibits.
PDF documents are also parsed for their text for all filing types. Each page is treated as a section.
'''

class MasterParserClass:
    '''
    Master filing parsing class.

    Main method is output_dfs, which returns a dictionary of dataframes holding the parsed filing data.
    '''

    tidy_filing_types = [ '10-q', '10-q/a', '10-k', '10-k/a', '8-k', '8-k/a', 's-1', 's-1/a', 's-3', 's-3/a' ]
    beneficial_owner_types = [ 'sc 13d', 'sc 13d/a', 'sc 13g', 'sc 13g/a' ] 
    holdings_report_filing_types = [ '13f-hr', '13f-hr/a']
    holdings_notice_types = [ '13f-nt', '13f-nt/a' ]
    insider_trans_filing_types = [ '4', '4/a' ]

    tidy_df_types = [settings.FILING_INFO_TABLE, settings.NAMED_SECTIONS_TABLE, settings.EXHIBITS_TABLE, settings.PDF_SECTIONS_TABLE]
    beneficial_owner_df_types = [settings.FILING_INFO_TABLE, settings.NAMED_SECTIONS_TABLE, settings.SUBJECT_COS_TABLE, settings.EXHIBITS_TABLE] # TODO Grab table-like data
    hr_df_types = [settings.FILING_INFO_TABLE, settings.HR_MANAGERS_TABLE, settings.HOLDINGS_TABLE, settings.EXHIBITS_TABLE, settings.PDF_SECTIONS_TABLE]
    hr_notice_df_types = [settings.FILING_INFO_TABLE, settings.HR_MANAGERS_TABLE, settings.EXHIBITS_TABLE, settings.PDF_SECTIONS_TABLE]
    insider_df_types = [settings.FILING_INFO_TABLE, settings.EXHIBITS_TABLE, settings.PDF_SECTIONS_TABLE] # TODO finish
    misc_df_types = [settings.FILING_INFO_TABLE, settings.TOC_SECTIONS_TABLE, settings.EXHIBITS_TABLE, settings.PDF_SECTIONS_TABLE]

    headers_10K = [
    "Item 1", "Item 1A", "Item 1B", "Item 2", "Item 3", "Item 4",
    "Item 5", "Item 6", "Item 7", "Item 7A", "Item 8", "Item 9",
    "Item 9A", "Item 9B", "Item 10", "Item 11", "Item 12", "Item 13",
    "Item 14", "Item 15", 'html_whole_doc'
    ]
    headers_10Q = [
        "Part I Item 1", "Part I Item 2", "Part I Item 3", "Part I Item 4",
        "Part II Item 1", "Part II Item 1A", "Part II Item 2", "Part II Item 3",
        "Part II Item 4", "Part II Item 5", "Part II Item 6", 'html_whole_doc'
        ]
    headers_8K = [
        "Item 1.01", "Item 1.02", "Item 1.03", "Item 1.04",
        "Item 2.01", "Item 2.02", "Item 2.03", "Item 2.04", "Item 2.05", "Item 2.06",
        "Item 3.01", "Item 3.02", "Item 3.03",
        "Item 4.01", "Item 4.02",
        "Item 5.01", "Item 5.02", "Item 5.03", "Item 5.04", "Item 5.05", "Item 5.06", "Item 5.07", "Item 5.08",
        "Item 6.01", "Item 6.02", "Item 6.03", "Item 6.04", "Item 6.05",
        "Item 7.01",
        "Item 8.01",
        "Item 9.01", 'html_whole_doc'
        ]
    headers_S1 = [
        # Front Matter
        "ABOUT THIS PROSPECTUS",
        "PROSPECTUS SUMMARY",
        "THE OFFERING",
        "SUMMARY CONSOLIDATED FINANCIAL DATA",
        
        # Risk
        "RISK FACTORS",
        "CAUTIONARY NOTE REGARDING FORWARD-LOOKING STATEMENTS",

        # Offering details
        "USE OF PROCEEDS",
        "DIVIDEND POLICY",
        "MARKET PRICE INFORMATION",
        "MARKET INFORMATION AND DIVIDEND POLICY",
        "CAPITALIZATION",
        "DILUTION",

        # Business & Financials
        "SELECTED FINANCIAL DATA",
        "MANAGEMENT’S DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS",
        "BUSINESS",
        "INDUSTRY OVERVIEW",
        "COMPETITIVE STRENGTHS",
        "GROWTH STRATEGY",
        "REGULATION",
        
        # Management
        "MANAGEMENT",
        "EXECUTIVE COMPENSATION",
        "CERTAIN RELATIONSHIPS AND RELATED PARTY TRANSACTIONS",

        # Shareholders and Stock
        "PRINCIPAL AND SELLING STOCKHOLDERS",
        "BENEFICIAL OWNERSHIP OF COMMON STOCK",
        "DESCRIPTION OF CAPITAL STOCK",

        # Legal / Deal structure
        "PLAN OF DISTRIBUTION",
        "SHARES ELIGIBLE FOR FUTURE SALE",
        "MATERIAL UNITED STATES FEDERAL INCOME TAX CONSIDERATIONS",
        "LEGAL MATTERS",
        "EXPERTS",
        "INDEMNIFICATION OF DIRECTORS AND OFFICERS",
        "DISCLOSURE OF COMMISSION POSITION ON INDEMNIFICATION FOR SECURITIES ACT LIABILITIES",

        # Reference and Compliance
        "INCORPORATION OF CERTAIN INFORMATION BY REFERENCE",
        "WHERE YOU CAN FIND MORE INFORMATION",
        "ADDITIONAL INFORMATION",
        
        # Financials
        "INDEX TO FINANCIAL STATEMENTS",
        "FINANCIAL STATEMENTS",
        'html_whole_doc'
    ]
    headers_13D = [ 
        'Item 1', 'Item 2', 'Item 3', 'Item 4', 
        'Item 5', 'Item 6', 'Item 7', 'html_whole_doc'
    ]
    headers_13G = [
        'Item 1', 'Item 1(a)', 'Item 1(b)', 
        'Item 2', 'Item 2(a)', 'Item 2(b)', 'Item 2(c)', 'Item 2(d)', 'Item 2(e)',
        'Item 3', 'Item 4', 'Item 5', 'Item 6', 
        'Item 7', 'Item 8', 'Item 9', 'Item 10', 'html_whole_doc'
    ]

    header_mappings_10K = {
        "item 1": "Business",
        "item 1a": "Risk Factors",
        "item 1b": "Unresolved Staff Comments",
        "item 2": "Properties",
        "item 3": "Legal Proceedings",
        "item 4": "Mine Safety Disclosures",
        "item 5": "Market for Registrant’s Common Equity, Related Stockholder Matters and Issuer Purchases of Equity Securities",
        "item 6": "Selected Financial Data",
        "item 7": "Management’s Discussion and Analysis of Financial Condition and Results of Operations",
        "item 7a": "Quantitative and Qualitative Disclosures about Market Risk",
        "item 8": "Financial Statements and Supplementary Data",
        "item 9": "Changes in and Disagreements with Accountants on Accounting and Financial Disclosure",
        "item 9a": "Controls and Procedures",
        "item 9b": "Other Information",
        "item 10": "Directors, Executive Officers and Corporate Governance",
        "item 11": "Executive Compensation",
        "item 12": "Security Ownership of Certain Beneficial Owners and Management and Related Stockholder Matters",
        "item 13": "Certain Relationships and Related Transactions, and Director Independence",
        "item 14": "Principal Accountant Fees and Services",
        "item 15": "Exhibits, Financial Statement Schedules"
    }  
    header_mappings_10Q = {
        "part i item 1": "Financial Statements",
        "part i item 2": "Management’s Discussion and Analysis of Financial Condition and Results of Operations",
        "part i item 3": "Quantitative and Qualitative Disclosures About Market Risk",
        "part i item 4": "Controls and Procedures",
        "part ii item 1": "Legal Proceedings",
        "part ii item 1a": "Risk Factors",
        "part ii item 2": "Unregistered Sales of Equity Securities and Use of Proceeds",
        "part ii item 3": "Defaults Upon Senior Securities",
        "part ii item 4": "Mine Safety Disclosures",
        "part ii item 5": "Other Information",
        "part ii item 6": "Exhibits"
    }  
    header_mappings_8K = {
        "item 1.01": "Entry into a Material Definitive Agreement",
        "item 1.02": "Termination of a Material Definitive Agreement",
        "item 1.03": "Bankruptcy or Receivership",
        "item 1.04": "Mine Safety - Reporting of Shutdowns and Patterns of Violations",
        "item 2.01": "Completion of Acquisition or Disposition of Assets",
        "item 2.02": "Results of Operations and Financial Condition",
        "item 2.03": "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement of a Registrant",
        "item 2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement",
        "item 2.05": "Costs Associated with Exit or Disposal Activities",
        "item 2.06": "Material Impairments",
        "item 3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard; Transfer of Listing",
        "item 3.02": "Unregistered Sales of Equity Securities",
        "item 3.03": "Material Modification to Rights of Security Holders",
        "item 4.01": "Changes in Registrant's Certifying Accountant",
        "item 4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report or Completed Interim Review",
        "item 5.01": "Changes in Control of Registrant",
        "item 5.02": "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers; Compensatory Arrangements of Certain Officers",
        "item 5.03": "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year",
        "item 5.04": "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans",
        "item 5.05": "Amendment to Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics",
        "item 5.06": "Change in Shell Company Status",
        "item 5.07": "Submission of Matters to a Vote of Security Holders",
        "item 5.08": "Shareholder Director Nominations",
        "item 6.01": "ABS Informational and Computational Material",
        "item 6.02": "Change of Servicer or Trustee",
        "item 6.03": "Change in Credit Enhancement or Other External Support",
        "item 6.04": "Failure to Make a Required Distribution",
        "item 6.05": "Securities Act Updating Disclosure",
        "item 7.01": "Regulation FD Disclosure",
        "item 8.01": "Other Events",
        "item 9.01": "Financial Statements and Exhibits"
    }
    header_mappings_13D = {
        "item 1": "Security and Issuer",
        "item 2": "Identity and Background",
        "item 3": "Source and Amount of Funds or Other Consideration",
        "item 4": "Purpose of Transaction",
        "item 5": "Interest in Securities of the Issuer",
        "item 6": "Contracts, Arrangements, Understandings or Relationships with Respect to Securities of the Issuer",
        "item 7": "Material to be Filed as Exhibits"
    }
    header_mappings_13G = { 
        "item 1": "Security and Issuer",
        "item 1(a)": "Name of Issuer",
        "item 1(b)": "Address of Issuer’s Principal Executive Offices",
        "item 2": "Identity and Background",
        "item 2(a)": "Name of Person Filing",
        "item 2(b)": "Address of Principal Business Office or, if None, Residence",
        "item 2(c)": "Citizenship",
        "item 2(d)": "Title of Class of Securities",
        "item 2(e)": "CUSIP Number",
        "item 3": "Filing Status",
        "item 4": "Ownership",
        "item 5": "Ownership of Five Percent or Less of a Class",
        "item 6": "Ownership of More than Five Percent on Behalf of Another Person",
        "item 7": "Identification and Classification of the Subsidiary",
        "item 8": "Identification and Classification of Members of the Group",
        "item 9": "Notice of Dissolution of Group",
        "item 10": "Certification"
    }

    inline_element_tags = ["font", "span", "a", "b", "i", "u", "strong", "em", "small"]
    
    def __init__(self, fulltext_cont, type):

        self.fulltext = fulltext_cont
        self.type = type
        self.filing_info = self.parse_sec_header(fulltext_cont)

        self.named_sections = pd.DataFrame()
        self.toc_sections = pd.DataFrame()
        self.exhibits = pd.DataFrame()
        self.subject_co = pd.DataFrame()
        self.pdf_sections = pd.DataFrame()
        self.hr_managers = pd.DataFrame()
        self.holdings = pd.DataFrame()

    def extract_sec_header(self, fulltext_contents):
        '''
        Extracts the SEC header (<SEC-HEADER>...</SEC-HEADER>) from the full text of a filing. 
        Helper method
        '''

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
    
    def parse_sec_header(self, fulltext_contents):
        '''
        Parses the filing's SEC header.
         
        Returns a dataframe.
        '''

        filing_info = {
            'type': None,
            'date': None,
            'accession_number': None,
            'cik': None,
            'sic_mjr_group_code': None,
            'sic_ind_group_code': None,
            'whole_sic_code': None,
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
        #filing_info['header_raw_text'] = header_text
        #filing_info['filing_raw_text'] = fulltext_contents

        # Extract required fields
        required_patterns = {
            'accession_number': r'accession number:\s+([^\n]+)',
            'type': r'form type:\s+([^\n]+)',
            'date': r'filed as of date:\s+(\d{8})' ######################################################################################### TODO Format as actual date!
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
                        filing_info['whole_sic_code'] = match.group('sic_code').strip()
                        filing_info['sic_div_code'] = filing_info['whole_sic_code'][0]
                        filing_info['sic_mjr_group_code'] = filing_info['whole_sic_code'][:2]
                        filing_info['sic_ind_group_code'] = filing_info['whole_sic_code'][:3]
                    else:
                        filing_info['sic_desc'] = None
                        filing_info['whole_sic_code'] = 0000
                        filing_info['sic_div_code'] = 0
                        filing_info['sic_mjr_group_code'] = 00
                        filing_info['sic_ind_group_code'] = 000
                        logging.warning('Failed to parse sic_code or sic_desc')
                
                else:
                    filing_info[key] = match.group(1).strip()
            else:
                logging.warning(f'Header field not found: {key}')
                if key == 'cik':
                    filing_info[key] = 0000000000
                elif key == 'sic_whole':
                    filing_info['sic_desc'] = None
                    filing_info['whole_sic_code'] = 0000
                    filing_info['sic_mjr_group_code'] = 00
                    filing_info['sic_ind_group_code'] = 000

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
        return pd.DataFrame([filing_info])

    def split_filing_documents(self):
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
            documents = re.findall(document_pattern, self.fulltext, re.DOTALL | re.IGNORECASE)
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
    
    def extract_text_from_element(self, element):
        """
        Extracts text while converting newlines inside inline tags to spaces.
        """
        # Handle tables by converting them to simplified text
        for table in element.find_all('table'):
            table_text = []
            for row in table.find_all('tr'):
                cells = [''.join(cell.stripped_strings) for cell in row.find_all(['th', 'td'])]
                table_text.append(' | '.join(cells))
            table.replace_with('\nTABLE: ' + '\n'.join(table_text) + '\n')

        # Replace <br> tags with newline
        for br in element.find_all('br'):
            br.replace_with('\n')

        # Traverse and normalize newline handling in inline contexts
        text_parts = []
        for desc in element.descendants:
            if isinstance(desc, NavigableString):
                parent_tag = desc.parent.name if desc.parent else ''
                content = str(desc)
                if '\n' in content and parent_tag in MasterParserClass.inline_element_tags:
                    content = content.replace('\n', ' ')
                text_parts.append(content)

        text = ''.join(text_parts)

        # Collapse multiple spaces but preserve newlines
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r' *\n *', '\n', text)  # Clean space around newlines

        return text.strip()
    
    def tidy_parse_with_p_tags(self, html_content): 
        """
        Parses HTML where paragraphs are in <p> tags with direct text.
        This is the most common structure in SEC filings.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Extracted text with double newlines between paragraphs
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        paragraphs = []
        
        for p in soup.find_all('p'):
            # Avoid capturing "container" <p> that wrap other <p> tags, which is bad HTML but present in SEC filing(s) (i.e.: https://www.sec.gov/Archives/edgar/data/2049248/000192998025000217/0001929980-25-000217-index.html)
            if p.find('p'):
                continue
            # Get direct text and text from all children
            text = self.extract_text_from_element(p)
            if text:
                paragraphs.append(text)
        
        return '\n\n'.join(paragraphs)

    def tidy_parse_with_div_tags(self, html_content):
        """
        Parses HTML where paragraphs are in <div> tags instead of <p> tags.
        Some filings use divs with specific classes for paragraphs.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Extracted text with double newlines between paragraphs
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        paragraphs = []
        
        # Look for divs that are likely to contain paragraphs
        for div in soup.find_all('div'):
            # Skip divs that contain block elements (they're probably containers)
            if div.find(['p', 'div', 'table', 'ul', 'ol']):
                continue
                
            text = self.extract_text_from_element(div)
            if text:
                paragraphs.append(text)
        
        return '\n\n'.join(paragraphs)

    def tidy_parse_with_nested_structures(self, html_content: str) -> str:
        """
        Parses HTML with complex nested structures where text might be
        in children of paragraph tags or in spans within paragraphs.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Extracted text with double newlines between paragraphs
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        paragraphs = []
        
        # First try p tags with nested content
        for p in soup.find_all('p'):
            text = self.extract_text_from_element(p)
            if text:
                paragraphs.append(text)
        
        # If we didn't get enough p tags, try divs
        if len(paragraphs) < 5:  # Arbitrary threshold
            for div in soup.find_all('div'):
                # Skip divs that contain block elements (unless they're likely text containers)
                if div.find(['table', 'ul', 'ol', 'img']):
                    continue
                    
                text = self.extract_text_from_element(div)
                if text:
                    paragraphs.append(text)
        
        return '\n\n'.join(paragraphs)
    
    def tidy_parse_fallback(self, html_content):
        """
        Fallback parser that tries to extract text when other methods fail.
        This is more aggressive and might get more noise.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Extracted text with double newlines between logical sections
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'table', 'footer', 'header', 'nav']):
            element.decompose()
        
        # Get all text blocks
        text = soup.get_text('\n')  # Use single newline for all line breaks
        
        # Clean up the text
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        # Join non-empty lines with double newlines
        return '\n\n'.join(lines)
    
    def extract_text_from_tidy_doc(self, html_content): # TODO: Some filings have <table> elements amongst <p> elements, both with text we need... consider more robust fallback logic upon section parsing
        """
        Main function to parse SEC filing HTML content of filing types with known section names.
        Attempts different parsing strategies until one succeeds.
        
        Args:
            html_content: Raw HTML content of the SEC filing
            
        Returns:
            Text content with paragraphs separated by double newlines
        """
        strategies = [
            self.tidy_parse_with_p_tags,
            self.tidy_parse_with_div_tags,
            self.tidy_parse_with_nested_structures,
            self.tidy_parse_fallback
        ]
        
        for strategy in strategies:
            logging.info(f'Attempting to extract tidy {self.type} filing text using: {strategy}')
            result = strategy(html_content)
            if result.strip():  # If we got meaningful content
                logging.info(f'Successfully extracted text from tidy {self.type} filing.\nStrategy used: {strategy}')
                return result
                
        return ""  # Fallback if all strategies fail

    def normalize_unicode(self, text: str) -> str:
        """
        Normalize Unicode text while preserving meaningful punctuation like dashes
        """
        # Normalize Unicode (convert to NFKD form)
        text = unicodedata.normalize('NFKD', text)
        
        # Process each character
        cleaned_chars = []
        for ch in text:
            if ch == '\n':
                # Preserve newlines
                cleaned_chars.append(ch)
            elif ch == '\xa0':
                # Convert non-breaking space to regular space
                cleaned_chars.append(' ')
            elif unicodedata.category(ch) == 'Cc' and ch != '\t':
                # Replace only ASCII control characters (category Cc) except tab
                cleaned_chars.append(' ')
            elif unicodedata.category(ch).startswith('Z') and ch != ' ':
                # Replace other space-like characters except normal space
                cleaned_chars.append(' ')
            else:
                # Preserve all other characters including dashes and hyphens
                cleaned_chars.append(ch)
        
        return ''.join(cleaned_chars)
    
    def normalize_extracted_text(self, text_str, newlines_to_spaces=True):
        '''
        Unicode + newline normalize.
        '''
        normalized_text = self.normalize_unicode(text_str)

        # Replace single newlines (not part of double-newline section separators) with space
        if newlines_to_spaces:
            normalized_text = re.sub(r'(?<!\n)\n(?!\n)', ' ', normalized_text)
        
        # Collapse any sequence of 3+ newlines to exactly 2 (for consistent paragraph breaks)
        normalized_text = re.sub(r'\n{3,}', '\n\n', normalized_text)
        return normalized_text
    
    def extract_and_normalize_tidy_doc_text(self, html_content):
        '''
        Extracts and normalizes the text of a tidy/known-section-names style filing document, notably with double newlines (\n\n) between paragraphs, section headings, etc.
        Later parsing regex relies on the existence of these double newline delimiters. 
        '''
        logging.info(f'Attempting to extract text from tidy filing type {self.type}.')
        extracted_text = self.extract_text_from_tidy_doc(html_content)
        logging.info(f'Normalizing and returning extracted text.')
        return self.normalize_extracted_text(extracted_text, True)
    
    def extract_parts(self, form_text, form_type):
        """Extracts every part from form plain text, where a "part" is defined
        specifically as a portion in the form starting with "PART (some roman numeral)".
        
        :type form_text: str
        :param form_text: The form plain text.
        
        :type form_type: str
        :param form_type: The form type (e.g. 10-K, 10-Q, 8-K)
        
        :rtype: Iterator[(str, str)]
        :returns: An iterator over the header and text for each part extracted from the form plain text.
            (e.g. for 10-K forms, we iterate through Part I through Part IV)
        """
        pattern = '((^PART|^Part|\n\nPART|\n\nPart) [IVXLCDM]+).*?(\n\n.*?)(?=\n\n(PART|Part) [IVXLCDM]+.*?\n\n|$)'
        return ((_.group(1).strip(), _.group(3)) for _ in re.finditer(pattern, form_text, re.DOTALL))
    
    def extract_indiv_tidy_sections(self, part_header, part_text, form_type, expected_sections):
        """Extracts the item/section header and its corresponding text for every item within the plain text of a "part" of a form.
    
        :type part_header: str
        :param part_header: The header of a "part" of a form (e.g. Part III)
        
        :type part_text: str
        :param part_text: The plain text of a "part" of a 10-Q form (e.g. Part III). In the case of other filing types, the "part" is just the whole form.
        
        :type form_type: str
        :param form_type: The form type (e.g. 10-K, 10-Q, 8-K)

        :rtype: Iterator[(str, str, str)]
        :returns: An iterator over tuples of the form (part_header, item_header, text) 
            where "item_header" is the item header and "text" is the corresponding text
            for each item in the "part". part_header is included to differentiate 
            between portions of a filing that have the same item number but are in different parts.
        """
        if form_type in ["10-k", "10-k/a", "10-q", "10-q/a"]:
            #pattern = '(?P<header>(\n\n(ITEM|Item) \d+[A-Z]*.*?)\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item) \d+[A-Z]*.*?)\n\n|$)'
            #pattern = r'(?P<header>(\n\n(ITEM|Item)\s+\d+[A-Z]*\b.*?)\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item)\s+\d+[A-Z]*\b.*?)\n\n|$)'
            pattern = r'(?P<header>\n\n(ITEM|Item)\s+\d+[A-Z]?\.*.*?\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item)\s+\d+[A-Z]?\.*.*?\n\n|$))'
            regex = re.compile(pattern, re.DOTALL)
        elif form_type in ["8-k", "8-k/a"]:
            #pattern = '(?P<header>\n\n(ITEM|Item)\s+\d+\.\d+\.*)(?P<text>.*?)(?=((\n\n(ITEM|Item)\s+\d+\.\d+.*?)\n\n|$))' # <- only grabs Item X.XX, not text after, requires no processing of header in items_to_df_row
            pattern = r'(?P<header>\n\n(ITEM|Item)\s+\d+\.\d+\.*.*?\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item)\s+\d+\.\d+\.*.*?\n\n|$))'
            regex = re.compile(pattern, re.DOTALL)
        elif form_type in ['s-1', 's-1/a', 's-3', 's-3/a']:
            # Build a regex that looks for all S-1 section headers with double newlines before/after
            #pattern = rf'(?P<header>\n\n(?:{"|".join(re.escape(h) for h in columns)}))\n\n(?P<text>.*?)(?=\n\n(?:{"|".join(re.escape(h) for h in columns)})\n\n|$)'
            pattern = rf'(?P<header>\n\n(?:{"|".join(re.escape(h) for h in expected_sections)})\s*\n\n)(?P<text>.*?)(?=\n\n(?:{"|".join(re.escape(h) for h in expected_sections)})\s*\n\n|$)'
            # Case-insensitive
            regex = re.compile(pattern, re.IGNORECASE | re.DOTALL)
        elif form_type in ['sc 13d', 'sc 13d/a']:
            pattern = r'(?P<header>\n\n(ITEM|Item)\s+\d+\.*.*?\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item)\s+\d+\.*.*?\n\n|$))'
            regex = re.compile(pattern, re.DOTALL)
        elif form_type in ['sc 13g', 'sc 13g/a']:
            pattern = r'(?P<header>\n\n(ITEM|Item)\s+\d+(?:\([a-z]\))?.*?\n\n)(?P<text>.*?)(?=(\n\n(ITEM|Item)\s+\d+(?:\([a-z]\))?.*?\n\n|$))'
            regex = re.compile(pattern, re.DOTALL)
        else: 
            regex = None
            logging.error(f'Unsupported form type {form_type} passed to extract_indiv_tidy_sections, unsure of section header format. Expecting a filing of known tidy types: {self.tidy_filing_types}')
        
        if not regex:
            return None
        
        logging.info(f'Chose regex for section/item extraction based on type: {form_type}.')
        return ((part_header, _.group('header').strip(), _.group('text').strip()) for _ in regex.finditer(part_text))
    
    def get_tidy_sections(self, form_text, form_type, expected_sections): 
        """Extracts the item header and its corresponding text for every item within a form's plaintext.
    
        :type form_text: str
        :param form_text: The form plain text.
        
        :type form_type: str
        :param form_type: The form type (e.g. 10-K, 10-Q, 8-K)
        
        :rtype: List[(str, str, str)]
        :returns: A list of tuples of the form (part_header, item_header, text) 
        """
        if form_type in ["10-q", "10-q/a"]:
            logging.info(f'Attempting to locate known items in {form_type} filing. First looking for Part headers.')
            for part_header, part_text in self.extract_parts(form_text, form_type):
                logging.info(f'Found {part_header} of {form_type}. Now looking for Items.')
                items = list(self.extract_indiv_tidy_sections(part_header, part_text, form_type, expected_sections)) # Generator is not 'truthy' testable
                if items:
                    logging.info(f'Extracted items from {part_header} of {form_type}.')
                    return items
                else:
                    logging.warning(f'Failed to extract items from {part_header} of {form_type}.')
        else: # Other tidy types don't use part headers
            logging.info(f'Attempting to locate known sections/items in {form_type} filing.')
            items = list(self.extract_indiv_tidy_sections("", form_text, form_type, expected_sections)) 
            if items:
                logging.info(f'Extracted sections/items of {form_type}.')
                return items
            else:
                logging.warning(f'Failed to extract known sections/items of {form_type}.')
        
        logging.warning(f"Unable to extract any known sections in {form_type} filing {self.filing_info['accession_number'].iloc[0]}.")
        return []

    def tidy_sections_to_df(self, sections_list, known_list, form_type, header_mappings):
        """Takes a list of tuples that is created from calling get_tidy_sections
        and generates a dataframe that has a row for each of the sections, 
        with columns linking back to the filing info dataframe via accession_number and with info about the section.
        
        :type sections_list: List[(str, str, str)]
        :param sections_list: A list of tuples of the form (part_header, section_header, item_text).
        
        :type known_list: List[str]
        :param known_list: A list of column names for the dataframe we wish to generate a row for.

        :type form_type: str
        :param form_type: The form type. Currently supported types include 10-K, 10-Q, 8-K, S-1, S-3, SC 13D/G

        :type header_mappings: Dict{section_name: section_meaning}
        :param header_mappings: A dictionary of section names to their meanings
        
        :rtype: DataFrame
        """
        section_rows = []
        section_dict = {} # Used to ensure we only save last occurance of each section (TOC causes false positives)

        for part_header, section_header, text in sections_list:
            processed_header = (part_header.lower() + " " + section_header.lower()).strip() 
            if form_type in ["10-q", "10-q/a"]:
                processed_header = re.search("part\s+[ivxlcdm]+\s+item\s+\d+[a-z]*", processed_header).group(0)
            elif form_type in ["10-k", "10-k/a"]:
                processed_header = re.search("item\s+\d+[a-z]*", processed_header).group(0)
            elif form_type in ["8-k", "8-k/a"]:
                if processed_header[-1] == ".":
                    processed_header = processed_header[:-1] # Some companies will include a period at the end of the header while others don't 
                # Testing, we just want the Item X.XX part 
                processed_header = re.search("item\s+\d+\.\d+", processed_header).group(0)
            elif form_type in ["s-1", "s-1/a", "s-3", "s-3/a"]:
                pass # No additional normalization needed here, already stripped and lowercased
            elif form_type in ['sc 13d', 'sc 13d/a']:
                # Only want Item X part
                processed_header = re.search("item\s+\d+", processed_header).group(0)
            elif form_type in ['sc 13g', 'sc 13g/a']:
                # We want Item X with optional '(y)' afterwards
                processed_header = re.search(r'item\s+\d+(?:\([a-z]\))?', processed_header).group(0)
            else:
                logging.error(f'Unsupported form type {form_type} passed to tidy_sections_to_df_row, unsure how to process section {processed_header}. Expecting a filing of known tidy types: {self.tidy_filing_types}')
        
            if processed_header in [x.lower() for x in known_list]:
                # Create row in DF with accession_number column from self.filing_info['accession_number'].iloc[0], section_name column with processed_header, section_meaning column with header_mappings, and section_text column with text
                if processed_header in section_dict.keys():
                    logging.info(f'Over-writing previous {processed_header} row, found a subsequent occurence.')
                
                section_dict[processed_header] = {
                    'accession_number': self.filing_info['accession_number'].iloc[0],
                    'section_name': processed_header,
                    'section_meaning': header_mappings.get(processed_header, '') if header_mappings else '',
                    'text': text
                }
                logging.info(f'Prepared tidy section for dataframe: {processed_header}.')
            else:
                logging.warning(f'Unexpected section header encountered for tidy {form_type} filing: {processed_header}. Does not match any known column: {known_list}\nTidy type filings should only be building headers we recognize...')
        
        section_rows = list(section_dict.values()) # Dict -> list
        logging.info(f'Finished building tidy sections dataframe for {form_type} filing.')
        return pd.DataFrame(section_rows)

    def parse_doc_named_sections(self, html_content, form_type): 
        '''
        Returns a dataframe with one record per extracted section, with an accession_number column for linking to filing info. 
        Section names and meanings will vary based on form/filing type.
        '''
        logging.info(f"Parsing main document of {form_type} filing {self.filing_info['accession_number'].iloc[0]} for known named sections.")
        normalized_text = self.extract_and_normalize_tidy_doc_text(html_content)
        if not normalized_text.strip():
            raise ValueError('Failed to extract meaningful text from HTML content.')
        logging.info(f'Extracted and normalized document text.')
        
        header_mappings = None
        if form_type in ["10-k", "10-k/a"]:
            known_sections = MasterParserClass.headers_10K
            header_mappings = MasterParserClass.header_mappings_10K
        elif form_type in ["10-q", "10-q/a"]:
            known_sections = MasterParserClass.headers_10Q
            header_mappings = MasterParserClass.header_mappings_10Q
        elif form_type in ["8-k", "8-k/a"]:
            known_sections = MasterParserClass.headers_8K
            header_mappings = MasterParserClass.header_mappings_8K
        elif form_type in ["s-1", "s-1/a", "s-3", "s-3/a"]:
            known_sections = MasterParserClass.headers_S1
        elif form_type in ['sc 13d']:
            known_sections = MasterParserClass.headers_13D
            header_mappings = MasterParserClass.header_mappings_13D
        elif form_type in ['sc 13g']:
            known_sections = MasterParserClass.headers_13G
            header_mappings = MasterParserClass.header_mappings_13G
        else: 
            raise ValueError(f"Unsupported form type {form_type} fed to parse_doc_items. Expecting a 'tidy' type: {MasterParserClass.tidy_filing_types}")
        logging.info(f'Chose possible sections list and header mappings based on tidy filing type ({self.type}).\nSections: {known_sections}\nHeader mappings: {header_mappings}')
        
        tidy_sections = self.get_tidy_sections(normalized_text, form_type, known_sections)
        if tidy_sections:
            logging.info(f'Got list of extracted sections/items of tidy {self.type} filing. Attempting to build dataframe with it.')
            return self.tidy_sections_to_df(tidy_sections, known_sections, form_type, header_mappings)
        
        logging.warning('Unable to find any known sections of tidy filing via get_tidy_sections method.')
        return pd.DataFrame()
    
    def parse_tidy_main_doc(self):
        '''
        Handles locating + parsing the main HTML document of a 'tidy' filing type (one with reliably named sections i.e. 10-K)

        Returns a dataframe with one record and one column per extracted section + a filing ID (accession number) column. 
        Columns and their names will vary based on form/filing type.
        '''
        logging.info(f'Attempting to locate main HTML document of tidy {self.type} filing for parsing.')

        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            # Look for main doc, is of the same 'type' as the filing itself
            if doc.get('doc_filename', '').lower().endswith('.htm') and doc.get('doc_type', '').lower() == self.type.lower():
                logging.info(f'Found main filing document: {doc.get("doc_filename", "")} of type {self.type}.')

                # Look for known sections
                named_sections_df = self.parse_doc_named_sections(doc.get('doc_text', ''), self.type.lower()) 
                if not named_sections_df.empty:
                    return named_sections_df
                else:
                    logging.warning(f"Falling back to TOC style parsing of tidy type filing {self.filing_info['accession_number'].iloc[0]}.")
                    toc_sections = self.get_toc_sections(doc.get('doc_text', ''))
                    if toc_sections:
                        logging.info(f'Building tidy sections dataframe with TOC method results.')
                        
                        toc_section_rows = []
                        for section in toc_sections:
                            # Have chosen to simply record the TOC results in the named_sections DF format, rather than having tidy filings liable to have their text in either named_sections or toc_sections
                            # Might change this behavior, but the thinking is that if I add toc_sections to the tidy df types list, it will unnecessarily build it when output_dfs is called unless I change output_dfs's
                            # behavior... Maybe could default to doing both named_sections and toc_sections for tidy style filings.. TODO
                            section_row = {
                                'accession_number': self.filing_info['accession_number'].iloc[0],
                                'section_name': section.get('section_name', ''),
                                'section_meaning': 'toc_fallback', # TODO: Possibly still somehow use header mapping if it matches
                                'text': section.get('section_parsed_text', '')
                            }
                            toc_section_rows.append(section_row)
                            logging.info(f"Prepared fallback TOC scraped section for tidy sections dataframe: {section.get('section_name', '')}.")
                        
                        return pd.DataFrame(toc_section_rows)
                    else:
                        logging.error(f'Failed to extract any sections via get_toc_sections method... Should at least record whole document as one section...')
            
        logging.error(f'Failed to parse main HTML document for {self.type} filing for text sections.')
        return pd.DataFrame()
    
    def classify_exhibit(self, form_type, exhibit_type):
        '''
        Attempts to determine the logical 'meaning'/category of an exhibit document.
        Returns a string, 'Miscellaneous / Unknown Exhibit' is the fallback.
        '''
        form_type = form_type.upper()
        exhibit_type = re.sub(r'[^0-9.]', '', exhibit_type)
        logging.info(f'Stripped exhibit type: {exhibit_type}')

        # Mapping of known exhibit numbers to meanings for each form type
        exhibit_lookup = {
            "10-K": {
                "10.1": "Material Contract",
                "21.1": "List of Subsidiaries",
                "23.1": "Auditor Consent",
                "31.1": "SOX CEO Certification",
                "31.2": "SOX CFO Certification",
                "32.1": "SOX Section 906 Certification",
                "99.1": "Additional Financial Info / Press Release",
            },
            "10-Q": {
                "10.1": "Material Contract",
                "31.1": "SOX CEO Certification",
                "31.2": "SOX CFO Certification",
                "32.1": "SOX Section 906 Certification",
                "99.1": "Supplemental Financial Data",
            },
            "8-K": {
                "99.1": "Earnings Release / Miscellaneous",
                "99.2": "Presentation Materials",
            },
            "6-K": {
                "99.1": "Earnings Release / Press Release",
                "99.2": "Financial Summary",
            },
            "FORM 4": {
                "99.1": "Power of Attorney / Signature Authorization",
            },
            "DEF 14A": {
                "99.1": "Presentation or Supporting Material",
            },
            "S-1": {
                "1.1": "Underwriting Agreement",
                "5.1": "Legal Opinion",
                "10.1": "Material Contract",
                "23.1": "Auditor Consent",
                "99.1": "Investor Presentation / Additional Info",
            },
            "S-3": {
                "1.1": "Underwriting Agreement",
                "5.1": "Legal Opinion",
                "10.1": "Material Contract",
                "23.1": "Auditor Consent",
                "99.1": "Supplemental Materials",
            },
            "13F-HR": {
                "INFO TABLE": "Institutional Investment Holdings",
            },
            "SC 13D": {
                "1": "Agreement / Letter",
                "2": "Power of Attorney / Exhibit B",
                "99.1": "Agreement / Offer Document",
            },
            "SC 13G": {
                "1": "Power of Attorney / Agreement",
                "99.1": "Supporting Document",
            }
        }

        # Attempt to classify using known form and exhibit mapping
        form_map = exhibit_lookup.get(form_type)
        if form_map:
            logging.info(f'Attempting to classify exhibit type {exhibit_type} of known form type {form_type}.')
            
            if exhibit_type in form_map:
                logging.info(f'Classified exhibit to {form_type} filing as {form_map[exhibit_type]}.')
                return form_map[exhibit_type]
        else:
            logging.info(f'Unfamiliar with exhibhit type mappings for {form_type} filings.')

        # Default fallback
        logging.warning(f'Was unable to categorize exhibit, falling back to Misc.')
        return "Miscellaneous / Unknown Exhibit"
    
    def parse_exhibits(self):
        '''
        Parses any exhibit documents present in a given filing for their text, one per row into a dataframe linked back to the filing via accession number. 
        '''
        logging.info(f'Looking for any HTML exhibit documents of the current {self.type} filing for parsing.')
        filing_docs = self.split_filing_documents()
        
        parsed_exhibits = []
        for doc in filing_docs:
            if doc.get('doc_filename', '').lower().endswith('.htm') and re.match(r'(EX-\d{1,3}(?:\.\d{1,3})?[A-Z]?)', doc.get('doc_type').upper()):
                logging.info(f'Identified {doc.get("doc_type")} exhibit document {doc.get("doc_filename")} of {self.type} filing.')

                # Get document text
                soup = BeautifulSoup(doc.get('doc_text', ''), 'html.parser')
                if not soup:
                    logging.error(f'Invalid exhibit document HTML encountered, unable to parse.')
                    continue
                
                exhibit_row = {
                    'accession_number': self.filing_info['accession_number'].iloc[0],
                    'exhibit_type': doc.get('doc_type'),
                    'exhibit_meaning': self.classify_exhibit(self.type, doc.get('doc_type')),
                    'text': soup.get_text(separator='\n', strip=True)
                }
                parsed_exhibits.append(exhibit_row)
                logging.info(f'Recorded {exhibit_row.get("exhibit_meaning")} type exhibit for {self.type} filing dataframe.')

        if not parsed_exhibits:
            logging.info(f'Unable to find or parse any exhibit documents for the current {self.type} filing.')
            return pd.DataFrame()

        return pd.DataFrame(parsed_exhibits)

    def parse_filing_subject(self):
        '''
        Parses the subject company block from the SEC header of a 13D/G filing.
        Returns a single-row dataframe.
        '''
        subject_info = {
            'accession_number': self.filing_info['accession_number'].iloc[0],
            'company_name': None,
            'cik': None,
            'sic_code': None,
            'sic_desc': None,
            'org_name': None,
            'sec_file_num': None,
            'film_num': None,
            'state_of_incorp': None,
            'fiscal_yr_end': None,
            'business_address': None,
            'business_phone': None,
            'name_changes': []
        }

        header_text = self.extract_sec_header(self.fulltext)
        if not header_text:
            logging.error('Failed to extract SEC header from filing content.')
            return pd.DataFrame([subject_info])
        
        subject_match = re.search(r'SUBJECT COMPANY:(.+?)(?:FILED BY:|FILER:|</SEC-HEADER>)', header_text, re.IGNORECASE | re.DOTALL)
        if not subject_match:
            logging.error('Subject company section not found in SEC header.')
            return pd.DataFrame([subject_info])

        subject_block = subject_match.group(1)

        patterns = {
            'company_name': r'COMPANY CONFORMED NAME:\s*(.+)',
            'cik': r'CENTRAL INDEX KEY:\s*(\d+)',
            'org_name': r'ORGANIZATION NAME:\s*(.+)',
            'sec_file_num': r'SEC FILE NUMBER:\s*([^\n\r]+)',
            'film_num': r'FILM NUMBER:\s*(\d+)',
            'state_of_incorp': r'STATE OF INCORPORATION:\s*(\w+)',
            'fiscal_yr_end': r'FISCAL YEAR END:\s*(\d+)',
            'business_phone': r'BUSINESS PHONE:\s*(.+)'
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, subject_block, re.IGNORECASE)
            if match:
                subject_info[key] = match.group(1).strip()
            else:
                logging.warning(f'Subject field not found: {key}.')

        # SIC parsing
        sic_match = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:\s*(.+?)\s*\[(\d{4})\]', subject_block, re.IGNORECASE)
        if sic_match:
            subject_info['sic_desc'] = sic_match.group(1).strip()
            subject_info['sic_code'] = sic_match.group(2).strip()
        else:
            logging.warning('SIC code or description not found in beneficial ownership subject section.')

        # Business address parsing
        address_fields = ['street1', 'street2', 'city', 'state', 'zip']
        addr_data = {}

        for field in address_fields:
            pattern = rf'{field.replace("street", "STREET ").replace("city", "CITY").replace("state", "STATE").replace("zip", "ZIP")}:\s*(.+)'
            match = re.search(pattern, subject_block, re.IGNORECASE)
            if match:
                addr_data[field] = match.group(1).strip()
            else:
                addr_data[field] = None

        subject_info['business_address'] = ', '.join([v for v in addr_data.values() if v])

        # Former company names
        former_name_matches = re.finditer(
            r'FORMER COMPANY:\s*FORMER CONFORMED NAME:\s*(.+?)\s*DATE OF NAME CHANGE:\s*(\d{8})',
            subject_block,
            re.IGNORECASE | re.DOTALL
        )
        for match in former_name_matches:
            subject_info['name_changes'].append({
                'former_name': match.group(1).strip(),
                'date_of_change': match.group(2).strip()
            })

        logging.info('Finished parsing beneficial ownership subject company section.')
        return pd.DataFrame([subject_info])

    def parse_pdf_to_df(self, decoded_pdf: io.BytesIO):
        '''
        Utilizes PyMuPDF to parse uudecoded PDF contents, 
        returning a dataframe with one row per PDF section or page.
        TODO: Possibly grab images too
        '''
        try:
            pdf_mu = pymupdf.open(stream=decoded_pdf, filetype='pdf')

            logging.info('Parsing PDF with PyMuPDF.')
        except Exception as e:
            logging.error(f'Failed to open PDF with PyMyPDF and grab metadata. Error: {e}.')
            return pd.DataFrame()
        
        metadata = pdf_mu.metadata
        metadata['num_pages'] = len(pdf_mu)
        
        pdf_sections = []
        toc = pdf_mu.get_toc()
        if toc:
            logging.info(f'Found PDF table of contents, will save section text accordingly.')
            for i, (level, title, start_page) in enumerate(toc):
                logging.info(f'Parsing PDF section: {title}.')

                start_page -= 1 # Convert to 0-based
                end_page = toc[i + 1][2] - 1 if i + 1 < len(toc) else len(pdf_mu)

                text = ''
                for p in range(start_page, end_page):
                    text += pdf_mu.load_page(p).get_text()

                pdf_section = {
                    'accession_number': self.filing_info['accession_number'].iloc[0],
                    'pdf_metadata': metadata,
                    'start_page': start_page,
                    'end_page': end_page,
                    'section_name': title,
                    'text': text.strip()
                }
                pdf_sections.append(pdf_section)
        else:
            logging.info(f'No PDF table of contents found, saving text per page.')
            for i in range(len(pdf_mu)):
                logging.info(f'Parsing PDF page {i+1}/{len(pdf_mu)}.')

                pdf_page = {
                    'accession_number': self.filing_info['accession_number'].iloc[0],
                    'pdf_metadata': metadata,
                    'start_page': i,
                    'end_page': i,
                    'section_name': 'page',
                    'text': pdf_mu.load_page(i).get_text().strip()
                }
                pdf_sections.append(pdf_page)

        return pd.DataFrame(pdf_sections)
    
    def parse_pdfs(self):
        '''
        Handles parsing of text from PDF document included in a given filing. 

        Returns a dataframe, with each row representing a section or page. 
        '''
        logging.info(f'Attempting to locate PDF documents of {self.type} filing for parsing.')

        filing_docs = self.split_filing_documents()
        parsed_pdfs = []
        for doc in filing_docs:
            if doc.get('doc_filename', '').lower().endswith('.pdf'):
                logging.info(f'Found a PDF file, going to attempt to parse: {doc["doc_filename"]}.')

                # Contents come UUencoded (represent PDF in ASCII chars)
                encoded_pdf_content = re.sub(r'</?pdf>', '', doc.get('doc_text', ''), flags=re.IGNORECASE)
                
                uu_encoded_bytes = io.BytesIO(encoded_pdf_content.encode('ascii'))
                pdf_bytes = io.BytesIO()

                try:
                    # Decode and write PDF content to BytesIO object
                    uu.decode(uu_encoded_bytes, pdf_bytes)
                    pdf_bytes.seek(0) # Reset pointer
                except Exception as e:
                    logging.error(f'Failed to decode UUencoded data. Error: {e}.')
                    continue
                logging.info(f'UU-decoded PDF contents.')

                # Parse contents into a DF
                parsed_pdfs.append(self.parse_pdf_to_df(pdf_bytes))
                logging.info(f'Parsed PDF metadata and text content. Current count of parsed PDFs of the current filing: {len(parsed_pdfs)}.')

        if parsed_pdfs:
            logging.info(f"Concatenating the DF's of {len(parsed_pdfs)} PDF files for the current {self.type} filing.")
            return pd.concat(parsed_pdfs, ignore_index=True)
        
        logging.info(f'Unable to find or parse any PDFs for the current {self.type} filing.')
        return pd.DataFrame()

    def is_text_page_number(self, question_text):
        '''
        Support method for find_section_with_toc, attempt to determine if the given text is simple a page number (duplicate link in my observations).
        '''
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
    
    def text_between_tags(self, start, end):
        '''
        Helper method to find_section_with_toc, extracts the text found inbetween 2 bs4 Tags/elements.
        '''
        cur = start
        found_text = ""

        # Loop through all elements inbetween the two
        while cur and cur != end:
            if isinstance(cur, NavigableString):

                text = cur.strip()
                if len(text):
                    found_text += "{} ".format(text)

            cur = cur.next_element

        return self.normalize_extracted_text(found_text.strip(), False) # Strip trailing space that the above pattern will result in
    
    def text_starting_at_tag(self, start):
        '''
        Helper method to find_section_with_toc, extracts the text found starting at a given tag through the end of the soup.
        '''
        cur = start
        found_text = ""

        # Loop through all elements
        while cur:
            if isinstance(cur, NavigableString):

                text = cur.strip()
                if len(text):
                    found_text += "{} ".format(text)

            cur = cur.next_element

        return self.normalize_extracted_text(found_text.strip(), False)
    
    def find_sections_with_toc(self, document_soup, toc_soup):
        """
        Use the hyperlinked TOC to find text sections. 
        Provide a bs4 Tag object for the located TOC. Returns a list of dictionaries:
        [
            {
            "section_name": "...",
            "section_raw_text": "...",
            "section_parsed_text": "..."
            },
            ...
        ]
        """
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

            link_dict[link_tag.get('href').replace('#', '')] = self.normalize_extracted_text(link_tag.text.strip(), True)

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
    
    def linked_toc_exists(self, document_soup):
        '''
        Helper function to below.
        Attempts to locate a table of contents by looking for a <table> element containing one or more <a href> elements.
        Returns the bs4.Element.Tag object of the first such table if it exists, or None.
        '''
        # Find all <table> tags
        all_tables = document_soup.find_all('table')
        for cur_table in all_tables:

            # Look for an <a href=...>
            links = cur_table.find_all('a', attrs = { 'href' : True })
            if len(links):
                return cur_table

        return None
    
    def get_toc_sections(self, doc_html):
        '''
        Attempts to locate and crawl the hyperlinked table of contents of an HTML document.
        Returns: A list of dicts for TOC sections if found, (in the case none is found, will be a single dict for the whole document's text)
        '''
        sections_list = []

        logging.info(f'Parsing filing HTML document for TOC.')

        # Parse using TOC if it exists
        soup = BeautifulSoup(doc_html, 'html.parser')
        toc_tag = self.linked_toc_exists(soup)
        if toc_tag:
            # TODO: better handling of table data
            doc_sections = self.find_sections_with_toc(soup, toc_tag)
            if not doc_sections:
                logging.warning(f'Found hyperlinked TOC but failed to parse it for section text. Parsing whole HTML file as one section.')
                sections_list.append({
                    'section_name': 'html_whole_doc',
                    'section_type': 'html_whole_doc',
                    #'section_raw_text': None,
                    'text': soup.get_text(separator='\n', strip=True)
                })

        else:
            doc_sections = None
            logging.warning(f'Could not find a hyperlinked TOC to crawl for text sections. Parsing whole file as one section.')
            sections_list.append({
                'section_name': 'html_whole_doc',
                'section_type': 'html_whole_doc',
                #'section_raw_text': None,
                'text': soup.get_text(separator='\n', strip=True)
            })

        # We will enforce section name uniqueness on a document level
        existing_section_names = set()
        if doc_sections:
            for result_section_data in doc_sections:

                logging.info(f'Parsing TOC section: {result_section_data.get("section_name")}')

                section_key_name = result_section_data.get('section_name', '')
                i = 2
                # Check for duplicates within the document using the set
                while section_key_name in existing_section_names:
                    section_key_name = f"{result_section_data.get('section_name')}_{i}"
                    i += 1
                
                # Add to dict after finding unused key
                text_section_info = {}
                text_section_info['section_name'] = section_key_name
                text_section_info['section_type'] = 'linked_toc_section'
                #text_section_info['section_raw_text'] = result_section_data['section_raw_text']
                text_section_info['text'] = result_section_data['section_parsed_text'] 

                sections_list.append(text_section_info)
                existing_section_names.add(section_key_name)  # Update the set with the new name

                logging.info(f'Finished parsing section: {result_section_data["section_name"]}')

        return sections_list
    
    def parse_doc_toc(self, html_content, form_type):
        '''
        Builds a dataframe of text sections extracted from the hyperlinked table of contents of an HTML document,
        or the whole document as one section if unable to locate/walk a table of contents.
        '''
        logging.info(f'Attempting to parse main document of {form_type} filing for a hyperlinked table of contents.')
        sections_list = self.get_toc_sections(html_content)
        logging.info(f'Extracted TOC sections of {form_type} filing.')

        for section in sections_list:
            # Prepare a row for each section, add accession_number column
            section['accession_number'] = self.filing_info['accession_number'].iloc[0]

        return pd.DataFrame(sections_list)
    
    def parse_toc_main_doc(self):
        '''
        Handles locating + parsing the main HTML document of a filing type without known section names,
        attempting to walk a hyperlinked table of contents for sections if present. Otherwise the entire document is 
        returned as one section. A filing ID (accession number) column is included as well.
        '''
        logging.info(f'Attempting to locate main HTML document for TOC walking of {self.type} filing.')

        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            # Look for main doc
            if doc.get('doc_filename', '').lower().endswith('.htm') and doc.get('doc_type', '').lower() == self.type.lower():
                logging.info(f'Found main filing document: {doc.get("doc_filename", "")} of type {self.type}.')

                return self.parse_doc_toc(doc.get('doc_text', ''), self.type)

        logging.error(f'Failed to find main HTML document for {self.type} filing.')
        return pd.DataFrame() 
    
    def format_address_xml(self, street1, street2, city, state, zip_code):
        '''
        Helper function to concat address components.
        '''

        if street2:
            return f'{street1}, {street2}, {city}, {state}, {zip_code}' if all([street1, city, state, zip_code, street2]) else None
        else:
            return f'{street1}, {city}, {state}, {zip_code}' if all([street1, city, state, zip_code]) else None
    
    def parse_hr_primary_xml(self, xml_content):
        '''
        Returns a dictionary
        '''
        soup = BeautifulSoup(xml_content, 'xml')

        report_info = {
            'report_yr_quarter': soup.find(re.compile(r'reportCalendarOrQuarter')).text.strip() if soup.find(re.compile(r'reportCalendarOrQuarter')) else None,
            'is_amendment': soup.find(re.compile(r'isAmendment')).text.strip() if soup.find(re.compile(r'isAmendment')) else None,
            'amendment_no': soup.find(re.compile(r'amendmentNo')).text.strip() if soup.find(re.compile(r'amendmentNo')) else None,
            'amendment_type': soup.find(re.compile(r'amendmentType')).text.strip() if soup.find(re.compile(r'amendmentType')) else None,  
            'filing_mgr_name': soup.find(re.compile(r'filingManager')).find(re.compile(r'name')).text.strip() if soup.find(re.compile(r'filingManager')) else None,
            'filing_mgr_addr': self.format_address_xml(
                street1=soup.find(re.compile(r'filingManager')).find(re.compile(r'street1')).text.strip() if soup.find(re.compile(r'filingManager')).find(re.compile(r'street1')) else None,
                street2=soup.find(re.compile(r'filingManager')).find(re.compile(r'street2')).text.strip() if soup.find(re.compile(r'filingManager')).find(re.compile(r'street2')) else None,
                city=soup.find(re.compile(r'filingManager')).find(re.compile(r'city')).text.strip() if soup.find(re.compile(r'filingManager')).find(re.compile(r'city')) else None,
                state=soup.find(re.compile(r'filingManager')).find(re.compile(r'stateOrCountry')).text.strip() if soup.find(re.compile(r'filingManager')).find(re.compile(r'stateOrCountry')) else None,
                zip_code=soup.find(re.compile(r'filingManager')).find(re.compile(r'zipCode')).text.strip() if soup.find(re.compile(r'filingManager')).find(re.compile(r'zipCode')) else None
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
        logging.info(f'Got main fields from {self.type} primary XML document. Checking for other manager info.')

        # Extract other managers information
        other_mgrs_info = soup.find_all(re.compile(r'otherManager2'))
        if other_mgrs_info:
            logging.info(f'Found other manager info on {self.type} primary XML document.')
            for mgr in other_mgrs_info:
                mgr_info = {
                    'mgr_seq': mgr.find(re.compile(r'sequenceNumber')).text.strip() if mgr.find(re.compile(r'sequenceNumber')) else None,
                    'mgr_cik': mgr.find(re.compile(r'cik')).text.strip() if mgr.find(re.compile(r'cik')) else None,
                    'mgr_13f_filenum': mgr.find(re.compile(r'form13FFileNumber')).text.strip() if mgr.find(re.compile(r'form13FFileNumber')) else None,
                    'mgr_sec_filenum': mgr.find(re.compile(r'secFileNumber')).text.strip() if mgr.find(re.compile(r'secFileNumber')) else None,
                    'mgr_crd_num': mgr.find(re.compile(r'crdNumber')).text.strip() if mgr.find(re.compile(r'crdNumber')) else None,
                    'mgr_name': mgr.find(re.compile(r'name')).text.strip() if mgr.find(re.compile(r'name')) else None
                }
                logging.info(f'Saving manager {mgr_info.get("mgr_seq")}.')
                report_info['other_mgrs'].append(mgr_info)
        else:
            report_info['other_mgrs'].append({
                'mgr_seq': 0, # Testing out defining as 0 instead of None so we can assume is always an 'int-able' field
                'mgr_cik': None,
                'mgr_13f_filenum': None,
                'mgr_sec_filenum': None,
                'mgr_crd_num': None,
                'mgr_name': None
            })
            logging.info(f'No other managers found on {self.type} primary XML document. Filing manager is the only one.')

        return report_info
    
    def parse_it_element_soup(self, element):
        '''
        Returns a dictionary from the given bs4 <infotable> element of a holdings report. 
        '''
        holding = {
        "issuer": "",
        "holding_class": "",
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
            holding["issuer"] = element.find(re.compile('nameofissuer')).text.strip()
            holding["holding_class"] = element.find(re.compile('titleofclass')).text.strip()
            holding["cusip"] = element.find(re.compile('cusip')).text.strip()
            holding["value"] = element.find(re.compile('value')).text.strip()
            holding["amount"] = element.find(re.compile('sshprnamt')).text.strip()
            holding["amt_type"] = element.find(re.compile('sshprnamttype')).text.strip()
            holding["discretion"] = element.find(re.compile('investmentdiscretion')).text.strip()

            vote_auth = element.find(re.compile('votingauthority'))
            if vote_auth:
                holding["sole_vote"] = vote_auth.find(re.compile('sole')).text.strip()
                holding['shared_vote'] = vote_auth.find(re.compile('shared')).text.strip()
                holding['no_vote'] = vote_auth.find(re.compile('none')).text.strip()
            else:
                logging.error('Failed to find voting authority XML element for holding entry, which should be mandatory.')

            # Optional
            holding['figi'] = element.find(re.compile('figi')).text.strip() if element.find(re.compile('figi')) else ''
            holding['other_mgr'] = element.find(re.compile('othermanager')).text.strip() if element.find(re.compile('othermanager')) else ''
            holding['option_type'] = element.find(re.compile('putcall')).text.strip() if element.find(re.compile('putcall')) else ''
        
        except AttributeError as e:
            logging.warning(f"Error extracting holding details: {e}. Holding data may be incomplete.") 
        
        return holding
    
    def get_hr_rows_iter(self, it_elements, mgr_df):
        '''
        Given a list of <infotable> elements, it generates dictionary objects. 
        Each containing the details of one holding as well as accession number for filing ID, for use as a dataframe row.
        '''
        for i, infotable in enumerate(it_elements):
            holding_dict = self.parse_it_element_soup(infotable)

            # Look up manager name
            other_mgr = holding_dict.get('other_mgr', '0')
            if not other_mgr or other_mgr == '0': # No <othermanager> or was set to 0 (seems to mean filing manager...)
                logging.info(f'<othermanager> value of {other_mgr} seems to refer to filing manager. Recording holding accordingly.')
                holding_dict['manager_name'] = mgr_df['filing_mgr_name'].iloc[0]
            elif not other_mgr.isnumeric(): # In some filings with no other managers on primary XML, <othermanager> on infotable holds filing manager name redundantly
                logging.info(f'Non-numeric <othermanager> value {other_mgr}, most likely refers to manager by name. Recording holding accordingly.')
                holding_dict['manager_name'] = other_mgr
            else: # If it is a number other than 0, refer to the manager dataframe
                logging.info(f'Processing holding of manager #{other_mgr}. Referring to manager dataframe.')
                if not mgr_df.empty:
                    if not mgr_df.loc[mgr_df['mgr_seq'].astype(int) == int(other_mgr), 'mgr_name'].empty:
                        holding_dict['manager_name'] = mgr_df.loc[mgr_df['mgr_seq'].astype(int) == int(other_mgr), 'mgr_name'].values[0]
                        logging.info(f'Found corresponding manager in manager dataframe, recording holding accordingly.')
                    else:
                        logging.warning(f'Was unable to find manager #{other_mgr} in manager dataframe, most likely improper use of the field by the filer.')
                        holding_dict['manager_name'] = other_mgr
                else:
                    logging.warning(f'Numeric reference to other manager #{other_mgr}, but other manager dataframe is empty...')
                    holding_dict['manager_name'] = other_mgr

            holding_dict['accession_number'] = self.filing_info['accession_number'].iloc[0]
            yield holding_dict

    def parse_hr_it_xml(self, mgr_df, it_xml_content):
        '''
        Returns a dataframe with one row per manager-issuer holding pair reported.
        '''
        logging.info(f'Attempting to parse {self.type} info table XML for holdings information.')
        soup = BeautifulSoup(it_xml_content, 'lxml')

        outer_table = soup.find(re.compile('informationtable'))
        if not outer_table:
            logging.error(f'Failed to find XML <informationtable> element in info table XML. No holdings can be extracted.')
            return pd.DataFrame()
        
        holdings_list = outer_table.find_all(re.compile('infotable'))
        if not holdings_list:
            logging.error(f'Failed to parse <informationtable> for child <infotable> elements. No holdings can be extracted.')
            return pd.DataFrame()
        
        hr_rows_iter = self.get_hr_rows_iter(holdings_list, mgr_df)
        logging.info(f'Got iterator to holdings report rows, building dataframe now.')
        return pd.DataFrame(hr_rows_iter)
        
    def parse_hr_managers(self):
        '''
        Parses the primary document XML content of a 13F filing.

        Returns a dataframe with one row per reported manager, with columns for various filing level fields and ID.
        '''
        logging.info(f'Looking for primary XML document of {self.type} filing.')
        filing_docs = self.split_filing_documents()
        primary_doc_content = ''
        for doc in filing_docs:
            if doc.get('doc_filename', '').lower().endswith('.xml') and doc.get('doc_type', '').lower() == self.type.lower():
                logging.info(f'Found primary XML document of {self.type} holdings report: {doc.get("doc_filename")}.')
                primary_doc_content = re.sub(r'</?xml>', '', doc.get('doc_text', ''), flags=re.IGNORECASE)
                break

        if not primary_doc_content:
            logging.error(f'Failed to find primary XML document of {self.type} holdings report.')
            return pd.DataFrame()
        
        primary_dict = self.parse_hr_primary_xml(primary_doc_content)
        if not primary_dict:
            logging.error(f'Failed to parse primary XML document of {self.type} holdings report.')
            return pd.DataFrame()
        
        # Build dataframe from dict, one row per manager
        mgr_rows = []
        for i, mgr in enumerate(primary_dict.get('other_mgrs', [])):
            logging.info(f'Preparing dataframe row for holding manager {i+1}/{len(primary_dict.get("other_mgrs"))}.')
            row_dict = {field: value for (field, value) in primary_dict.items() if field != 'other_mgrs'} 
            row_dict['accession_number'] = self.filing_info['accession_number'].iloc[0]
        
            for other_mgr_field, inner_value in mgr.items():
                row_dict[other_mgr_field] = inner_value
                logging.info(f'Copied inner value for {other_mgr_field} to row.')
            mgr_rows.append(row_dict)
            logging.info(f'Built dataframe row for manager {i+1}.')

        return pd.DataFrame(mgr_rows)
    
    def parse_hr_holdings(self):
        '''
        Parses the XML info table data of a 13F-HR filing, returning a dataframe with one row per manager-issuer holding pair reported.

        Relies on the ability to parse the primary document XML first to determine manager information.
        '''
        logging.info(f'Attempting to extract holdings from {self.type} report.')

        # Make sure hr_managers has been created
        if self.hr_managers.empty:
            self.hr_managers = self.parse_hr_managers()
        logging.info(f'Ensured primary XML document has been parsed first.')

        # Now XML infotable (currently assuming is only one per filing... should test more extensively)
        logging.info(f'Looking for {self.type} holdings report XML info table.')
        filing_docs = self.split_filing_documents()
        for doc in filing_docs:
            if doc.get('doc_filename', '').lower().endswith('.xml') and doc.get('doc_type', '').lower() == 'information table':
                logging.info(f'Found info table document: {doc.get("doc_filename")} of {self.type} report.')
                
                info_table_content = re.sub(r'</?xml>', '', doc.get('doc_text', ''), flags=re.IGNORECASE)
                return self.parse_hr_it_xml(self.hr_managers, info_table_content)
            
        logging.warning(f'Unable to find or parse info table XML document of {self.type} holdings report, no holdings were extracted.')
        return pd.DataFrame()
    
    def _get_parser_for(self, df_type):
        '''
        Maps the correct parser to the correct result dataframe type
        '''
        return {
            settings.NAMED_SECTIONS_TABLE: self.parse_tidy_main_doc,
            settings.EXHIBITS_TABLE: self.parse_exhibits,
            settings.SUBJECT_COS_TABLE: self.parse_filing_subject,
            settings.PDF_SECTIONS_TABLE: self.parse_pdfs,
            settings.TOC_SECTIONS_TABLE: self.parse_toc_main_doc,
            settings.HR_MANAGERS_TABLE: self.parse_hr_managers,
            settings.HOLDINGS_TABLE: self.parse_hr_holdings
        }.get(df_type, None)
    
    def output_dfs(self):
        '''
        Returns a dictionary of dataframes containing parsed data from a single filing. 
        Structure + contents depends on filing type, see top of main source/script.
        '''
        # Map filing type category to relevant attribute names list
        category_map = {
            'tidy': MasterParserClass.tidy_df_types,
            'beneficial': MasterParserClass.beneficial_owner_df_types,
            'hr': MasterParserClass.hr_df_types,
            'hr_notice': MasterParserClass.hr_notice_df_types,
            'insider': MasterParserClass.insider_df_types,
            'misc': MasterParserClass.misc_df_types
        }

        type = self.type.lower()

        if type in MasterParserClass.tidy_filing_types:
            category = 'tidy'
        elif type in MasterParserClass.beneficial_owner_types:
            category = 'beneficial'
        elif type in MasterParserClass.holdings_report_filing_types:
            category = 'hr'
        elif type in MasterParserClass.holdings_notice_types:
            category = 'hr_notice'
        elif type in MasterParserClass.insider_trans_filing_types:
            category = 'insider'
        else:
            category = 'misc'

        logging.info(f"Outputting parsed dataframes for {type} filing {self.filing_info['accession_number'].iloc[0]}. Type category: {category}.\nDataframes that will be created:{category_map.get(category, [])}")
        
        # Populate filing info record
        output = {
            settings.FILING_INFO_TABLE: self.filing_info
        }

        # Loop through the relevant attributes/dataframes
        for df_type in category_map.get(category, []):
            value = getattr(self, df_type)
            if value.empty:
                # Build any that have not been built before
                logging.info(f'Need to build {df_type} dataframe.')
                parse_method = self._get_parser_for(df_type)
                if parse_method:
                    logging.info(f'Chose parser method to build {df_type}. Handing off: {parse_method}')
                    value = parse_method()
                    setattr(self, df_type, value)
                else:
                    logging.error(f'Failed to determine correct parser method to build {df_type}.')
                    value = pd.DataFrame()
            # Populate output dict with DF
            output[df_type] = value

        return output
