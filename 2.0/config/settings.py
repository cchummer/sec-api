TARGET_FILING_TYPES = ['10-q', '10-k', '8-k', 's-1', 's-3', 'def 14a', 'sec staff action', '13f-hr', '13f-nt', 'sc 13d', 'sc 13g']

SEC_RATE_LIMIT = 10 # Req/s
SEC_REQ_HEADERS = { 
    "User-Agent": "Scholarly Research Project securedhummer@gmail.com",
    "Accept-Encoding": "gzip, deflate", 
    "Host": "www.sec.gov"
    }

STORAGE_DIR = 'data'
#SQL_DATABASE_URL = f'sqlite:///{STORAGE_DIR}/test.db'
SQL_DATABASE_URL = 'postgresql+psycopg2://myuser:mypassword@db:5432/mydatabase' # NOTE: The host 'db' must be defined in whatever container service is using this string

FILING_INFO_TABLE = 'filing_info'
FILING_INFO_CLASS_NAME = 'FilingInfo'
NAMED_SECTIONS_TABLE = 'named_sections'
NAMED_SECTIONS_CLASS_NAME = 'NamedSections'
NAMED_SECTIONS_EMBEDDINGS_TABLE = 'named_sections_embeddings'
NAMED_SECTIONS_EMBEDDINGS_CLASS_NAME = 'NamedSectionEmbeddings'
EXHIBITS_TABLE = 'exhibits'
EXHIBITS_CLASS_NAME = 'Exhibits'
EXHIBITS_EMBEDDINGS_TABLE = 'exhibits_embeddings'
EXHIBITS_EMBEDDINGS_CLASS_NAME = 'ExhibitEmbeddings'
PDF_SECTIONS_TABLE = 'pdf_sections'
PDF_SECTIONS_CLASS_NAME = 'PdfSections'
PDF_SECTIONS_EMBEDDINGS_TABLE = 'pdf_sections_embeddings'
PDF_SECTIONS_EMBEDDINGS_CLASS_NAME = 'PdfSectionEmbeddings'
SUBJECT_COS_TABLE = 'subject_cos'
TOC_SECTIONS_TABLE = 'toc_sections'
TOC_SECTIONS_CLASS_NAME = 'HtmlSections'
TOC_SECTIONS_EMBEDDINGS_TABLE = 'toc_sections_embeddings'
TOC_SECTIONS_EMBEDDINGS_CLASS_NAME = 'HtmlSectionEmbeddings'
HR_MANAGERS_TABLE = 'hr_managers'
HOLDINGS_TABLE = 'holdings'

# Tables containing text we will embed for analysis, and their corresponding embedding tables.
TEXT_TYPE_TABLES = { 
    NAMED_SECTIONS_TABLE: NAMED_SECTIONS_EMBEDDINGS_TABLE, 
    TOC_SECTIONS_TABLE: TOC_SECTIONS_EMBEDDINGS_TABLE, 
    PDF_SECTIONS_TABLE: PDF_SECTIONS_EMBEDDINGS_TABLE, 
    EXHIBITS_TABLE: EXHIBITS_EMBEDDINGS_TABLE 
    }

PREFECT_FLOW_NAME = 'full_process_day_flow'