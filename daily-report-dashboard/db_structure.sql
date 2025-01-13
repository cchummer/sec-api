/*

Tables for daily SEC filing analysis.

Delete with code:

-- Drop 8-K Event Items Table
DROP TABLE IF EXISTS Event8KItems;

-- Drop 8-K Event Filing Table
DROP TABLE IF EXISTS Event8K;

-- Drop Form 4 Signatures Table
DROP TABLE IF EXISTS Form4SignatureInfo;

-- Drop Form 4 Footnotes Table
DROP TABLE IF EXISTS Form4Footnotes;

-- Drop Form 4 Non-Derivative Transaction Facts Table
DROP TABLE IF EXISTS Form4NonDerivTransactionInfo;

-- Drop Form 4 Owner Table
DROP TABLE IF EXISTS Form4OwnerInfo;

-- Drop Form 4 Issuer Table
DROP TABLE IF EXISTS Form4IssuerInfo;

-- Drop Holdings Report Entries/Facts Table
DROP TABLE IF EXISTS HoldingsEntries;

-- Drop Other Managers Table
DROP TABLE IF EXISTS OtherManagers;

-- Drop Holdings Report Table
DROP TABLE IF EXISTS HoldingsReport;

-- Drop PDF Page Text Table
DROP TABLE IF EXISTS PDFPageText;

-- Drop PDF Document Table
DROP TABLE IF EXISTS PDFDocument;

-- Drop Text Section Facts Table
DROP TABLE IF EXISTS TextSectionFacts;

-- Drop Financial Report Facts Table
DROP TABLE IF EXISTS FinancialReportFacts;

-- Drop Text Document Table
DROP TABLE IF EXISTS TextDocument;

-- Drop Financial Report Table
DROP TABLE IF EXISTS FinancialReport;

-- Drop Master Filing Table
DROP TABLE IF EXISTS MasterFiling;

*/

-- Master Filing Table
CREATE TABLE MasterFiling (
    filing_id INT PRIMARY KEY IDENTITY(1,1), 
    cik VARCHAR(10) NOT NULL,
    type VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    accession_number VARCHAR(20) NOT NULL,
    company_name VARCHAR(255),
    sic_code VARCHAR(10),
    sic_desc VARCHAR(255),
    report_period VARCHAR(20),
    state_of_incorp VARCHAR(2),
    fiscal_yr_end VARCHAR(4),
    business_address VARCHAR(255),
    business_phone VARCHAR(20),
    name_changes VARCHAR(500)
);

-- Financial Report Table
CREATE TABLE FinancialReport (
    financial_report_id INT PRIMARY KEY IDENTITY(1,1), 
    filing_id INT NOT NULL,
    report_doc VARCHAR(255),
    report_name VARCHAR(255),
    report_title_read VARCHAR(255),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Text Document Table
CREATE TABLE TextDocument (
    text_document_id INT PRIMARY KEY IDENTITY(1,1), 
    filing_id INT NOT NULL,
    section_doc VARCHAR(255),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Financial Report Facts Table
CREATE TABLE FinancialReportFacts (
    fact_id INT PRIMARY KEY IDENTITY(1,1), 
    financial_report_id INT NOT NULL,
    account_name VARCHAR(500) NOT NULL,
    time_period VARCHAR(255) NOT NULL,
    value VARCHAR(255),
    FOREIGN KEY (financial_report_id) REFERENCES FinancialReport(financial_report_id) ON DELETE CASCADE
);

-- Text Section Facts Table
CREATE TABLE TextSectionFacts (
    text_section_id INT PRIMARY KEY IDENTITY(1,1), 
    text_document_id INT NOT NULL,
    section_name VARCHAR(500),
    section_type VARCHAR(100),
    section_text VARCHAR(MAX), -- Changed from TEXT to VARCHAR(MAX)
    FOREIGN KEY (text_document_id) REFERENCES TextDocument(text_document_id) ON DELETE CASCADE
);

-- PDF Document Table
CREATE TABLE PDFDocument (
    pdf_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    pdf_name VARCHAR(255),
    doc_type VARCHAR(100),
    metadata NVARCHAR(MAX), -- Changed from JSON to NVARCHAR(MAX)
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- PDF Page Text Table
CREATE TABLE PDFPageText (
    page_id INT PRIMARY KEY IDENTITY(1,1),
    pdf_id INT NOT NULL, 
    page_num INT NOT NULL,
    page_text VARCHAR(MAX), -- Changed from TEXT to VARCHAR(MAX)
    FOREIGN KEY (pdf_id) REFERENCES PDFDocument(pdf_id) ON DELETE CASCADE
);

-- Holdings Report Table
CREATE TABLE HoldingsReport (
    holdings_report_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    report_yr_quarter VARCHAR(20),
    is_amendment BIT,
    amendment_no VARCHAR(10),
    amendment_type VARCHAR(50),
    filing_mgr_name VARCHAR(255),
    filing_mgr_addr VARCHAR(255),
    report_type VARCHAR(50),
    form13f_filenum VARCHAR(50),
    sec_filenum VARCHAR(50),
    info_instruction5 VARCHAR(MAX), -- Changed from TEXT to VARCHAR(MAX)
    sig_name VARCHAR(255),
    sig_title VARCHAR(100),
    sig_phone VARCHAR(20),
    sic_loc VARCHAR(50),
    sig_date DATE,
    other_mgrs_count INT,
    it_entries_count INT,
    it_value_total DECIMAL(18, 2),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Other Managers Table
CREATE TABLE OtherManagers (
    manager_id INT PRIMARY KEY IDENTITY(1,1),
    holdings_report_id INT NOT NULL, 
    mgr_seq INT,
    mgr_cik VARCHAR(10),
    mgr_13f_filenum VARCHAR(50),
    mgr_sec_filenum VARCHAR(50),
    mgr_crd_num VARCHAR(50),
    mgr_name VARCHAR(255),
    FOREIGN KEY (holdings_report_id) REFERENCES HoldingsReport(holdings_report_id) ON DELETE CASCADE
);

-- Holdings Report Entries/Facts Table
CREATE TABLE HoldingsEntries (
    holding_entry_id INT PRIMARY KEY IDENTITY(1,1),
    holdings_report_id INT NOT NULL, 
    issuer VARCHAR(255),
    class VARCHAR(255),
    cusip VARCHAR(50),
    value DECIMAL(18, 2),
    amount INT,
    amt_type VARCHAR(10),
    discretion VARCHAR(255),
    sole_vote INT,
    shared_vote INT,
    no_vote INT,
    figi VARCHAR(50),
    other_manager VARCHAR(255),
    option_type VARCHAR(10),
    FOREIGN KEY (holdings_report_id) REFERENCES HoldingsReport(holdings_report_id) ON DELETE CASCADE
);

-- Form 4 Issuer Table
CREATE TABLE Form4IssuerInfo (
    issuer_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL,
    issuer_cik VARCHAR(10),
    issuer_name VARCHAR(255),
    issuer_trading_symbol VARCHAR(10),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Form 4 Owner Table
CREATE TABLE Form4OwnerInfo (
    owner_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    owner_cik VARCHAR(10),
    owner_name VARCHAR(255),
    owner_city VARCHAR(100),
    owner_state VARCHAR(100),
    is_officer BIT,
    officer_title VARCHAR(255),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Form 4 Non-Derivative Transaction Facts Table
CREATE TABLE Form4NonDerivTransactionInfo (
    transaction_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    security_title VARCHAR(255),
    transaction_date DATE,
    transaction_code VARCHAR(10),
    transaction_shares DECIMAL(18, 2),
    transaction_price_per_share DECIMAL(18, 2),
    transaction_acquired_disposed_code CHAR(1),
    shares_owned_following_transaction DECIMAL(18, 2),
    direct_or_indirect_ownership VARCHAR(50),
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Form 4 Footnotes Table
CREATE TABLE Form4Footnotes (
    footnote_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    footnote_ref_id VARCHAR(10),
    footnote_text VARCHAR(MAX), -- Changed from TEXT to VARCHAR(MAX)
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- Form 4 Signatures Table
CREATE TABLE Form4SignatureInfo (
    signature_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    signature_name VARCHAR(255),
    signature_date DATE,
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE
);

-- 8-K Event Filing Table
CREATE TABLE Event8K (
    event_id INT PRIMARY KEY IDENTITY(1,1),
    filing_id INT NOT NULL, 
    event_info NVARCHAR(MAX), -- Changed from JSON to NVARCHAR(MAX)
    FOREIGN KEY (filing_id) REFERENCES MasterFiling(filing_id) ON DELETE CASCADE 
);

-- 8-K Event Items Table
CREATE TABLE Event8KItems (
    item_id INT PRIMARY KEY IDENTITY(1,1),
    event_id INT NOT NULL,     
    item_name VARCHAR(255) NOT NULL,  
    FOREIGN KEY (event_id) REFERENCES Event8K(event_id) ON DELETE CASCADE
);
