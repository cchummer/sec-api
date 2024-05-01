# sec-api
Updated 5/1/2024

This project is my first relatively deep venture into webscraping + web-document parsing in Python. I have chosen not to organize it into a package or even into objects/classes but instead leave each method, heavily commented, in a somehwat modular form to fit whatever use-case the reader may have. The jupyter notbooks were written and tested in Google Collaboratory but for the most part the code has also been tested on locally installed Python environments on Mac OS and Linux. I come from a Windows C coding background so forgive my for loops and other syntax preferences. Working on being more "pythonic".

Current notebooks:
  - company_search_endpoint : Methods for querying the "company" search endpoint (https://www.sec.gov/cgi-bin/browse-edgar)
  - fulltext_search_endpoint : Methods for querying the "full text" search endpoint (https://efts.sec.gov/LATEST/search-index)
  - index_endpoints : Methods for parsing both the daily-index and full-index endpoints
  - submissions_restful_api : Methods for querying the "submissions" RESTful API endpoint (https://data.sec.gov/submissions)
  - cik_ticker_lookup : Methods for quickly looking up CIK / ticker / entity name relationships
  - 13f_parsing (incomplete) : Methods for parsing 13-F holdings reports and comparing holdings across time
  - 10q_k_financial_parsing (incomplete) : Methods for parsing financial data from 10-Q and 10-K filings
  - 10q_k_text_parsing (incomplete) : Methods for parsing text from 10-Q and 10-K filings
  - s1_prospectus_parsing (incomplete) : Methods for parsing text from S-1 filings. Heavily based off code for 10-Q and 10-K parsing
  - s1_topic_analysis : Methods for performing topic analysis on S-1 filings
 
Upcoming:
  - XBRL RESTful API documentation
  - More form-specific parsing methods, including finishing 13-F and 10's. The main work to be done in these notebooks is regarding "legacy" (pre-XBRL in the case of financial data, and pre-HTML in the case of text) filings which are significantly less structured and more challenging to parse. 
