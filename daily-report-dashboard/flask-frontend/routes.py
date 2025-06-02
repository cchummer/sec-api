from flask import Flask, Blueprint, render_template, request, redirect, url_for, session, jsonify
import get_data
import summarization_module
from topic_analysis_module import topic_analyze_corpus
from urllib.parse import unquote
import config
import json

# Define the blueprint
routes = Blueprint('routes', __name__)

@routes.route("/")
def login():
    """Display the login page."""
    if "logged_in" in session and session["logged_in"]:
        return redirect(url_for("routes.home"))
    return render_template("login.html")

@routes.route("/login", methods=["POST"])
def handle_login():
    """Handle login form submission."""
    passcode = request.form.get("passcode")
    if passcode == config.LOGIN_PASSCODE:
        session["logged_in"] = True
        return redirect(url_for("routes.home"))
    else:
        return "Incorrect passcode. Please try again."

@routes.route("/logout")
def logout():
    """Log the user out."""
    session.pop("logged_in", None)
    return redirect(url_for("routes.login"))

@routes.route("/home")
def home():
    """Display the main landing page with summary stats."""
    if "logged_in" in session and session["logged_in"]:
        get_data.refresh_csv_summary()
        get_data.refresh_sql_summary()
        
        return render_template("filings_summary.html", csv_summary=get_data.data_cache.csv_summary, sql_summary=get_data.data_cache.sql_summary)
    else:
        return redirect(url_for("routes.login"))

@routes.route('/filing-type/<filing_type>')
def filing_type_page(filing_type):
    """Display daily summary statistics depending on the filing type. Renders one of several appropriate templates."""
    if "logged_in" in session and session["logged_in"]:
        filing_type = filing_type.replace('_', '/').upper()

        # Financial report 
        if filing_type in config.FIN_REPORT_TYPES:
            get_data.refresh_fin_reports_summary(filing_type)
            return render_template("fin_reports.html", filing_type=filing_type, sql_summary=get_data.data_cache.sql_summary, reports_data=get_data.data_cache.fin_summary.get(filing_type))
        
        # Prospectus filings
        elif filing_type in config.PROSPECTUS_FILING_TYPES:
            get_data.refresh_prospectus_reports_summary(filing_type)
            return render_template('prospectus_summaries.html', filing_type=filing_type, sql_summary=get_data.data_cache.sql_summary, summaries_data=get_data.data_cache.prospectus_summary.get(filing_type))
        
        # Current event filing
        elif filing_type in config.EVENT_FILING_TYPES:
            get_data.refresh_events_summary(filing_type)
            return render_template('events.html', filing_type=filing_type, sql_summary=get_data.data_cache.sql_summary, events_data=get_data.data_cache.events_summary.get(filing_type))

        # Insider transaction filing

        # SEC staff filing
        elif filing_type in config.SEC_STAFF_FILING_TYPES:
            get_data.refresh_sec_staff_summary(filing_type)
            return render_template('sec_staff_summary.html', filing_type=filing_type, sql_summary=get_data.data_cache.sql_summary, summaries_data=get_data.data_cache.sec_staff_summary.get(filing_type))

        # Proxy filing
        elif filing_type in config.PROXY_REPORT_TYPES:
            get_data.refresh_proxy_summary(filing_type)
            return render_template('proxy_filings_summary.html', filing_type=filing_type, sql_summary=get_data.data_cache.sql_summary, summaries_data=get_data.data_cache.proxy_summary.get(filing_type))
        
        # TODO etc

        else:
            print(f'Unknown filing category: {filing_type}. Not sure which page to serve...')
            return redirect(url_for("routes.home"))
    
    else:
        return redirect(url_for("routes.login"))

@routes.route('/industry/<sic>')
def industry_page(sic):
    """Display the industry summary page, giving stats on the given SIC's daily filings."""
    if "logged_in" in session and session["logged_in"]:
        get_data.refresh_sic_summary(sic)

        # Parse the topic_json for each topic detail in place.
        sic_data = get_data.data_cache.sic_summary.get(sic)
        if sic_data and 'topic_analysis_results' in sic_data:
            for run in sic_data['topic_analysis_results']:
                for detail in run.get('details', []):
                    try:
                        # Replace the JSON string with the parsed dictionary.
                        detail['topic_json'] = json.loads(detail['topic_json'])
                    except Exception as e:
                        print(f"Error parsing topic_json for detail: {e}")

        return render_template("industry.html", sql_summary=get_data.data_cache.sql_summary, sic_summary=get_data.data_cache.sic_summary.get(sic))
    else:
        return redirect(url_for("routes.login"))
    
@routes.route('/view-filing/<filing_id>')
def view_filing(filing_id):
    """Display details of a single filing"""
    if "logged_in" in session and session["logged_in"]:
        get_data.get_masterfiling_info(filing_id)

        filing_info = get_data.data_cache.indiv_filings.get(filing_id, {})
        filing_type = filing_info.get('type')

        if filing_type:
            get_data.get_filing_data(filing_id)
        else:
            print(f'No filing type found for filing_id {filing_id}.')

        return render_template('indiv_filing.html', sql_summary=get_data.data_cache.sql_summary, filing_info=get_data.data_cache.indiv_filings.get(filing_id))
    else:
        return redirect(url_for("routes.login"))
    
@routes.route('/run_filing_nlp_analysis/<filing_id>')
def run_filing_nlp_analysis(filing_id):
    '''
    Attempts to analyze all text data of the given filing.
    '''
    print(f'Attempting filing level NLP of filing ID: {filing_id}.')
    
    get_data.get_filing_data(filing_id)
    filing_data = get_data.data_cache.indiv_filings.get(filing_id, {})
    if not filing_data:
        return jsonify({'error': f'Filing data not found (ID {filing_id}).'}), 404
    
    print(f'Got filing data, running analysis.')
    
    nlp_results = summarization_module.perform_filing_level_nlp(filing_data)

    print('Returning analyzed text data from filing.')
    return jsonify(nlp_results)

@routes.route('/run_filing_section_summary/<filing_id>/<text_section_id>')
def run_filing_section_summary(filing_id, text_section_id):
    '''
    Attempts to summarize a specific section of text of the given filing.
    '''
    print(f'Attempting section level summarization of filing ID {filing_id}, text section ID {text_section_id}.')

    get_data.get_filing_data(filing_id)
    filing_data = get_data.data_cache.indiv_filings.get(filing_id, {})
    if not filing_data:
        return jsonify({'error': f'Filing data not found (ID {filing_id}).'}), 404
    
    print(f'Got filing data, running summary analysis on section ID {text_section_id}.')

    section_summary = summarization_module.perform_section_level_summary(filing_data, text_section_id)

    print(f'Returning the generated section summary.')
    return jsonify({"summary": section_summary})

@routes.route('/run_filing_document_summary/<filing_id>/<text_document_id>')
def run_filing_document_summary(filing_id, text_document_id):
    '''
    Attempts to summarize an entire text document of the given filing.
    '''
    print(f'Attempting document level summarization of filing ID {filing_id}, document ID {text_document_id}.')

    get_data.get_filing_data(filing_id)
    filing_data = get_data.data_cache.indiv_filings.get(filing_id, {})
    if not filing_data:
        return jsonify({'error': f'Filing data not found (ID {filing_id}).'}), 404
    
    print(f'Got filing data, running summary analysis on document ID {text_document_id}.')

    doc_summary = summarization_module.perform_document_level_summary(filing_data, text_document_id)

    print(f'Returning the generated document summary.')
    return jsonify({'summary': doc_summary})

@routes.route('/run_filing_section_sentiment_summary/<filing_id>/<text_section_id>')
def run_filing_section_sentiment_summary(filing_id, text_section_id):
    '''
    Attempts to identify and summarize sentimental sentences in a specific text section of the given filing.
    '''
    print(f'Attempting section level sentiment summarization of filing ID {filing_id}, text section ID {text_section_id}.')

    get_data.get_filing_data(filing_id)
    filing_data = get_data.data_cache.indiv_filings.get(filing_id, {})
    if not filing_data:
        return jsonify({'error': f'Filing data not found (ID {filing_id}).'}), 404
    
    print(f'Got filing data, running sentiment summary analysis on section ID {text_section_id}.')

    sentiment_summary = summarization_module.perform_section_level_sentiment_summary(filing_data, text_section_id)

    print(f'Returning sentimental text summary from section.')
    return jsonify(sentiment_summary)

@routes.route('/run_industry_topic_analysis/<sic>')
def run_industry_topic_analysis(sic):
    '''
    Attempts to perform an industry level topic analysis, filtering text sections based on the given SIC.
    '''
    print(f'Attempting industry level topic analysis for SIC {sic}.')

    get_data.get_industry_text_sections(sic)
    industry_text_sections = get_data.data_cache.industry_text_sections.get(sic, [])
    if not industry_text_sections:
        return jsonify({'error': f'Failed to retrieve text sections for industry {sic}.'})
    
    print(f'Got text sections for SIC {sic}, running topic analysis.')

    # Relies on SQL table structure
    prepped_text_sections = [ { 'doc_id' : x[0], 'doc_text' : x[4] } for x in industry_text_sections]

    run_metadata = {
        'run_type': 'industry_level',
        'industry': sic
    }
    industry_topics = topic_analyze_corpus(prepped_text_sections, additional_metadata=run_metadata)

    # Process dictionary structure...

    print(f'Returning industry level topic analysis (SIC {sic}).')
    input() ## testing
    return jsonify(industry_topics)