from flask import Flask, Blueprint, render_template, request, redirect, url_for, session
from get_data import refresh_csv_summary, refresh_sql_summary, refresh_sic_summary, refresh_fin_reports_summary, refresh_prospectus_reports_summary, refresh_events_summary, get_masterfiling_info, get_filing_data, data_cache
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
        refresh_csv_summary()
        refresh_sql_summary()
        
        return render_template("filings_summary.html", csv_summary=data_cache.csv_summary, sql_summary=data_cache.sql_summary)
    else:
        return redirect(url_for("routes.login"))

@routes.route('/filing-type/<filing_type>')
def filing_type_page(filing_type):
    """Display daily summary statistics depending on the filing type. Renders one of several appropriate templates."""
    if "logged_in" in session and session["logged_in"]:
        filing_type = filing_type.replace('_', '/').upper()

        # Financial report 
        if filing_type in config.FIN_REPORT_TYPES:
            refresh_fin_reports_summary(filing_type)
            return render_template("fin_reports.html", filing_type=filing_type, sql_summary=data_cache.sql_summary, reports_data=data_cache.fin_summary.get(filing_type))
        
        # Prospectus filings
        elif filing_type in config.PROSPECTUS_FILING_TYPES:
            refresh_prospectus_reports_summary(filing_type)
            return render_template('prospectus_summaries.html', filing_type=filing_type, sql_summary=data_cache.sql_summary, summaries_data=data_cache.prospectus_summary.get(filing_type))
        
        # Current event filing
        elif filing_type in config.EVENT_FILING_TYPES:
            refresh_events_summary(filing_type)
            return render_template('events.html', filing_type=filing_type, sql_summary=data_cache.sql_summary, events_data=data_cache.events_summary.get(filing_type))

        # Insider transaction filing

        # SEC staff filing

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
        refresh_sic_summary(sic)

        # Parse the topic_json for each topic detail in place.
        sic_data = data_cache.sic_summary.get(sic)
        if sic_data and 'topic_analysis_results' in sic_data:
            for run in sic_data['topic_analysis_results']:
                for detail in run.get('details', []):
                    try:
                        # Replace the JSON string with the parsed dictionary.
                        detail['topic_json'] = json.loads(detail['topic_json'])
                    except Exception as e:
                        print(f"Error parsing topic_json for detail: {e}")

        return render_template("industry.html", sql_summary=data_cache.sql_summary, sic_summary=data_cache.sic_summary.get(sic))
    else:
        return redirect(url_for("routes.login"))
    
@routes.route('/view-filing/<filing_id>')
def view_filing(filing_id):
    """Display details of a single filing"""
    if "logged_in" in session and session["logged_in"]:
        get_masterfiling_info(filing_id)

        filing_info = data_cache.indiv_filings.get(filing_id, {})
        filing_type = filing_info.get('type')

        if filing_type:
            get_filing_data(filing_id)
        else:
            print(f'No filing type found for filing_id {filing_id}.')

        return render_template('indiv_filing.html', sql_summary=data_cache.sql_summary, filing_info=data_cache.indiv_filings.get(filing_id))
    else:
        return redirect(url_for("routes.login"))