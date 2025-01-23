from flask import Flask, Blueprint, render_template, request, redirect, url_for, session
from get_data import refresh_csv_summary, refresh_sql_summary, refresh_sic_summary, refresh_fin_reports_summary, refresh_prospectus_reports_summary, refresh_events_summary, data_cache
from urllib.parse import unquote
import config

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
        
        return render_template("main.html", csv_summary=data_cache.csv_summary, sql_summary=data_cache.sql_summary)
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
            return render_template("fin_reports.html", filing_type=filing_type, reports_data=data_cache.fin_summary[filing_type])
        
        # Prospectus filings
        elif filing_type in config.PROSPECTUS_FILING_TYPES:
            refresh_prospectus_reports_summary(filing_type)
            return render_template('prospectus_summaries.html', filing_type=filing_type, summaries_data=data_cache.prospectus_summary[filing_type])
        
        # Current event filing
        elif filing_type in config.EVENT_FILING_TYPES:
            refresh_events_summary()
            return render_template('events.html', sql_summary=data_cache.sql_summary, events_data=data_cache.events_summary)

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

        return render_template("industry.html", sic_summary=data_cache.sic_summary[sic])
    else:
        return redirect(url_for("routes.login"))