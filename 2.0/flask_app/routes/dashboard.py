from flask import Blueprint, render_template, jsonify, session, redirect, url_for, request
from datetime import date
from dateutil.relativedelta import relativedelta

from services.db_flask_interface import DBService
from services.sic_hierarchy import build_sic_hierarchy
import config.settings as settings
from config.log_config import config_logging
config_logging('web') ## TESTING

dashboard_bp = Blueprint('dashboard', __name__)

db_service = DBService()

@dashboard_bp.route("/")
def login():
    """Display the login page."""
    if "logged_in" in session and session["logged_in"]:
        return redirect(url_for("dashboard.home"))
    return render_template("login.html")

@dashboard_bp.route("/login", methods=["POST"])
def handle_login():
    """Handle login form submission."""
    passcode = request.form.get("passcode")
    if passcode == settings.FLASK_LOGIN_PASSCODE:
        session["logged_in"] = True
        return redirect(url_for("dashboard.home"))
    else:
        return "Incorrect passcode. Please try again."

@dashboard_bp.route("/logout")
def logout():
    """Log the user out."""
    session.pop("logged_in", None)
    return redirect(url_for("dashboard.login"))

@dashboard_bp.route("/home")
def home():
    """Display the main landing page with summary stats."""
    if "logged_in" in session and session["logged_in"]:
        timeframe = request.args.get("timeframe", "week")  # default to weekly agg.
        anchor_str = request.args.get("anchor_date", str(date.today()))
        filing_type_filter = request.args.get("filing_type", "all")
        print(f'/home route processing request. Arguments:\ntimeframe: {timeframe}, anchor_str: {anchor_str}, filing_type_filter: {filing_type_filter}')

        anchor_date = db_service.get_current_anchor_date(timeframe, date.fromisoformat(anchor_str))
        print(f'Calculated anchor_date based on anchor_str argument: {anchor_date}')
        db_service.refresh_sql_summary(timeframe, anchor_date, filing_type_filter)
        print(f'Refreshed SQL summary, now constructing hierarchical representation.') 
        sic_hierarchy = build_sic_hierarchy(db_service.data_cache.get('sql_summary', {}).get('parsed_by_industry', {}))

        # Calculate statistics
        current_count = sum(item['count'] for item in db_service.data_cache.get('sql_summary', {}).get('parsed_by_industry', {}))
        total_industries = len(db_service.data_cache.get('sql_summary', {}).get('parsed_by_industry', {}))
        group_count = sum(len(major_group.get('groups')) for major_group in sic_hierarchy.values())
        division_count = len(sic_hierarchy)
        print(f'current_count for timeframe + filing type selection: {current_count}\nTotal # industries represented: {total_industries}\nIndustry divisions: {division_count}\nMajor groups: {group_count}')

        return render_template("filings_summary.html", 
                            sql_summary=db_service.data_cache.get('sql_summary', {}),
                            sic_hierarchy=sic_hierarchy, 
                            timeframe=timeframe,
                            anchor_date=anchor_date,
                            filing_type=filing_type_filter,
                            current_count=current_count,
                            total_industries=total_industries,
                            division_count=division_count,
                            total_major_groups=group_count)
    else:
        return redirect(url_for("dashboard.login"))
    
@dashboard_bp.route('/filing-type/<filing_type>')
def filing_type_page(filing_type):
    """Display daily summary statistics depending on the filing type. Renders one of several appropriate templates."""
    if "logged_in" in session and session["logged_in"]:
        filing_type = filing_type.replace('_', '/').upper()

        # TODO: Implement
    else:
        return redirect(url_for("dashboard.login"))

@dashboard_bp.route('/industry')
def industry_page():
    pass # TODO