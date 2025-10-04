from flask import Blueprint, render_template, request, jsonify
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from config.log_config import config_logging
config_logging('web') ## TESTING

# Create the blueprint
industry_bp = Blueprint('industry', __name__, url_prefix='/industry-analysis')

@industry_bp.route('/division/<division_code>')
def division_analysis(division_code):
    """
    Render the industry analysis page for a specific division (2-digit SIC code)
    """
    # Get query parameters (filters passed from home page)
    timeframe = request.args.get('timeframe', 'month')
    anchor_date = request.args.get('anchor_date', datetime.now().strftime('%Y-%m-%d'))
    filing_type = request.args.get('filing_type', 'all')
    
    # TODO: Query your database for division data
    # Example structure:
    # division_data = {
    #     'code': division_code,
    #     'name': 'Manufacturing',  # Get from SIC lookup
    #     'companies': [...],  # List of companies in this division
    #     'filings': [...],  # List of filings in this division
    #     'stats': {...}  # Summary statistics
    # }
    
    # For now, pass the parameters to the template
    context = {
        'level': 'division',
        'code': division_code,
        'timeframe': timeframe,
        'anchor_date': anchor_date,
        'filing_type': filing_type,
        # Add your database query results here
    }
    
    return render_template('industry_analysis.html', **context)


@industry_bp.route('/major-group/<major_group_code>')
def major_group_analysis(major_group_code):
    """
    Render the industry analysis page for a specific major group (3-digit SIC code)
    """
    # Get query parameters
    timeframe = request.args.get('timeframe', 'month')
    anchor_date = request.args.get('anchor_date', datetime.now().strftime('%Y-%m-%d'))
    filing_type = request.args.get('filing_type', 'all')
    
    # TODO: Query your database for major group data
    
    context = {
        'level': 'major-group',
        'code': major_group_code,
        'timeframe': timeframe,
        'anchor_date': anchor_date,
        'filing_type': filing_type,
        # Add your database query results here
    }
    
    return render_template('industry_analysis.html', **context)


@industry_bp.route('/industry/<industry_code>')
def industry_analysis(industry_code):
    """
    Render the industry analysis page for a specific industry (4-digit SIC code)
    """
    # Get query parameters
    timeframe = request.args.get('timeframe', 'month')
    anchor_date = request.args.get('anchor_date', datetime.now().strftime('%Y-%m-%d'))
    filing_type = request.args.get('filing_type', 'all')
    
    # TODO: Query your database for industry data
    
    context = {
        'level': 'industry',
        'code': industry_code,
        'timeframe': timeframe,
        'anchor_date': anchor_date,
        'filing_type': filing_type,
        # Add your database query results here
    }
    
    return render_template('industry_analysis.html', **context)


# Optional: API endpoints for dynamic data loading
@industry_bp.route('/api/<level>/<code>/companies')
def get_companies(level, code):
    """
    API endpoint to get companies for a given industry level
    """
    timeframe = request.args.get('timeframe', 'month')
    anchor_date = request.args.get('anchor_date', datetime.now().strftime('%Y-%m-%d'))
    filing_type = request.args.get('filing_type', 'all')
    
    # TODO: Query database for companies
    companies = []
    
    return jsonify({
        'level': level,
        'code': code,
        'companies': companies,
        'filters': {
            'timeframe': timeframe,
            'anchor_date': anchor_date,
            'filing_type': filing_type
        }
    })


@industry_bp.route('/api/<level>/<code>/filings')
def get_filings(level, code):
    """
    API endpoint to get filings for a given industry level
    """
    timeframe = request.args.get('timeframe', 'month')
    anchor_date = request.args.get('anchor_date', datetime.now().strftime('%Y-%m-%d'))
    filing_type = request.args.get('filing_type', 'all')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # TODO: Query database for filings with pagination
    filings = []
    total = 0
    
    return jsonify({
        'level': level,
        'code': code,
        'filings': filings,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        },
        'filters': {
            'timeframe': timeframe,
            'anchor_date': anchor_date,
            'filing_type': filing_type
        }
    })