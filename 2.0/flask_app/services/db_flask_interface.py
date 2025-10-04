from sqlalchemy import text
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from conn.db_engine import engine  

class DBService:

    def __init__(self):
        self.data_cache = {
            #'sql_summary': {}
        }

    def get_current_anchor_date(self, timeframe, input_date):
        if timeframe == 'day':
            return input_date
        elif timeframe == 'week':
            return input_date - timedelta(days=input_date.weekday())  # Monday
        elif timeframe == 'month':
            return input_date.replace(day=1)
        elif timeframe == 'quarter':
            start_month = 3 * ((input_date.month - 1) // 3) + 1
            return input_date.replace(month=start_month, day=1)
        elif timeframe == 'year':
            return input_date.replace(month=1, day=1)
        else:
            raise ValueError("Invalid timeframe. Must be one of: day, week, month, quarter, year.")
    
    def calculate_query_start_end_date(self, timeframe, anchor_date):
        '''Given a timeframe (day/week/month/quarter/year) and an anchor date, will return the beginning and ending dates of the timeframe/chunk which the anchor date is in'''
        valid_frames = ["day", "week", "month", "quarter", "year"]
        if timeframe not in valid_frames:
            print(f'Invalid timeframe sent to refresh_sql_summary: {timeframe}, defaulting to weekly aggregation.')
            timeframe = "week"

        try:
            if timeframe == "day":
                start = anchor_date
                end = anchor_date + timedelta(days=1)

            elif timeframe == "week":
                start = anchor_date - timedelta(days=anchor_date.weekday())  # Monday
                end = start + timedelta(weeks=1)

            elif timeframe == "month":
                start = anchor_date.replace(day=1)
                end = start + relativedelta(months=1)

            elif timeframe == "quarter":
                quarter = (anchor_date.month - 1) // 3 + 1
                start = date(anchor_date.year, 3 * quarter - 2, 1)
                end = start + relativedelta(months=3)

            elif timeframe == "year":
                start = date(anchor_date.year, 1, 1)
                end = date(anchor_date.year + 1, 1, 1)

        except Exception as e:
            print(f'Failed to parse given anchor_date object: {e}')
            raise
        
        print(f'Calculated target start and end dates for query: {start}, {end}')
        return start, end
    
    def get_count_in_current_timeframe(self, anchor_date):
        for item in self.data_cache['sql_summary']['parsed_by_date']:
            item_date = date.fromisoformat(item['date'])
            if item_date == anchor_date:
                print(f"Found target: {item_date}, count: {item['count']}")
                return item['count']
    
    def refresh_sql_summary(self, timeframe, anchor_date, single_filing_type_filter="all"):
        """Fetch filing counts by type, filer/company, and industry, for the given time period"""
        print(f'Refreshing sql_summary dictionary, aggregating by: {timeframe}, with anchor date: {anchor_date}. Filtering by filing type: {single_filing_type_filter}')
        # Get dates
        start, end = self.calculate_query_start_end_date(timeframe, anchor_date)
        
        try:
            with engine.connect() as conn:

                # Perform queries
                result = conn.execute(text("SELECT COUNT(*) FROM filing_info;"))
                total_parsed_filings = result.fetchone()[0]

                # No filing type filter
                if single_filing_type_filter == 'all':
                    print(f'No filing type filter specified, targetting all types in given timeframe.')

                    sql = text("""
                        SELECT date_trunc(:timeframe, date)::date AS period, COUNT(*) AS count
                        FROM filing_info
                        GROUP BY period
                        ORDER BY period ASC;
                    """)
                    params = {
                        'timeframe': timeframe
                    }
                    result = conn.execute(sql, parameters=params)
                    parsed_by_date = result.fetchall()
                    
                    sql = text("""
                        SELECT type, COUNT(*) AS count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        GROUP BY type
                        ORDER BY count DESC;
                    """)
                    params = {
                        'start': start,
                        'end': end
                    }
                    result = conn.execute(sql, parameters=params)
                    parsed_by_type = result.fetchall()

                    sql = text("""
                        SELECT company_name, COUNT(*) AS count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        GROUP BY company_name
                        ORDER BY count DESC;
                    """)
                    result = conn.execute(sql, parameters=params)
                    parsed_by_company = result.fetchall()

                    sql = text("""
                        SELECT whole_sic_code, sic_desc, COUNT(*) as count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        GROUP BY whole_sic_code, sic_desc
                        ORDER BY count DESC;
                    """)
                    result = conn.execute(sql, parameters=params)
                    parsed_by_industry = result.fetchall()

                # Edit queries to filter by specified filing type
                else:
                    print(f'Filing type filter specified: {single_filing_type_filter}')
                    sql = text("""
                        SELECT date_trunc(:timeframe, date)::date AS period, COUNT(*) AS count
                        FROM filing_info
                        WHERE type = :filing_type
                        GROUP BY period
                        ORDER BY period ASC;
                    """)
                    params = {
                        'timeframe': timeframe,
                        'filing_type': single_filing_type_filter
                    }
                    result = conn.execute(sql, parameters=params)
                    parsed_by_date = result.fetchall()
                    
                    sql = text("""
                        SELECT type, COUNT(*) AS count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        AND type = :filing_type
                        GROUP BY type
                        ORDER BY count DESC;
                    """)
                    params = {
                        'start': start,
                        'end': end,
                        'filing_type': single_filing_type_filter
                    }
                    result = conn.execute(sql, parameters=params)
                    parsed_by_type = result.fetchall()

                    sql = text("""
                        SELECT company_name, COUNT(*) AS count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        AND type = :filing_type
                        GROUP BY company_name
                        ORDER BY count DESC;
                    """)
                    result = conn.execute(sql, parameters=params)
                    parsed_by_company = result.fetchall()

                    sql = text("""
                        SELECT whole_sic_code, sic_desc, COUNT(*) as count
                        FROM filing_info
                        WHERE date >= :start AND date < :end
                        AND type = :filing_type
                        GROUP BY whole_sic_code, sic_desc
                        ORDER BY count DESC;
                    """)
                    result = conn.execute(sql, parameters=params)
                    parsed_by_industry = result.fetchall()
                
                # Convert results to dictionaries
                self.data_cache['sql_summary'] = {
                    'total_filings_parsed': total_parsed_filings,
                    'parsed_by_date': [{'date': row[0].isoformat(), 'count': row[1]} for row in parsed_by_date],
                    'parsed_by_type': [{'type': row[0], 'count': row[1]} for row in parsed_by_type],
                    'parsed_by_company': [{'company_name': row[0], 'count': row[1]} for row in parsed_by_company],
                    'parsed_by_industry': [{'whole_sic_code': row[0], 'sic_desc': row[1], 'count': row[2]} for row in parsed_by_industry]
                }

                conn.close()
                print('Refreshed sql_summary dictionary.')
        except Exception as e:
            print(f"Error fetching SQL summary: {e}.")
            self.data_cache['sql_summary'] = {}