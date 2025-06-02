import logging

from config.log_config import config_logging

logconfig_dict = {}  # prevents Gunicorn from overriding your logging
capture_output = True
enable_stdio_inheritance = True

accesslog = '-'    # optional: sends access logs to stdout
errorlog = '-'     # optional: sends error logs to stdout

def when_ready(server):
    config_logging('web')
    logging.info("Gunicorn master is ready. Forking workers...")

def post_fork(server, worker):
    config_logging('web')
    logging.info("Logging configured in Gunicorn worker")