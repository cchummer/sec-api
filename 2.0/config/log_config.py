import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def config_logging(service_name: str):
    log_dir = "/var/log/app"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{service_name}.log")

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=3)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = []  # Clear default handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    # Also redirect Flask's internal logs (werkzeug) to the same handlers
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)
    werkzeug_logger.handlers = []
    werkzeug_logger.addHandler(file_handler)
    werkzeug_logger.addHandler(stream_handler)

    logging.info(f'Configured logging for current service: {service_name}')