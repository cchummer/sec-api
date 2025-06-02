from flask import Flask
import os
import logging

from config.log_config import config_logging

# Configure logging when module is imported
config_logging('web')

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Register route blueprints
from routes import all_blueprints
for bp in all_blueprints:
    app.register_blueprint(bp)
    logging.info(f'Registered flask route blueprint: {bp.name}')
