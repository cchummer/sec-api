from flask import Flask
from routes import routes # Import routes blueprint
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.register_blueprint(routes) # Register it

if __name__ == "__main__":
    app.run(debug=True)
