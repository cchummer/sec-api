from .dashboard import dashboard_bp
from .flows import flows_bp
from .rag import rag_bp

all_blueprints = [
    dashboard_bp,
    flows_bp,
    rag_bp
]