from flask import Blueprint, request, jsonify
import logging

from config.log_config import config_logging
from services.llm_service import RAGService

rag_bp = Blueprint('rag', __name__, url_prefix='/api/rag')
rag_service = RAGService()
config_logging('web') ### TESTING. Maybe move to routes __init__???

@rag_bp.route('/query', methods=['POST'])
def handle_rag_query():
    data = request.get_json()
    query = data.get('query', '').strip()
    logging.info(f'RAG query route processing query: {query}')
    
    if not query or len(query) < 3:
        return jsonify({"error": "Query too short"}), 400
    
    try:
        result = rag_service.rag_query(query)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500