import os
import requests
import logging
from typing import List, Dict

from .vector_search import VectorSearch
import config.settings as settings

class RAGService:
    def __init__(self):
        self.ollama_url = os.getenv("OLLAMA_ENDPOINT")
        self.ollama_model = os.getenv("OLLAMA_MODEL")
        self.vector_search = VectorSearch()
    
    def query_ollama(self, prompt: str, context: str) -> str:
        logging.info(f'Querying ollama with prompt: {prompt}\nContext: {context}')
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": f"Use the following SEC filing excerpts to answer the user's question\n\n: {context}\n\nQuestion: {prompt}\nAnswer:",
                    "stream": False
                },
                timeout=300
            )
            logging.info(f'Sent ollama query.')
            return response.json().get("response", "No answer generated")
        except Exception as e:
            print(f"LLM query failed: {e}")
            return "I couldn't process that request."

    def rag_query(self, user_query: str) -> Dict:
        logging.info(f'Performing RAG query using user query: {user_query}')
        # 1. Retrieve relevant sections
        results = []
        for text_table, embed_table in settings.TEXT_TYPE_TABLES.items():
            results.extend(self.vector_search.search_sections(user_query, text_table, embed_table))
            logging.info(f'Found top text section matches for user query in {text_table}.')
        
        top_results = sorted(results, key=lambda x: x['score'], reverse=True)[:5]

        # TODO: CONSIDER SUMMARIZATION HERE! ~6-8k token limit
        
        # 2. Generate context string
        context = "\n\n".join(
            f"Filer: {res['company_name']} ({res['filing_date']})\n"
            f"Filing type: {res['filing_type']}\n"
            f"Section name: {res['section_name']}\n"
            f"Content: {res['text'][:1000]}...\n" # TODO summarize instead of truncate
            f"Relevance: {res['score']:.2f}"
            for res in top_results
        )
        logging.info(f'Built query context string: {context}')
        
        # 3. Get LLM response
        answer = self.query_ollama(user_query, context)
        logging.info(f'Ollama answered, returning to user.')
        
        return {
            "answer": answer,
            "sources": top_results,
            "context": context
        }