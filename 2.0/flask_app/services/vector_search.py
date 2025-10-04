import logging
from typing import List, Dict
from sentence_transformers import SentenceTransformer
import numpy as np
from sqlalchemy import text

import config.settings as settings
from conn.db_engine import engine  

class VectorSearch:
    def __init__(self, model_name: str = settings.DEFAULT_EMBEDDING_MODEL):
        self.model_name = model_name
        self.model = None
        self._initialize()

    def _initialize(self):
        try:
            self.model = SentenceTransformer(self.model_name)
            logging.info("Vector search initialized successfully")
        except Exception as e:
            logging.error(f"Model initialization failed: {str(e)}")
            raise

    def get_embedding(self, text: str) -> np.ndarray:
        return self.model.encode(text)

    def _get_section_name_expr(self, table_name):
        if table_name == 'exhibits':
            return """
            CASE 
                WHEN s.exhibit_meaning IS NOT NULL 
                    THEN s.exhibit_type || ' – ' || s.exhibit_meaning
                ELSE s.exhibit_type
            END
            """
        else:
            return "s.section_name"

    def search_sections(self, query: str, table_name: str, embed_table_name: str, top_k: int = 5, threshold: float = 0.25) -> List[Dict]: # TESTING lowering threshhold from 0.5
        try:
            logging.info(f'Searching {table_name} for text sections most relevant to query: {query}\nMinimum similarity threshold: {threshold}.')
            query_embedding = self.get_embedding(query)
            section_name_expr = self._get_section_name_expr(table_name)

            with engine.connect() as conn:
                
                # Set tuning parameters for search behavior (optional but recommended)
                conn.execute(text("SET enable_seqscan = off"))  # Force index usage during testing
                conn.execute(text("SET ivfflat.probes = 10"))   # Adjust search recall/speed tradeoff

                stmt = text(f"""
                    SELECT 
                        e.section_id, 
                        {section_name_expr} AS section_name,
                        s.text, 
                        f.date, 
                        f.type,
                        f.company_name, 
                        1 - (e.embedding <=> CAST(:embedding AS vector)) AS cosine_similarity
                    FROM {embed_table_name} e
                    JOIN {table_name} s ON e.section_id = s.id
                    JOIN {settings.FILING_INFO_TABLE} f ON s.accession_number = f.accession_number
                    WHERE 1 - (e.embedding <=> CAST(:embedding AS vector)) > :threshold
                    ORDER BY cosine_similarity DESC
                    LIMIT :top_k
                """)
                logging.info(f'SQL query for vector search:\n{stmt}') ### TESTING
                result = conn.execute(stmt, {
                    'embedding': query_embedding.tolist(),
                    'threshold': threshold,
                    'top_k': top_k
                })

                rows = result.fetchall()
                logging.info(f'Found {len(rows)} relevant {table_name} text sections.')

            return [{
                'section_id': r.section_id,
                'text': r.text,
                'section_name': r.section_name,
                'filing_date': r.date,
                'filing_type': r.type,
                'company_name': r.company_name,
                'score': float(r.cosine_similarity),
                'source': table_name
            } for r in rows]

        except Exception as e:
            logging.error(f"Search failed: {str(e)}")
            return []

