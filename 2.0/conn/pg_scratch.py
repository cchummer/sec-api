from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text

SQL_DATABASE_URL = 'postgresql+psycopg2://myuser:mypassword@localhost:5432/mydatabase'
engine = create_engine(SQL_DATABASE_URL)

model = SentenceTransformer('all-MiniLM-L6-v2')
query = "What companies mentioned the Russia and Ukraine conflict?"
embedding = model.encode(query)

with engine.connect() as conn:
                
                # Set tuning parameters for search behavior (optional but recommended)
                conn.execute(text("SET enable_seqscan = off"))  # Force index usage during testing
                conn.execute(text("SET ivfflat.probes = 10"))   # Adjust search recall/speed tradeoff

                stmt = text(f"""
                    SELECT 
                        e.section_id, 
                        s.section_name,
                        s.text, 
                        f.date, 
                        f.type,
                        f.company_name, 
                        1 - (e.embedding <=> CAST(:embedding AS vector)) AS cosine_similarity
                    FROM named_sections_embeddings e
                    JOIN named_sections s ON e.section_id = s.id
                    JOIN filing_info f ON s.accession_number = f.accession_number
                    ORDER BY cosine_similarity DESC
                    LIMIT 5
                """)
                print(f'SQL query for vector search:\n{stmt}') ### TESTING
                result = conn.execute(stmt, {
                    'embedding': embedding.tolist(),
                })

                rows = result.fetchall()
                print(f'Found {len(rows)} relevant text sections.')

                for r in rows:
                        section_info = {
                        'section_id': r.section_id,
                        'text': r.text,
                        'section_name': r.section_name,
                        'filing_date': r.date,
                        'filing_type': r.type,
                        'company_name': r.company_name,
                        'score': float(r.cosine_similarity),
                        }

                        print(f'Relevant section:\n{section_info}')
                        input()
                input()
