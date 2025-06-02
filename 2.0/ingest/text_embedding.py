import logging
from sqlalchemy import text
from sentence_transformers import SentenceTransformer

import config.settings as settings

# Tables containing text we generate embeddings for
TEXT_TABLE_RELATIONS = settings.TEXT_TYPE_TABLES

# Define embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

def _rebuild_ivfflat_index(connection, table_name):
    '''Internal: Optimized IVFFlat index rebuild'''
    logging.info(f"Rebuilding IVFFlat index for {table_name}")
    index_name = f'idx_{table_name}_embedding_ivfflat_cosine'

    # Calculate optimal lists parameter
    list_count = connection.execute(text(f'''
        SELECT GREATEST(100, COUNT(*)/1000)::int 
        FROM {table_name}
    ''')).scalar()
    
    connection.execute(text(f'''
        DROP INDEX IF EXISTS {index_name};
        CREATE INDEX {index_name}
        ON {table_name} USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = {list_count});
        ANALYZE {table_name};
    '''))

def generate_embeddings_for_table(source_table, text_column, embeddings_table, engine):
    '''
    Generate embeddings for entries in specific table.
    '''
    with engine.connect() as conn:
        trans = conn.begin()
        
        try:
            # Get unembedded sections
            result = conn.execute(text(f'''
                SELECT id, {text_column} 
                FROM {source_table} 
                WHERE id NOT IN (
                    SELECT section_id FROM {embeddings_table}
                )
                ORDER BY id
            '''))
            rows = result.fetchall()

            if not rows:
                logging.info(f'No new text sections in {source_table}')
                return
            logging.info(f'Processing {len(rows)} new text sections from {source_table}.')
            
            # Batch processing
            batch_size = 100  # Adjust based on memory constraints
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                texts = [row[1] for row in batch]
                embeddings = model.encode(texts, show_progress_bar=False)
                
                # Batch insert
                conn.execute(text(f'''
                    INSERT INTO {embeddings_table} 
                    (section_id, embedding)
                    VALUES {','.join([f'(:id_{j}, :embedding_{j})' for j in range(len(batch))])}
                '''), {
                    **{f'id_{j}': batch[j][0] for j in range(len(batch))},
                    **{f'embedding_{j}': embeddings[j].tolist() for j in range(len(batch))}
                })
                
                logging.info(f'Processed batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} of new {source_table} text sections.')

            # Rebuild index
            logging.info(f'Finished processing embeddings of {source_table}, rebuilding index for {embeddings_table}.')
            _rebuild_ivfflat_index(conn, embeddings_table)
            logging.info(f'Rebuilt index for {embeddings_table}.')
            trans.commit()
            
        except Exception as e:
            trans.rollback()
            logging.error(f"Failed embedding {source_table}: {str(e)}")
            raise

def embed_new_text_sections(engine):
    '''
    Process all the tables containing text we want to embed + eventually analyze. 
    '''
    # TODO: Parallel via ThreadPoolExec etc
    for text_table, embedding_table in TEXT_TABLE_RELATIONS.items():
        generate_embeddings_for_table(text_table, 'text', embedding_table, engine)
