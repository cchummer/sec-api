from db_models import Base
from .db_engine import engine
from sqlalchemy import inspect, text
import logging

def create_db_tables():
    '''
    Enables pgvector (extension must be installed) and creates the model classes found in Base.
    '''

    # TODO: Proper indexing for embedding tables
    
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.commit()
    logging.info(f'Enabled pgvector extension in DB (if not already).')

    model_classes = [mapper.class_ for mapper in Base.registry.mappers]
    for cls in model_classes:
        logging.info(f'Found model class (table definition): {cls.__name__} in {cls.__module__}.')

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    logging.info(f'Tables found in db:')
    for table in existing_tables:
        logging.info(f' - {table}')

    defined_tables = Base.metadata.tables.keys()
    new_tables = set(defined_tables) - set(existing_tables)

    logging.info(f'Tables to be created:')
    for table in new_tables:
        logging.info(f' - {table}')

    Base.metadata.create_all(bind=engine)