import logging
import json
from sqlalchemy.dialects.postgresql import insert

from db_models.filing_info import FilingInfo
from db_models.named_sections import NamedSections
from db_models.named_section_embds import NamedSectionEmbeddings
from db_models.pdf_sections import PdfSections
from db_models.pdf_section_embds import PdfSectionEmbeddings
from db_models.toc_sections import HtmlSections
from db_models.toc_section_embds import HtmlSectionEmbeddings
from db_models.hr_managers import HoldingsReportManagers
from db_models.holdings import HoldingsReportHoldings
from db_models.exhibits import Exhibits

TABLE_CLASS_MAP = {
    'filing_info': FilingInfo,
    'named_sections': NamedSections,
    'named_section_embeddings': NamedSectionEmbeddings,
    'pdf_sections': PdfSections,
    'pdf_section_embeddings': PdfSectionEmbeddings,
    'toc_sections': HtmlSections,
    'toc_section_embeddings': HtmlSectionEmbeddings,
    'hr_managers': HoldingsReportManagers,
    'holdings': HoldingsReportHoldings,
    'exhibits': Exhibits,
}

def serialize_nonbasic_df_columns(df):
        '''
        Helper function to convert lists/dicts to JSON strings for SQL storage
        '''
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                logging.info(f'Serialized dataframe column to JSON string: {col}.')

        return df

def ingest_dataframe_on_conflict(df, table_name, engine):
    '''
    Testing, new method with handling of PK conflicts.
    '''
    df = serialize_nonbasic_df_columns(df)
    model = TABLE_CLASS_MAP.get(table_name)
    with engine.begin() as conn:
        for row in df.to_dict(orient='records'):
            stmt = insert(model).values(**row)
            stmt = stmt.on_conflict_do_nothing(index_elements=['accession_number']) 
            try:
                conn.execute(stmt)
            except Exception as e:
                logging.error(f"Failed to insert row {row['accession_number']}: {e}")

def ingest_dataframe(df, table_name, engine):
    df = serialize_nonbasic_df_columns(df)

    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
    except Exception as e:
        logging.error(f'Failed to ingest {table_name} DF into database: {e}.')
