from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, func
from sqlalchemy.dialects.postgresql import JSON
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class PdfSectionEmbeddings(Base):
    __tablename__ = settings.PDF_SECTIONS_EMBEDDINGS_TABLE

    id = Column(Integer, primary_key=True)
    # Relationships
    #filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.PDF_SECTION_EMBEDDINGS_TABLE)
    pdf_sections = relationship(settings.PDF_SECTIONS_CLASS_NAME, back_populates=settings.PDF_SECTIONS_EMBEDDINGS_TABLE)
    
    '''
    # Foreign key to filing_info (the main filing record)
    accession_number = Column(String(20), 
                            ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'),
                            nullable=False,
                            index=True)
    '''
    
    # Foreign key to pdf_sections 
    section_id = Column(Integer, 
                       ForeignKey(f'{settings.PDF_SECTIONS_TABLE}.id'),
                       nullable=True,
                       index=True)
    
    # The vector embedding (384 dimensions for all-MiniLM-L6-v2)
    embedding = Column(Vector(384),  # pgvector column type
                      nullable=False)
    
    # The actual text that was embedded (for reference/debugging)
    #text = Column(Text, nullable=False)
    
    # Metadata about the embedding (optional but useful)
    #section_name = Column(String(255), nullable=True)  # e.g., "Management Discussion"
    embedding_model = Column(String(50), default=settings.DEFAULT_EMBEDDING_MODEL)
    created_at = Column(DateTime, server_default=func.now())