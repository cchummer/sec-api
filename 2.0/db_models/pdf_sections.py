from sqlalchemy import Column, Integer, String, Text, ForeignKey, JSON
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class PdfSections(Base):
    __tablename__ = settings.PDF_SECTIONS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.PDF_SECTIONS_TABLE)
    pdf_sections_embeddings = relationship(settings.PDF_SECTIONS_EMBEDDINGS_CLASS_NAME,
                            back_populates=settings.PDF_SECTIONS_TABLE,
                            cascade="all, delete-orphan")

    pdf_metadata = Column(JSON)
    start_page = Column(Integer)
    end_page = Column(Integer)
    section_name = Column(Text)
    text = Column(Text)