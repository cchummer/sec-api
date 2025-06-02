from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class HtmlSections(Base):
    __tablename__ = settings.TOC_SECTIONS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.TOC_SECTIONS_TABLE)
    toc_sections_embeddings = relationship(settings.TOC_SECTIONS_EMBEDDINGS_CLASS_NAME,
                            back_populates=settings.TOC_SECTIONS_TABLE,
                            cascade="all, delete-orphan")

    section_name = Column(Text)
    section_type = Column(Text)
    text = Column(Text)