from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class NamedSections(Base):
    __tablename__ = settings.NAMED_SECTIONS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.NAMED_SECTIONS_TABLE)
    named_sections_embeddings = relationship(settings.NAMED_SECTIONS_EMBEDDINGS_CLASS_NAME,
                            back_populates=settings.NAMED_SECTIONS_TABLE,
                            cascade="all, delete-orphan")

    section_name = Column(Text)
    section_meaning = Column(Text)
    text = Column(Text)