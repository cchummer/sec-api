from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class Exhibits(Base):
    __tablename__ = settings.EXHIBITS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.EXHIBITS_TABLE)
    exhibits_embeddings = relationship(settings.EXHIBITS_EMBEDDINGS_CLASS_NAME,
                            back_populates=settings.EXHIBITS_TABLE,
                            cascade="all, delete-orphan")

    exhibit_type = Column(Text)
    exhibit_meaning = Column(Text)
    text = Column(Text)