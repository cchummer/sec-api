from sqlalchemy import Column, Integer, String, Text, ForeignKey, JSON
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class BeneficialSubjects(Base):
    __tablename__ = settings.SUBJECT_COS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.SUBJECT_COS_TABLE)

    cik = Column(String(10))
    company_name = Column(Text)
    sic_code = Column(Integer)
    sic_desc = Column(Text)
    org_name = Column(Text)
    sec_file_num = Column(Text)
    film_num = Column(Text)
    state_of_incorp = Column(Text)
    fiscal_yr_end = Column(Text)
    business_address = Column(Text)
    business_phone = Column(Text)
    name_changes = Column(JSON)