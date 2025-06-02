from sqlalchemy import Column, Integer, String, JSON, Text
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

from .named_sections import NamedSections
from .toc_sections import HtmlSections
from .exhibits import Exhibits
from .pdf_sections import PdfSections
from .beneficial_subjects import BeneficialSubjects
from .hr_managers import HoldingsReportManagers
from .holdings import HoldingsReportHoldings
from .named_section_embds import NamedSectionEmbeddings
from .toc_section_embds import HtmlSectionEmbeddings
from .pdf_section_embds import PdfSectionEmbeddings

class FilingInfo(Base):
    __tablename__ = settings.FILING_INFO_TABLE

    accession_number = Column(String(20), primary_key=True)
    type = Column(String(20))
    date = Column(String(8))  # stored as YYYYMMDD
    cik = Column(String(10), nullable=False)
    sic_code = Column(String(10))
    sic_desc = Column(String(255))
    company_name = Column(String(255))
    report_period = Column(String(8))
    state_of_incorp = Column(String(10))
    fiscal_yr_end = Column(String(4))
    business_address = Column(String(255))
    business_phone = Column(String(50))
    name_changes = Column(JSON)  # JSON list of dicts
    header_raw_text = Column(Text)
    filing_raw_text = Column(Text)

    named_sections = relationship(NamedSections, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    toc_sections = relationship(HtmlSections, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    exhibits = relationship(Exhibits, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    pdf_sections = relationship(PdfSections, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    beneficial_subjects = relationship(BeneficialSubjects, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    hr_managers = relationship(HoldingsReportManagers, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    holdings = relationship(HoldingsReportHoldings, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    named_section_embeddings = relationship(NamedSectionEmbeddings, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    toc_section_embeddings = relationship(HtmlSectionEmbeddings, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')
    pdf_section_embeddings = relationship(PdfSectionEmbeddings, back_populates=settings.FILING_INFO_TABLE, cascade='all, delete-orphan')