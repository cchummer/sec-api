from sqlalchemy import Column, String, Integer, Date, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class HoldingsReportManagers(Base):
    __tablename__ = settings.HR_MANAGERS_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.HR_MANAGERS_TABLE)

    # Filing-level fields
    report_yr_quarter = Column(String)
    is_amendment = Column(String)
    amendment_no = Column(String)
    amendment_type = Column(String)
    filing_mgr_name = Column(String)
    filing_mgr_addr = Column(String)
    report_type = Column(String)
    form13f_filenum = Column(String)
    sec_filenum = Column(String)
    info_instruction5 = Column(String)
    sig_name = Column(String)
    sig_title = Column(String)
    sig_phone = Column(String)
    sic_loc = Column(String)
    sig_date = Column(String)  # Could use Date if you parse it

    other_mgrs_count = Column(String)
    it_entries_count = Column(String)
    it_value_total = Column(String)

    # Per-manager fields
    mgr_seq = Column(Integer)
    mgr_cik = Column(String)
    mgr_13f_filenum = Column(String)
    mgr_sec_filenum = Column(String)
    mgr_crd_num = Column(String)
    mgr_name = Column(String)
