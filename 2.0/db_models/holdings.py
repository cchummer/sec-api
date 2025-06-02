from sqlalchemy import Column, Integer, BigInteger, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
import config.settings as settings

class HoldingsReportHoldings(Base):
    __tablename__ = settings.HOLDINGS_TABLE

    id = Column(Integer, primary_key=True)
    accession_number = Column(String, ForeignKey(f'{settings.FILING_INFO_TABLE}.accession_number'))
    filing_info = relationship(settings.FILING_INFO_CLASS_NAME, back_populates=settings.HOLDINGS_TABLE)

    issuer = Column(Text)
    holding_class = Column(Text)
    cusip = Column(Text)
    value = Column(BigInteger)
    amount = Column(BigInteger)
    amt_type = Column(Text)
    discretion = Column(Text)
    sole_vote = Column(Text)
    shared_vote = Column(Text)
    no_vote = Column(Text)
    figi = Column(String(12))
    other_mgr = Column(Text)
    manager_name = Column(Text)
    option_type = Column(Text)