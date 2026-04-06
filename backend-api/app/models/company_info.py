"""SQLAlchemy model for company info table."""

from sqlalchemy import Column, String

from app.database import Base


class CompanyInfo(Base):
    __tablename__ = "company_info"

    symbol = Column(String(20), primary_key=True, index=True)
    organ_name = Column(String(500), nullable=True)
    icb_name = Column(String(200), nullable=True)
    exchange = Column(String(20), nullable=True)
