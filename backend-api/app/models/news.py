"""SQLAlchemy model for persisted news articles."""

from sqlalchemy import Column, DateTime, Index, Integer, String, Text, desc, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func

from app.database import Base


class NewsArticle(Base):
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, nullable=False, index=True)
    title = Column(Text, nullable=False)
    summary = Column(Text)
    source = Column(String(50), index=True)
    published_at = Column(DateTime, index=True)
    category = Column(String(100))
    symbols = Column(
        ARRAY(String),
        nullable=False,
        default=list,
        server_default=text("'{}'::character varying[]"),
    )
    fetched_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_news_published_at_desc", desc("published_at")),
        Index("ix_news_symbols", symbols, postgresql_using="gin"),
    )
