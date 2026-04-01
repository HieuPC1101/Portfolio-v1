"""
User model for authentication and profile management.
"""

from sqlalchemy import Boolean, Column, Integer, String, DateTime, Numeric, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.database import Base


class User(Base):
    """User account model."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    default_investment = Column(Numeric(15, 2), default=1000000)
    preferred_market = Column(String(20), default="HOSE")
    risk_tolerance = Column(
        String(20), default="moderate"
    )  # conservative, moderate, aggressive
    notifications_enabled = Column(Boolean, default=True)
    theme = Column(String(20), default="light")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    user = relationship("User", back_populates="settings")

    def __repr__(self):
        return f"<UserSettings(user_id={self.user_id}, risk_tolerance='{self.risk_tolerance}')>"
