"""Notification and notification rule models."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    desc,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Notification(Base):
    """Per-user notification records for in-app notification center."""

    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    type = Column(String(50), nullable=False)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=True)
    status = Column(
        String(20),
        nullable=False,
        default="unread",
        server_default=text("'unread'"),
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    read_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="notifications")

    __table_args__ = (
        Index(
            "ix_notifications_user_status_created_at_desc",
            "user_id",
            "status",
            desc("created_at"),
        ),
        Index("ix_notifications_user_created_at_desc", "user_id", desc("created_at")),
    )


class NotificationRule(Base):
    """User-defined rules used to create notifications."""

    __tablename__ = "notification_rules"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    rule_type = Column(String(50), nullable=False)
    symbol = Column(String(20), nullable=True)
    operator = Column(String(20), nullable=True)
    threshold_value = Column(Numeric(18, 6), nullable=True)
    is_active = Column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    cooldown_minutes = Column(
        Integer, nullable=False, default=60, server_default=text("60")
    )
    last_triggered_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    user = relationship("User", back_populates="notification_rules")

    __table_args__ = (
        Index("ix_notification_rules_user_active", "user_id", "is_active"),
        Index("ix_notification_rules_user_type", "user_id", "rule_type"),
    )
