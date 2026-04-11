"""Business logic for notifications and notification settings."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.notification import Notification, NotificationRule
from app.models.user import UserSettings
from app.schemas.notification import (
    NotificationRuleCreate,
    NotificationRuleToggle,
    NotificationRuleUpdate,
    NotificationSettingsUpdate,
)

VALID_NOTIFICATION_STATUSES = {"unread", "read", "archived"}


class NotificationService:
    """Application service for notification-related use cases."""

    @staticmethod
    def _ensure_settings(db: Session, user_id: int) -> UserSettings:
        settings = (
            db.query(UserSettings).filter(UserSettings.user_id == user_id).first()
        )
        if settings:
            return settings

        settings = UserSettings(
            user_id=user_id,
            notifications_enabled=True,
            notification_price_enabled=True,
            notification_news_enabled=True,
            notification_portfolio_enabled=True,
        )
        db.add(settings)
        db.commit()
        db.refresh(settings)
        return settings

    def list_notifications(
        self,
        db: Session,
        *,
        user_id: int,
        status: Optional[str],
        limit: int,
        offset: int,
    ) -> Tuple[list[Notification], int]:
        query = db.query(Notification).filter(Notification.user_id == user_id)
        if status:
            normalized = status.strip().lower()
            if normalized not in VALID_NOTIFICATION_STATUSES:
                raise HTTPException(
                    status_code=400, detail="Invalid notification status"
                )
            query = query.filter(Notification.status == normalized)

        total = query.count()
        items = (
            query.order_by(Notification.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return items, total

    def unread_count(self, db: Session, *, user_id: int) -> int:
        return (
            db.query(Notification)
            .filter(Notification.user_id == user_id, Notification.status == "unread")
            .count()
        )

    def mark_as_read(
        self, db: Session, *, user_id: int, notification_id: int
    ) -> Notification:
        notification = (
            db.query(Notification)
            .filter(Notification.id == notification_id, Notification.user_id == user_id)
            .first()
        )
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")

        if notification.status != "read":
            notification.status = "read"
            notification.read_at = datetime.utcnow()
            db.commit()
            db.refresh(notification)
        return notification

    def mark_all_as_read(self, db: Session, *, user_id: int) -> int:
        now = datetime.utcnow()
        result = (
            db.query(Notification)
            .filter(Notification.user_id == user_id, Notification.status == "unread")
            .update(
                {Notification.status: "read", Notification.read_at: now},
                synchronize_session=False,
            )
        )
        db.commit()
        return int(result or 0)

    def create_notification(
        self,
        db: Session,
        *,
        user_id: int,
        type: str,
        title: str,
        message: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Notification:
        notification = Notification(
            user_id=user_id,
            type=type,
            title=title,
            message=message,
            payload=payload,
            status="unread",
        )
        db.add(notification)
        db.commit()
        db.refresh(notification)
        return notification

    def list_rules(self, db: Session, *, user_id: int) -> list[NotificationRule]:
        return (
            db.query(NotificationRule)
            .filter(NotificationRule.user_id == user_id)
            .order_by(NotificationRule.created_at.desc())
            .all()
        )

    def create_rule(
        self,
        db: Session,
        *,
        user_id: int,
        data: NotificationRuleCreate,
    ) -> NotificationRule:
        rule = NotificationRule(
            user_id=user_id,
            rule_type=data.rule_type,
            symbol=data.symbol.upper().strip() if data.symbol else None,
            operator=data.operator,
            threshold_value=data.threshold_value,
            is_active=data.is_active,
            cooldown_minutes=data.cooldown_minutes,
        )
        db.add(rule)
        db.commit()
        db.refresh(rule)
        return rule

    def update_rule(
        self,
        db: Session,
        *,
        user_id: int,
        rule_id: int,
        data: NotificationRuleUpdate,
    ) -> NotificationRule:
        rule = (
            db.query(NotificationRule)
            .filter(NotificationRule.id == rule_id, NotificationRule.user_id == user_id)
            .first()
        )
        if not rule:
            raise HTTPException(status_code=404, detail="Notification rule not found")

        for field, value in data.model_dump(exclude_unset=True).items():
            if field == "symbol" and isinstance(value, str):
                setattr(rule, field, value.upper().strip())
            else:
                setattr(rule, field, value)

        db.commit()
        db.refresh(rule)
        return rule

    def toggle_rule(
        self,
        db: Session,
        *,
        user_id: int,
        rule_id: int,
        data: NotificationRuleToggle,
    ) -> NotificationRule:
        rule = (
            db.query(NotificationRule)
            .filter(NotificationRule.id == rule_id, NotificationRule.user_id == user_id)
            .first()
        )
        if not rule:
            raise HTTPException(status_code=404, detail="Notification rule not found")

        rule.is_active = data.is_active
        db.commit()
        db.refresh(rule)
        return rule

    def delete_rule(self, db: Session, *, user_id: int, rule_id: int) -> None:
        rule = (
            db.query(NotificationRule)
            .filter(NotificationRule.id == rule_id, NotificationRule.user_id == user_id)
            .first()
        )
        if not rule:
            raise HTTPException(status_code=404, detail="Notification rule not found")
        db.delete(rule)
        db.commit()

    def get_settings(self, db: Session, *, user_id: int) -> Dict[str, bool]:
        settings = self._ensure_settings(db, user_id)
        return {
            "notifications_enabled": bool(settings.notifications_enabled),
            "notification_price_enabled": bool(settings.notification_price_enabled),
            "notification_news_enabled": bool(settings.notification_news_enabled),
            "notification_portfolio_enabled": bool(
                settings.notification_portfolio_enabled
            ),
        }

    def update_settings(
        self,
        db: Session,
        *,
        user_id: int,
        data: NotificationSettingsUpdate,
    ) -> Dict[str, bool]:
        settings = self._ensure_settings(db, user_id)

        for field, value in data.model_dump(exclude_unset=True).items():
            setattr(settings, field, value)

        db.commit()
        db.refresh(settings)
        return {
            "notifications_enabled": bool(settings.notifications_enabled),
            "notification_price_enabled": bool(settings.notification_price_enabled),
            "notification_news_enabled": bool(settings.notification_news_enabled),
            "notification_portfolio_enabled": bool(
                settings.notification_portfolio_enabled
            ),
        }
