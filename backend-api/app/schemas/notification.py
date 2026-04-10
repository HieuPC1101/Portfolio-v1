"""Schemas for notifications APIs."""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

NotificationType = Literal[
    "price_alert",
    "news_alert",
    "portfolio_alert",
    "system_alert",
]
NotificationStatus = Literal["unread", "read", "archived"]
NotificationRuleType = Literal["price_cross", "price_change_percent", "news_watchlist"]
NotificationOperator = Literal["gt", "lt", "gte", "lte", "cross_up", "cross_down"]


class NotificationResponse(BaseModel):
    id: int
    user_id: int
    type: NotificationType
    title: str
    message: str
    payload: Optional[Dict[str, Any]] = None
    status: NotificationStatus
    created_at: datetime
    read_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class NotificationListResponse(BaseModel):
    items: List[NotificationResponse] = Field(default_factory=list)
    total: int
    limit: int
    offset: int


class UnreadCountResponse(BaseModel):
    unread_count: int


class MarkAllReadResponse(BaseModel):
    updated: int


class NotificationRuleBase(BaseModel):
    rule_type: NotificationRuleType
    symbol: Optional[str] = None
    operator: Optional[NotificationOperator] = None
    threshold_value: Optional[float] = None
    is_active: bool = True
    cooldown_minutes: int = Field(default=60, ge=1, le=10080)


class NotificationRuleCreate(NotificationRuleBase):
    pass


class NotificationRuleUpdate(BaseModel):
    rule_type: Optional[NotificationRuleType] = None
    symbol: Optional[str] = None
    operator: Optional[NotificationOperator] = None
    threshold_value: Optional[float] = None
    is_active: Optional[bool] = None
    cooldown_minutes: Optional[int] = Field(default=None, ge=1, le=10080)


class NotificationRuleToggle(BaseModel):
    is_active: bool


class NotificationRuleResponse(BaseModel):
    id: int
    user_id: int
    rule_type: NotificationRuleType
    symbol: Optional[str] = None
    operator: Optional[str] = None
    threshold_value: Optional[float] = None
    is_active: bool
    cooldown_minutes: int
    last_triggered_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class NotificationSettingsResponse(BaseModel):
    notifications_enabled: bool = True
    notification_price_enabled: bool = True
    notification_news_enabled: bool = True
    notification_portfolio_enabled: bool = True


class NotificationSettingsUpdate(BaseModel):
    notifications_enabled: Optional[bool] = None
    notification_price_enabled: Optional[bool] = None
    notification_news_enabled: Optional[bool] = None
    notification_portfolio_enabled: Optional[bool] = None
