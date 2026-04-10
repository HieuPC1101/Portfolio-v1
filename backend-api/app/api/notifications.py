"""Notification API endpoints."""

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.schemas.notification import (
    MarkAllReadResponse,
    NotificationListResponse,
    NotificationResponse,
    NotificationRuleCreate,
    NotificationRuleResponse,
    NotificationRuleToggle,
    NotificationRuleUpdate,
    NotificationSettingsResponse,
    NotificationSettingsUpdate,
    UnreadCountResponse,
)
from app.services.notification_service import NotificationService

router = APIRouter(prefix="/notifications", tags=["Notifications"])

notification_service = NotificationService()


@router.get("", response_model=NotificationListResponse)
def list_notifications(
    status_filter: Optional[str] = Query(
        default=None,
        alias="status",
        description="Filter by status: unread | read | archived",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    items, total = notification_service.list_notifications(
        db,
        user_id=current_user.id,
        status=status_filter,
        limit=limit,
        offset=offset,
    )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/unread-count", response_model=UnreadCountResponse)
def get_unread_count(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    unread_count = notification_service.unread_count(db, user_id=current_user.id)
    return {"unread_count": unread_count}


@router.patch("/read-all", response_model=MarkAllReadResponse)
def mark_all_notifications_as_read(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    updated = notification_service.mark_all_as_read(db, user_id=current_user.id)
    return {"updated": updated}


@router.get("/rules", response_model=list[NotificationRuleResponse])
def list_notification_rules(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.list_rules(db, user_id=current_user.id)


@router.post(
    "/rules",
    response_model=NotificationRuleResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_notification_rule(
    data: NotificationRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.create_rule(db, user_id=current_user.id, data=data)


@router.put("/rules/{rule_id}", response_model=NotificationRuleResponse)
def update_notification_rule(
    rule_id: int,
    data: NotificationRuleUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.update_rule(
        db,
        user_id=current_user.id,
        rule_id=rule_id,
        data=data,
    )


@router.patch("/rules/{rule_id}/toggle", response_model=NotificationRuleResponse)
def toggle_notification_rule(
    rule_id: int,
    data: NotificationRuleToggle,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.toggle_rule(
        db,
        user_id=current_user.id,
        rule_id=rule_id,
        data=data,
    )


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_notification_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    notification_service.delete_rule(db, user_id=current_user.id, rule_id=rule_id)


@router.get("/settings", response_model=NotificationSettingsResponse)
def get_notification_settings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.get_settings(db, user_id=current_user.id)


@router.patch("/settings", response_model=NotificationSettingsResponse)
def update_notification_settings(
    data: NotificationSettingsUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.update_settings(db, user_id=current_user.id, data=data)


@router.patch("/{notification_id}/read", response_model=NotificationResponse)
def mark_notification_as_read(
    notification_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    return notification_service.mark_as_read(
        db,
        user_id=current_user.id,
        notification_id=notification_id,
    )
