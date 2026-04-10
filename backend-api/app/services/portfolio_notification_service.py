"""Helpers for watchlist-related notifications."""

from __future__ import annotations

from typing import Any


def should_emit_portfolio_notification(settings: dict[str, Any]) -> bool:
    return bool(settings.get("notifications_enabled", True)) and bool(
        settings.get("notification_portfolio_enabled", True)
    )


def build_watchlist_notification_content(
    *, symbol: str, action: str
) -> tuple[str, str, dict[str, str]]:
    normalized_symbol = symbol.strip().upper()

    if action == "added":
        return (
            f"Đã thêm {normalized_symbol} vào danh sách theo dõi",
            f"Mã {normalized_symbol} vừa được thêm vào danh sách theo dõi của bạn.",
            {"symbol": normalized_symbol, "action": "watchlist_add"},
        )

    return (
        f"Đã xóa {normalized_symbol} khỏi danh sách theo dõi",
        f"Mã {normalized_symbol} vừa được xóa khỏi danh sách theo dõi của bạn.",
        {"symbol": normalized_symbol, "action": "watchlist_remove"},
    )
