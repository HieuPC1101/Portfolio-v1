from app.services.portfolio_notification_service import (
    build_watchlist_notification_content,
    should_emit_portfolio_notification,
)


def test_build_watchlist_notification_for_added_symbol():
    title, message, payload = build_watchlist_notification_content(
        symbol=" fpt ", action="added"
    )

    assert title == "Đã thêm FPT vào danh sách theo dõi"
    assert message == "Mã FPT vừa được thêm vào danh sách theo dõi của bạn."
    assert payload == {"symbol": "FPT", "action": "watchlist_add"}


def test_build_watchlist_notification_for_removed_symbol():
    title, message, payload = build_watchlist_notification_content(
        symbol="vnm", action="removed"
    )

    assert title == "Đã xóa VNM khỏi danh sách theo dõi"
    assert message == "Mã VNM vừa được xóa khỏi danh sách theo dõi của bạn."
    assert payload == {"symbol": "VNM", "action": "watchlist_remove"}


def test_emit_portfolio_notification_when_settings_enabled():
    settings = {
        "notifications_enabled": True,
        "notification_portfolio_enabled": True,
    }

    assert should_emit_portfolio_notification(settings) is True


def test_do_not_emit_portfolio_notification_when_master_disabled():
    settings = {
        "notifications_enabled": False,
        "notification_portfolio_enabled": True,
    }

    assert should_emit_portfolio_notification(settings) is False


def test_do_not_emit_portfolio_notification_when_portfolio_disabled():
    settings = {
        "notifications_enabled": True,
        "notification_portfolio_enabled": False,
    }

    assert should_emit_portfolio_notification(settings) is False
