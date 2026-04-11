from types import SimpleNamespace

from app.services.notification_scheduler import NotificationScheduler


def test_build_price_notification_payload_for_cross_rule():
    rule = SimpleNamespace(
        rule_type="price_cross",
        operator="cross_up",
        threshold_value=140000,
        symbol="FPT",
    )

    title, message, payload = NotificationScheduler.build_price_notification_payload(
        rule=rule,
        current_price=140500,
        daily_change_percent=2.1,
    )

    assert "FPT" in title
    assert "140000" in title
    assert "140500" in message
    assert payload["symbol"] == "FPT"
    assert payload["direction"] == "cross_up"


def test_build_price_notification_payload_for_percent_rule():
    rule = SimpleNamespace(
        rule_type="price_change_percent",
        operator="lte",
        threshold_value=-3,
        symbol="VNM",
    )

    title, message, payload = NotificationScheduler.build_price_notification_payload(
        rule=rule,
        current_price=62500,
        daily_change_percent=-3.8,
    )

    assert "VNM" in title
    assert "-3.8" in message
    assert payload["symbol"] == "VNM"
    assert payload["rule_type"] == "price_change_percent"
