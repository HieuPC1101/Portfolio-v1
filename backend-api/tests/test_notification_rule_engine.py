from datetime import datetime, timedelta
from types import SimpleNamespace

from app.services.notification_rule_engine import NotificationRuleEngine


def test_price_cross_up_requires_crossing_threshold():
    triggered = NotificationRuleEngine.evaluate_price_cross_rule(
        operator="cross_up",
        threshold_value=100,
        previous_price=99,
        current_price=101,
    )

    assert triggered is True


def test_price_change_percent_uses_operator_and_threshold():
    assert (
        NotificationRuleEngine.evaluate_price_change_percent_rule(
            operator="lte",
            threshold_value=-3,
            daily_change_percent=-3.5,
        )
        is True
    )
    assert (
        NotificationRuleEngine.evaluate_price_change_percent_rule(
            operator="gte",
            threshold_value=5,
            daily_change_percent=2,
        )
        is False
    )


def test_should_skip_by_cooldown_when_recently_triggered():
    now = datetime.utcnow()
    rule = SimpleNamespace(
        last_triggered_at=now - timedelta(minutes=5),
        cooldown_minutes=30,
    )

    skipped = NotificationRuleEngine.should_skip_by_cooldown(rule, now=now)
    assert skipped is True
