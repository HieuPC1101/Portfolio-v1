"""Rule evaluation helpers for notification triggers."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from app.models.notification import NotificationRule


class NotificationRuleEngine:
    """Evaluates notification rules against market/news inputs."""

    @staticmethod
    def _to_float(value: Optional[Decimal | float]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    @classmethod
    def should_skip_by_cooldown(
        cls,
        rule: NotificationRule,
        now: Optional[datetime] = None,
    ) -> bool:
        if rule.last_triggered_at is None:
            return False

        check_time = now or datetime.utcnow()
        cooldown = timedelta(minutes=max(int(rule.cooldown_minutes or 0), 0))
        return (check_time - rule.last_triggered_at) < cooldown

    @classmethod
    def evaluate_price_cross_rule(
        cls,
        *,
        operator: Optional[str],
        threshold_value: Optional[Decimal | float],
        current_price: Optional[float],
        previous_price: Optional[float],
    ) -> bool:
        if operator is None or current_price is None:
            return False

        threshold = cls._to_float(threshold_value)
        if threshold is None:
            return False

        op = operator.lower()
        if op == "gt":
            return current_price > threshold
        if op == "lt":
            return current_price < threshold
        if op == "gte":
            return current_price >= threshold
        if op == "lte":
            return current_price <= threshold

        if previous_price is None:
            return False
        if op == "cross_up":
            return previous_price <= threshold < current_price
        if op == "cross_down":
            return previous_price >= threshold > current_price
        return False

    @classmethod
    def evaluate_price_change_percent_rule(
        cls,
        *,
        operator: Optional[str],
        threshold_value: Optional[Decimal | float],
        daily_change_percent: Optional[float],
    ) -> bool:
        if operator is None or daily_change_percent is None:
            return False

        threshold = cls._to_float(threshold_value)
        if threshold is None:
            return False

        op = operator.lower()
        if op == "gt":
            return daily_change_percent > threshold
        if op == "lt":
            return daily_change_percent < threshold
        if op == "gte":
            return daily_change_percent >= threshold
        if op == "lte":
            return daily_change_percent <= threshold
        return False
