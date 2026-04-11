"""Background scheduler for evaluating notification rules."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models.news import NewsArticle
from app.models.notification import Notification, NotificationRule
from app.models.portfolio import Watchlist, WatchlistStock
from app.models.user import UserSettings
from app.services.market_service import MarketService
from app.services.notification_rule_engine import NotificationRuleEngine
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


@dataclass
class PriceSnapshot:
    current_price: Optional[float]
    previous_price: Optional[float]
    daily_change_percent: Optional[float]


class NotificationScheduler:
    """Runs a full evaluation cycle for active notification rules."""

    def __init__(
        self,
        *,
        market_service: Optional[MarketService] = None,
        notification_service: Optional[NotificationService] = None,
        rule_engine: Optional[NotificationRuleEngine] = None,
        interval_seconds: Optional[int] = None,
        news_lookback_hours: Optional[int] = None,
    ):
        self.market_service = market_service or MarketService()
        self.notification_service = notification_service or NotificationService()
        self.rule_engine = rule_engine or NotificationRuleEngine()
        self.interval_seconds = (
            interval_seconds or settings.notification_scheduler_interval_seconds
        )
        self.news_lookback_hours = (
            news_lookback_hours or settings.notification_news_lookback_hours
        )

    @staticmethod
    def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
        if symbol is None:
            return None
        cleaned = symbol.strip().upper()
        return cleaned or None

    @staticmethod
    def _to_float(value: Optional[Decimal | float]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def build_price_notification_payload(
        *,
        rule: NotificationRule,
        current_price: Optional[float],
        daily_change_percent: Optional[float],
    ) -> tuple[str, str, dict]:
        symbol = (rule.symbol or "").upper()
        threshold = NotificationScheduler._to_float(rule.threshold_value)
        current_label = (
            f"{current_price:.0f}" if current_price is not None else "không xác định"
        )
        change_label = (
            f"{daily_change_percent:+.2f}%"
            if daily_change_percent is not None
            else "không xác định"
        )

        if rule.rule_type == "price_cross":
            threshold_label = f"{threshold:.0f}" if threshold is not None else "?"
            title = f"{symbol} vượt ngưỡng {threshold_label}"
            message = f"Giá hiện tại {current_label} ({change_label})."
            payload = {
                "symbol": symbol,
                "current_price": current_price,
                "threshold": threshold,
                "direction": (rule.operator or "").lower() or None,
                "rule_type": "price_cross",
            }
            return title, message, payload

        threshold_label = f"{threshold:+.2f}%" if threshold is not None else "?"
        title = f"{symbol} biến động {change_label}"
        message = (
            f"Mức biến động hiện tại {change_label}, đã chạm ngưỡng {threshold_label}."
        )
        payload = {
            "symbol": symbol,
            "current_price": current_price,
            "daily_change_percent": daily_change_percent,
            "threshold": threshold,
            "operator": (rule.operator or "").lower() or None,
            "rule_type": "price_change_percent",
        }
        return title, message, payload

    @staticmethod
    def build_news_notification_payload(
        *,
        symbol: str,
        article: NewsArticle,
    ) -> tuple[str, str, dict]:
        title = f"Có tin mới liên quan {symbol}"
        message = article.title
        payload = {
            "symbol": symbol,
            "article_url": article.url,
            "article_title": article.title,
            "source": article.source,
            "published_at": article.published_at.isoformat()
            if article.published_at
            else None,
        }
        return title, message, payload

    def _fetch_price_snapshot(self, symbol: str) -> Optional[PriceSnapshot]:
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=10)

        price_data = self.market_service.get_stock_price(
            symbol,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        if not price_data:
            return None

        ordered = sorted(price_data.items(), key=lambda item: item[0])
        values = [float(v) for _, v in ordered if v is not None]
        if not values:
            return None

        current_price = values[-1]
        previous_price = values[-2] if len(values) >= 2 else None

        daily_change_percent = None
        if previous_price is not None and previous_price != 0:
            daily_change_percent = (
                (current_price - previous_price) / previous_price
            ) * 100

        return PriceSnapshot(
            current_price=current_price,
            previous_price=previous_price,
            daily_change_percent=daily_change_percent,
        )

    def _is_rule_allowed_by_settings(
        self,
        *,
        rule: NotificationRule,
        user_settings: Optional[UserSettings],
    ) -> bool:
        if user_settings is None:
            return True
        if not bool(user_settings.notifications_enabled):
            return False

        if rule.rule_type in {"price_cross", "price_change_percent"}:
            return bool(user_settings.notification_price_enabled)
        if rule.rule_type == "news_watchlist":
            return bool(user_settings.notification_news_enabled)
        return True

    @staticmethod
    def _has_duplicate_notification(
        db: Session,
        *,
        user_id: int,
        type: str,
        title: str,
        message: str,
        since: datetime,
    ) -> bool:
        existing = (
            db.query(Notification.id)
            .filter(
                Notification.user_id == user_id,
                Notification.type == type,
                Notification.title == title,
                Notification.message == message,
                Notification.created_at >= since,
            )
            .first()
        )
        return existing is not None

    def _process_price_rules(
        self,
        db: Session,
        *,
        rules: list[NotificationRule],
        settings_map: dict[int, UserSettings],
    ) -> int:
        now = datetime.utcnow()
        price_rules = [
            rule
            for rule in rules
            if rule.rule_type in {"price_cross", "price_change_percent"}
            and self._normalize_symbol(rule.symbol)
        ]
        if not price_rules:
            return 0

        symbol_map: dict[str, PriceSnapshot] = {}
        for symbol in sorted(
            {
                self._normalize_symbol(rule.symbol)
                for rule in price_rules
                if self._normalize_symbol(rule.symbol)
            }
        ):
            snapshot = self._fetch_price_snapshot(symbol)
            if snapshot:
                symbol_map[symbol] = snapshot

        created_count = 0
        for rule in price_rules:
            user_settings = settings_map.get(rule.user_id)
            if not self._is_rule_allowed_by_settings(
                rule=rule, user_settings=user_settings
            ):
                continue
            if self.rule_engine.should_skip_by_cooldown(rule, now=now):
                continue

            symbol = self._normalize_symbol(rule.symbol)
            if symbol is None:
                continue

            snapshot = symbol_map.get(symbol)
            if snapshot is None:
                continue

            if rule.rule_type == "price_cross":
                triggered = self.rule_engine.evaluate_price_cross_rule(
                    operator=rule.operator,
                    threshold_value=rule.threshold_value,
                    current_price=snapshot.current_price,
                    previous_price=snapshot.previous_price,
                )
            else:
                triggered = self.rule_engine.evaluate_price_change_percent_rule(
                    operator=rule.operator,
                    threshold_value=rule.threshold_value,
                    daily_change_percent=snapshot.daily_change_percent,
                )

            if not triggered:
                continue

            title, message, payload = self.build_price_notification_payload(
                rule=rule,
                current_price=snapshot.current_price,
                daily_change_percent=snapshot.daily_change_percent,
            )

            since = now - timedelta(minutes=max(int(rule.cooldown_minutes or 0), 1))
            if not self._has_duplicate_notification(
                db,
                user_id=rule.user_id,
                type="price_alert",
                title=title,
                message=message,
                since=since,
            ):
                self.notification_service.create_notification(
                    db,
                    user_id=rule.user_id,
                    type="price_alert",
                    title=title,
                    message=message,
                    payload=payload,
                )
                created_count += 1

            rule.last_triggered_at = now
            db.commit()

        return created_count

    def _fetch_watchlist_symbols(
        self, db: Session, user_ids: list[int]
    ) -> dict[int, set[str]]:
        if not user_ids:
            return {}

        rows = (
            db.query(Watchlist.user_id, WatchlistStock.symbol)
            .join(WatchlistStock, Watchlist.id == WatchlistStock.watchlist_id)
            .filter(Watchlist.user_id.in_(user_ids))
            .all()
        )

        symbol_map: dict[int, set[str]] = {}
        for user_id, symbol in rows:
            normalized = self._normalize_symbol(symbol)
            if normalized is None:
                continue
            symbol_map.setdefault(int(user_id), set()).add(normalized)
        return symbol_map

    def _fetch_recent_news(
        self,
        db: Session,
        *,
        symbols: list[str],
        since: datetime,
        limit: int = 100,
    ) -> list[NewsArticle]:
        if not symbols:
            return []

        return (
            db.query(NewsArticle)
            .filter(
                and_(
                    or_(
                        NewsArticle.published_at >= since,
                        NewsArticle.fetched_at >= since,
                    ),
                    NewsArticle.symbols.overlap(symbols),
                )
            )
            .order_by(
                NewsArticle.published_at.desc().nullslast(), NewsArticle.id.desc()
            )
            .limit(limit)
            .all()
        )

    def _process_news_rules(
        self,
        db: Session,
        *,
        rules: list[NotificationRule],
        settings_map: dict[int, UserSettings],
    ) -> int:
        now = datetime.utcnow()
        lookback_since = now - timedelta(hours=max(int(self.news_lookback_hours), 1))
        news_rules = [rule for rule in rules if rule.rule_type == "news_watchlist"]
        if not news_rules:
            return 0

        user_ids = sorted({rule.user_id for rule in news_rules})
        watchlist_map = self._fetch_watchlist_symbols(db, user_ids)
        created_count = 0

        for rule in news_rules:
            user_settings = settings_map.get(rule.user_id)
            if not self._is_rule_allowed_by_settings(
                rule=rule, user_settings=user_settings
            ):
                continue
            if self.rule_engine.should_skip_by_cooldown(rule, now=now):
                continue

            symbols = sorted(watchlist_map.get(rule.user_id, set()))
            if rule.symbol:
                target_symbol = self._normalize_symbol(rule.symbol)
                symbols = [symbol for symbol in symbols if symbol == target_symbol]
            if not symbols:
                continue

            recent_articles = self._fetch_recent_news(
                db,
                symbols=symbols,
                since=lookback_since,
            )
            if not recent_articles:
                continue

            created_for_rule = 0
            for article in recent_articles:
                article_symbols = {
                    self._normalize_symbol(symbol)
                    for symbol in (article.symbols or [])
                    if self._normalize_symbol(symbol)
                }
                matched = sorted(article_symbols.intersection(symbols))
                if not matched:
                    continue

                primary_symbol = matched[0]
                title, message, payload = self.build_news_notification_payload(
                    symbol=primary_symbol,
                    article=article,
                )
                if self._has_duplicate_notification(
                    db,
                    user_id=rule.user_id,
                    type="news_alert",
                    title=title,
                    message=message,
                    since=lookback_since,
                ):
                    continue

                self.notification_service.create_notification(
                    db,
                    user_id=rule.user_id,
                    type="news_alert",
                    title=title,
                    message=message,
                    payload=payload,
                )
                created_count += 1
                created_for_rule += 1

            if created_for_rule > 0:
                rule.last_triggered_at = now
                db.commit()

        return created_count

    def run_cycle(self) -> int:
        """Run a single scheduler cycle and return number of created notifications."""
        db = SessionLocal()
        try:
            rules = (
                db.query(NotificationRule)
                .filter(NotificationRule.is_active.is_(True))
                .order_by(NotificationRule.created_at.asc())
                .all()
            )
            if not rules:
                return 0

            user_ids = sorted({rule.user_id for rule in rules})
            settings_rows = (
                db.query(UserSettings).filter(UserSettings.user_id.in_(user_ids)).all()
            )
            settings_map = {row.user_id: row for row in settings_rows}

            created = 0
            created += self._process_price_rules(
                db, rules=rules, settings_map=settings_map
            )
            created += self._process_news_rules(
                db, rules=rules, settings_map=settings_map
            )

            if created > 0:
                logger.info("Notification scheduler created %s notifications", created)
            return created
        except Exception:
            db.rollback()
            logger.exception("Notification scheduler cycle failed")
            return 0
        finally:
            db.close()


class NotificationSchedulerRunner:
    """Manages scheduler lifecycle in FastAPI app startup/shutdown."""

    def __init__(self, scheduler: Optional[NotificationScheduler] = None):
        self.scheduler = scheduler or NotificationScheduler()
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def _run_loop(self) -> None:
        logger.info(
            "Notification scheduler started (interval=%ss)",
            self.scheduler.interval_seconds,
        )
        while not self._stop_event.is_set():
            self.scheduler.run_cycle()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(int(self.scheduler.interval_seconds), 1),
                )
            except asyncio.TimeoutError:
                continue
        logger.info("Notification scheduler stopped")

    def start(self) -> None:
        if not settings.notification_scheduler_enabled:
            logger.info("Notification scheduler disabled by configuration")
            return
        if self._task and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if not self._task:
            return

        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None
