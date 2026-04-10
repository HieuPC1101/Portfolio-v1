"""Repository functions for news_articles table."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.news import NewsArticle as NewsArticleModel


def upsert_articles(db: Session, articles: list[dict]) -> int:
    if not articles:
        return 0

    rows = [article for article in articles if article.get("url")]
    if not rows:
        return 0

    stmt = pg_insert(NewsArticleModel).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["url"],
        set_={
            "title": stmt.excluded.title,
            "summary": stmt.excluded.summary,
            "source": stmt.excluded.source,
            "published_at": stmt.excluded.published_at,
            "category": stmt.excluded.category,
            "symbols": stmt.excluded.symbols,
            "fetched_at": func.now(),
        },
    )
    try:
        result = db.execute(stmt)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return max(result.rowcount or 0, 0)


def get_latest(db: Session, limit: int, category: Optional[str] = None) -> list[dict]:
    query = db.query(NewsArticleModel)
    if category:
        query = query.filter(func.lower(NewsArticleModel.category) == category.lower())

    rows = (
        query.order_by(
            NewsArticleModel.published_at.desc().nullslast(), NewsArticleModel.id.desc()
        )
        .limit(limit)
        .all()
    )
    return [_to_news_article_dict(row) for row in rows]


def get_by_symbol(db: Session, symbol: str, limit: int) -> list[dict]:
    symbol_upper = symbol.upper()

    rows = (
        db.query(NewsArticleModel)
        .filter(
            or_(
                NewsArticleModel.symbols.any(symbol_upper),
                func.upper(NewsArticleModel.title).contains(symbol_upper),
                func.upper(NewsArticleModel.summary).contains(symbol_upper),
            )
        )
        .order_by(
            NewsArticleModel.published_at.desc().nullslast(), NewsArticleModel.id.desc()
        )
        .limit(limit)
        .all()
    )
    return [_to_news_article_dict(row) for row in rows]


def get_newest_fetched_at(db: Session) -> Optional[datetime]:
    return db.query(func.max(NewsArticleModel.fetched_at)).scalar()


def _to_news_article_dict(article: NewsArticleModel) -> dict:
    return {
        "title": article.title,
        "summary": article.summary,
        "url": article.url,
        "source": article.source,
        "published_at": article.published_at,
        "category": article.category,
        "symbols": article.symbols or [],
    }
