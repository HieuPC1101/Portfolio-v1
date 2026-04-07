"""News fetching and normalization service."""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

import feedparser
import numbers
import requests
from bs4 import BeautifulSoup

from app.services.news_crawler import fetch_all_sources, fetch_stock_news_from_sources

# ---------------------------------------------------------------------------
# In-memory cache (2-layer: short-term API cache)
# ---------------------------------------------------------------------------
_news_cache: Dict[str, Tuple[List[dict], datetime]] = {}
_NEWS_TTL_SECONDS = 300  # 5 minutes


def _cache_key(kind: str, identifier: str = "", category: str = "") -> str:
    return f"{kind}:{identifier}:{category}"


def _get_cached(key: str) -> Optional[List[dict]]:
    entry = _news_cache.get(key)
    if entry:
        data, ts = entry
        if datetime.utcnow() - ts < timedelta(seconds=_NEWS_TTL_SECONDS):
            return data
    return None


def _set_cached(key: str, data: List[dict]) -> None:
    _news_cache[key] = (data, datetime.utcnow())


def _crawler_to_news_article(art: dict) -> dict:
    """Map internal crawler dict → NewsArticle schema fields."""
    return {
        "title": art.get("title") or "",
        "summary": art.get("summary"),
        "url": art.get("url"),
        "source": art.get("source"),
        "published_at": art.get("published_at"),
        "category": art.get("category"),
        "symbols": art.get("symbols") or [],
    }

VN_STOCK_KEYWORDS = [
    "chứng khoán",
    "thị trường việt nam",
    "thị trường chứng khoán",
    "vn-index",
    "vnindex",
    "vn30",
    "vni",
    "hose",
    "hnx",
    "upcom",
    "vietstock",
    "doanh nghiệp niêm yết",
    "cổ phiếu",
    "ssi",
    "vcb",
    "vic",
    "vnm",
]

EXCLUDED_TOPIC_KEYWORDS = [
    "crypto",
    "bitcoin",
    "ethereum",
    "blockchain",
    "forex",
    "fed",
    "nasdaq",
    "dow jones",
    "s&p",
    "us market",
    "wall street",
    "goldman sachs",
    "chứng khoán mỹ",
    "trái phiếu mỹ",
    "tiền ảo",
    "tiền điện tử",
]

VIETSTOCK_RSS_FEEDS = [
    "https://vietstock.vn/830/chung-khoan/co-phieu.rss",
    "https://vietstock.vn/739/chung-khoan/giao-dich-noi-bo.rss",
    "https://vietstock.vn/741/chung-khoan/niem-yet.rss",
]

VNECONOMY_ARTICLE_SLUG = re.compile(r"^/[\w\-/]+-e\d+\.htm$")
POSITIVE_NEWS_KEYWORDS = ["tăng", "hồi phục", "lãi", "tang", "hoi phuc", "lai"]
NEGATIVE_NEWS_KEYWORDS = ["giảm", "bán tháo", "lỗ", "giam", "ban thao", "lo"]


def convert_relative_date(relative_date: str) -> datetime:
    """Convert relative timestamps into datetime."""
    try:
        if "minute" in relative_date:
            minutes = int(relative_date.split()[0])
            return datetime.now() - timedelta(minutes=minutes)
        if "hour" in relative_date:
            hours = int(relative_date.split()[0])
            return datetime.now() - timedelta(hours=hours)
        if "day" in relative_date:
            days = int(relative_date.split()[0])
            return datetime.now() - timedelta(days=days)
        return datetime.now()
    except Exception:
        return datetime.now()


def parse_cafebiz_datetime(raw_text: str) -> datetime:
    """Parse CafeBiz timestamp strings like '07:57 23/11/2025'."""
    if not raw_text:
        return datetime.now()

    cleaned = raw_text.replace("|", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    patterns = [
        "%H:%M %d/%m/%Y",
        "%H:%M %d-%m-%Y",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%I:%M %p %d/%m/%Y",
        "%d/%m/%Y %I:%M %p",
    ]

    for pattern in patterns:
        try:
            return datetime.strptime(cleaned, pattern)
        except ValueError:
            continue

    return datetime.now()


def is_vietnam_stock_article(title: str, content: str) -> bool:
    """Check if article matches Vietnam stock market context."""
    combined_text = f"{title or ''} {content or ''}".lower()
    if any(excluded in combined_text for excluded in EXCLUDED_TOPIC_KEYWORDS):
        return False
    return any(keyword in combined_text for keyword in VN_STOCK_KEYWORDS)


def format_display_date(date_value):
    """Format date into DD/MM/YYYY - HH:MM string."""
    try:
        if isinstance(date_value, datetime):
            dt = date_value
        elif isinstance(date_value, numbers.Number):
            timestamp = float(date_value)
            if timestamp > 1e12:
                timestamp /= 1000
            dt = datetime.fromtimestamp(timestamp)
        elif isinstance(date_value, time.struct_time):
            dt = datetime.fromtimestamp(time.mktime(date_value))
        elif isinstance(date_value, str):
            stripped_value = date_value.strip()
            if stripped_value.isdigit():
                timestamp = float(stripped_value)
                if timestamp > 1e12:
                    timestamp /= 1000
                dt = datetime.fromtimestamp(timestamp)
            else:
                dt = parsedate_to_datetime(stripped_value)
        else:
            dt = datetime.now()

        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)

        return dt.strftime("%d/%m/%Y - %H:%M")
    except Exception:
        if isinstance(date_value, str) and date_value:
            return date_value
        return datetime.now().strftime("%d/%m/%Y - %H:%M")


def get_news_sentiment_styles(title: str, content: str) -> Dict[str, str]:
    """Determine sentiment styling based on keyword scan."""
    text = f"{title or ''} {content or ''}".lower()
    sentiment = "neutral"

    if any(keyword in text for keyword in POSITIVE_NEWS_KEYWORDS):
        sentiment = "positive"
    elif any(keyword in text for keyword in NEGATIVE_NEWS_KEYWORDS):
        sentiment = "negative"

    styles = {
        "positive": {
            "border": "#22c55e",
            "background": "linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)",
            "label": "Tin tích cực",
        },
        "negative": {
            "border": "#ef4444",
            "background": "linear-gradient(135deg, #fee2e2 0%, #fecaca 100%)",
            "label": "Tin tiêu cực",
        },
        "neutral": {
            "border": "#d97706",
            "background": "linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)",
            "label": "Tin trung lập",
        },
    }
    return styles[sentiment]


class NewsService:
    """Service for fetching and normalizing market news."""

    def fetch_news(self, source: str = "vnexpress", max_articles: int = 50) -> Tuple[List[dict], Optional[str], Optional[str]]:
        """Fetch news by source with fallback handling."""
        if source == "vnEconomy":
            items = self._scrape_vneconomy_news(max_articles)
            if items:
                return items, None, None
            return [], "⚠️ vnEconomy không có bài phù hợp lúc này", None

        if source == "cafebiz":
            items = self._scrape_cafebiz_news(max_articles)
            if items:
                return items, None, None
            return [], "⚠️ CafeBiz không có bài phù hợp lúc này", None

        rss_urls = {
            "vnexpress": "https://vnexpress.net/rss/kinh-doanh.rss",
            "cafef": "https://cafef.vn/thi-truong-chung-khoan.rss",
            "vietstock": VIETSTOCK_RSS_FEEDS,
        }
        if source not in rss_urls:
            return [], None, f"Nguồn tin không hợp lệ: {source}"

        urls = rss_urls[source]
        if not isinstance(urls, list):
            urls = [urls]

        aggregated_news: List[dict] = []
        last_warning: Optional[str] = None

        for url in urls:
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/rss+xml, application/xml, text/xml, */*",
                    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
                response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
                response.raise_for_status()
                feed = feedparser.parse(response.content)

                if not feed.entries:
                    last_warning = f"⚠️ Không tìm thấy bài viết từ {source}"
                    continue

                for entry in feed.entries:
                    if len(aggregated_news) >= max_articles:
                        break

                    try:
                        title = entry.title if hasattr(entry, "title") else "No Title"
                        link = entry.link if hasattr(entry, "link") else ""

                        published_struct = getattr(entry, "published_parsed", None)
                        updated_struct = getattr(entry, "updated_parsed", None)
                        if published_struct:
                            date = format_display_date(published_struct)
                        elif updated_struct:
                            date = format_display_date(updated_struct)
                        elif hasattr(entry, "published"):
                            date = format_display_date(entry.published)
                        elif hasattr(entry, "updated"):
                            date = format_display_date(entry.updated)
                        else:
                            date = format_display_date(datetime.now())

                        if hasattr(entry, "summary"):
                            content = BeautifulSoup(entry.summary, "html.parser").get_text(strip=True)
                        elif hasattr(entry, "description"):
                            content = BeautifulSoup(entry.description, "html.parser").get_text(strip=True)
                        else:
                            content = "Nội dung đang được cập nhật..."

                        normalized_content = content[:500] + "..." if len(content) > 500 else content
                        if not is_vietnam_stock_article(title, normalized_content):
                            continue

                        aggregated_news.append(
                            {
                                "title": title,
                                "date": date,
                                "content": normalized_content,
                                "link": link,
                                "source": source.upper(),
                            }
                        )
                    except Exception:
                        continue

                if len(aggregated_news) >= max_articles:
                    break

            except requests.exceptions.HTTPError as exc:
                last_warning = f"⚠️ Không thể tải RSS từ {source}: HTTP {exc.response.status_code}"
            except requests.exceptions.Timeout:
                last_warning = f"⚠️ Timeout khi tải RSS từ {source}"
            except requests.exceptions.ConnectionError:
                last_warning = f"⚠️ Lỗi kết nối đến {source}"
            except Exception as exc:
                last_warning = f"⚠️ Không thể tải RSS từ {source}: {str(exc)[:80]}"

        if aggregated_news:
            return aggregated_news[:max_articles], None, None

        return [], last_warning or f"⚠️ Không thể tải RSS từ {source}", None

    def _scrape_vneconomy_news(self, max_articles: int = 5) -> List[dict]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }

        base_section = "https://vneconomy.vn/chung-khoan.htm"
        max_section_pages = 5
        urls_to_try: List[str] = []
        for page in range(1, max_section_pages + 1):
            if page == 1:
                urls_to_try.append(base_section)
            else:
                urls_to_try.append(f"{base_section}?p={page}")
        urls_to_try.extend(["https://vneconomy.vn/kinh-te.htm", "https://vneconomy.vn"])

        collected_news: List[dict] = []
        seen_links = set()

        for base_url in urls_to_try:
            try:
                response = requests.get(base_url, headers=headers, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")

                article_selectors = ["div.story", "div.story-item", "article.story", "div.news-item", "div.item-news"]
                articles = []
                for selector in article_selectors:
                    articles = soup.select(selector)
                    if articles:
                        break

                if not articles:
                    articles = soup.find_all("a", href=True)
                    articles = [a for a in articles if "/tin-tuc/" in a.get("href", "") or "/kinh-te/" in a.get("href", "")][: max_articles * 2]

                page_news: List[dict] = []
                for article in articles[: max_articles * 3]:
                    if len(collected_news) >= max_articles:
                        break
                    try:
                        title_elem = article.find("h3") or article.find("h2") or article.find("a")
                        if not title_elem:
                            continue
                        title = title_elem.get_text(strip=True)
                        if not title or len(title) < 10:
                            continue

                        link_elem = article.find("a") if article.name != "a" else article
                        link = link_elem.get("href", "") if link_elem else ""
                        if link and not link.startswith("http"):
                            link = f"https://vneconomy.vn{link}"

                        time_elem = article.find("time") or article.find("span", class_=["time", "date", "published"])
                        raw_date = time_elem.get_text(strip=True) if time_elem else datetime.now()
                        date = format_display_date(raw_date)

                        desc_elem = article.find("p") or article.find("div", class_=["description", "desc", "summary"])
                        content = desc_elem.get_text(strip=True) if desc_elem else "Đọc thêm tại vneconomy.vn"
                        if len(content) < 20:
                            content = f"{title[:100]}... Đọc thêm tại vneconomy.vn"

                        normalized_content = content[:500] + "..." if len(content) > 500 else content
                        passes_filter = is_vietnam_stock_article(title, normalized_content)
                        lower_text = f"{title} {normalized_content}".lower()
                        if not passes_filter:
                            if (
                                (link.startswith("https://vneconomy.vn/chung-khoan") or link.startswith("/chung-khoan") or "chung-khoan" in base_url.lower())
                                and not any(excluded in lower_text for excluded in EXCLUDED_TOPIC_KEYWORDS)
                            ):
                                passes_filter = True
                        if not passes_filter:
                            continue

                        unique_key = link or title
                        if unique_key in seen_links:
                            continue
                        seen_links.add(unique_key)

                        page_news.append(
                            {
                                "title": title,
                                "date": date,
                                "content": normalized_content,
                                "link": link,
                                "source": "VNECONOMY",
                            }
                        )
                    except Exception:
                        continue

                if len(collected_news) + len(page_news) < max_articles:
                    for anchor in soup.find_all("a", href=True):
                        if len(collected_news) + len(page_news) >= max_articles:
                            break
                        raw_href = anchor.get("href", "")
                        if not raw_href or raw_href.startswith("javascript") or raw_href.startswith("#"):
                            continue
                        if not VNECONOMY_ARTICLE_SLUG.match(raw_href):
                            continue
                        anchor_title = anchor.get_text(strip=True)
                        if not anchor_title or len(anchor_title) < 10:
                            continue
                        link = raw_href if raw_href.startswith("http") else f"https://vneconomy.vn{raw_href}"
                        if link in seen_links:
                            continue

                        placeholder_content = f"Tin nhanh VnEconomy: {anchor_title}. Đọc nội dung chi tiết trên trang gốc."
                        passes_filter = is_vietnam_stock_article(anchor_title, placeholder_content)
                        if not passes_filter:
                            lower_text = anchor_title.lower()
                            if (
                                (link.startswith("https://vneconomy.vn/chung-khoan") or raw_href.startswith("/chung-khoan") or "chung-khoan" in base_url.lower())
                                and not any(excluded in lower_text for excluded in EXCLUDED_TOPIC_KEYWORDS)
                            ):
                                passes_filter = True
                        if not passes_filter:
                            continue

                        seen_links.add(link)
                        page_news.append(
                            {
                                "title": anchor_title,
                                "date": format_display_date(datetime.now()),
                                "content": placeholder_content,
                                "link": link,
                                "source": "VNECONOMY",
                            }
                        )

                if page_news:
                    collected_news.extend(page_news)
                    if len(collected_news) >= max_articles:
                        return collected_news[:max_articles]

            except Exception:
                continue

        return collected_news[:max_articles]

    def _scrape_cafebiz_news(self, max_articles: int = 5) -> List[dict]:
        section_url = "https://cafebiz.vn/cau-chuyen-kinh-doanh/chung-khoan.chn"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }

        def fetch_article_body(url: str) -> str:
            try:
                detail_response = requests.get(url, headers=headers, timeout=15)
                detail_response.raise_for_status()
                detail_soup = BeautifulSoup(detail_response.content, "html.parser")
                content_div = detail_soup.find("div", class_="newscontent")
                if content_div:
                    return content_div.get_text(separator=" ", strip=True)
            except Exception:
                return ""
            return ""

        try:
            response = requests.get(section_url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        article_boxes = soup.select("div.cfbiznews_box")
        if not article_boxes:
            return []

        collected_news: List[dict] = []
        seen_links = set()

        for box in article_boxes:
            if len(collected_news) >= max_articles:
                break

            title_elem = box.select_one(".cfbiznews_title")
            if not title_elem:
                continue
            title = title_elem.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            link = title_elem.get("href") or ""
            if not link:
                continue
            if link.startswith("//"):
                link = f"https:{link}"
            elif link.startswith("/"):
                link = f"https://cafebiz.vn{link}"
            if link in seen_links:
                continue
            seen_links.add(link)

            timestamp_elem = box.select_one(".cfbiznews_tt")
            raw_timestamp = timestamp_elem.get_text(strip=True) if timestamp_elem else ""
            published_dt = parse_cafebiz_datetime(raw_timestamp)
            display_date = format_display_date(published_dt)

            summary_elem = box.select_one(".cfbiznews_des") or box.select_one(".cfbiznews_sapo")
            summary_text = summary_elem.get_text(strip=True) if summary_elem else ""

            article_text = summary_text
            if len(article_text) < 120:
                detailed = fetch_article_body(link)
                article_text = detailed or summary_text or "Đọc thêm trên CafeBiz.vn"

            normalized_content = article_text[:500] + "..." if len(article_text) > 500 else article_text
            if not is_vietnam_stock_article(title, normalized_content):
                continue

            collected_news.append(
                {
                    "title": title,
                    "date": display_date,
                    "content": normalized_content,
                    "link": link,
                    "source": "CAFEBIZ",
                }
            )

        return collected_news[:max_articles]


    def get_latest_news(
        self,
        limit: int = 20,
        category: Optional[str] = None,
        refresh: bool = False,
    ) -> List[dict]:
        """Fetch latest VN stock news, with optional cache bypass."""
        key = _cache_key("latest", category=category or "")
        if not refresh:
            cached = _get_cached(key)
            if cached is not None:
                return cached[:limit]

        articles = fetch_all_sources(limit=limit * 3)
        if category:
            cat_lower = category.lower()
            articles = [a for a in articles if (a.get("category") or "").lower() == cat_lower]

        result = [_crawler_to_news_article(a) for a in articles[:limit]]
        _set_cached(key, result)
        return result

    def get_stock_news(
        self,
        symbol: str,
        limit: int = 10,
        refresh: bool = False,
    ) -> List[dict]:
        """Fetch news related to a specific stock symbol, with optional cache bypass."""
        symbol_upper = symbol.upper()
        key = _cache_key("stock", identifier=symbol_upper)
        if not refresh:
            cached = _get_cached(key)
            if cached is not None:
                return cached[:limit]

        articles = fetch_stock_news_from_sources(symbol=symbol_upper, limit=limit)
        result = [_crawler_to_news_article(a) for a in articles]
        _set_cached(key, result)
        return result


_news_service: Optional[NewsService] = None


def get_news_service() -> NewsService:
    """Return singleton news service."""
    global _news_service
    if _news_service is None:
        _news_service = NewsService()
    return _news_service
