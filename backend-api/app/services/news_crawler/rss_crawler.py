"""Multi-source RSS crawler cho tin tức tài chính Việt Nam."""

from __future__ import annotations

import html
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    "Cache-Control": "no-cache",
}

RSS_SOURCES: dict[str, list[str]] = {
    "CafeF": [
        "https://cafef.vn/thi-truong-chung-khoan.rss",
        "https://cafef.vn/doanh-nghiep.rss",
        "https://cafef.vn/tai-chinh-ngan-hang.rss",
    ],
    "VietStock": [
        "https://vietstock.vn/830/chung-khoan/co-phieu.rss",
        "https://vietstock.vn/739/chung-khoan/giao-dich-noi-bo.rss",
        "https://vietstock.vn/741/chung-khoan/niem-yet.rss",
    ],
    "VnExpress": [
        "https://vnexpress.net/rss/kinh-doanh.rss",
    ],
    "TinNhanhChungKhoan": [
        "https://tinnhanhchungkhoan.vn/rss/chung-khoan.rss",
        "https://tinnhanhchungkhoan.vn/rss/tai-chinh-ngan-hang.rss",
    ],
    "VnEconomy": [
        "https://vneconomy.vn/chung-khoan.rss",
        "https://vneconomy.vn/tai-chinh.rss",
        "https://vneconomy.vn/thi-truong.rss",
    ],
    "BaoDauTu": [
        "https://baodautu.vn/chung-khoan-d6/rss",
        "https://baodautu.vn/dau-tu-tai-chinh-d6/rss",
        "https://baodautu.vn/kinh-doanh-d3/rss",
    ],
}

# Mã cổ phiếu: 2-5 chữ hoa
_SYMBOL_RE = re.compile(r"\b([A-Z]{2,5})\b")

# Whitelist mã phổ biến để giảm false positive
_KNOWN_SYMBOLS = {
    "VNM",
    "VCB",
    "HPG",
    "VIC",
    "SSI",
    "MWG",
    "FPT",
    "TCB",
    "VHM",
    "BID",
    "CTG",
    "ACB",
    "MBB",
    "VPB",
    "HDB",
    "STB",
    "SHB",
    "TPB",
    "VIB",
    "OCB",
    "MSN",
    "SAB",
    "GAS",
    "PLX",
    "POW",
    "PVD",
    "PVS",
    "REE",
    "GVR",
    "VRE",
    "NVL",
    "PDR",
    "DXG",
    "KDH",
    "DIG",
    "VND",
    "HCM",
    "VCI",
    "SHS",
    "BSI",
    "HAG",
    "HNG",
    "VGC",
    "BMP",
    "GMD",
    "DPM",
    "DCM",
    "FMC",
    "ANV",
    "VHC",
    "VNI",
    "VN30",
    "VNINDEX",
    "HNX",
    "UPCOM",
    "HOSE",
}

_VN_STOCK_KW = [
    "chứng khoán",
    "cổ phiếu",
    "vn-index",
    "vnindex",
    "vn30",
    "hose",
    "hnx",
    "upcom",
    "thị trường",
    "niêm yết",
]

_EXCLUDED_KW = [
    "bitcoin",
    "ethereum",
    "crypto",
    "tiền ảo",
    "tiền điện tử",
    "nasdaq",
    "dow jones",
    "s&p",
    "wall street",
    "chứng khoán mỹ",
]

_BROKEN_NUMERIC_ENTITY_RE = re.compile(r"(?<!&)#(x[\da-fA-F]+|\d+);")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(entry) -> Optional[datetime]:
    """Trích xuất datetime từ feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        ts = getattr(entry, attr, None)
        if ts:
            try:
                return datetime(*ts[:6], tzinfo=timezone.utc).replace(tzinfo=None)
            except Exception:
                pass
    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                parsed = parsedate_to_datetime(raw)
                if parsed.tzinfo is None:
                    return parsed
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                pass
    return None


def _decode_entities(value: str) -> str:
    if not value:
        return ""

    decoded = _BROKEN_NUMERIC_ENTITY_RE.sub(r"&#\1;", value)
    for _ in range(3):
        next_value = html.unescape(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    return decoded


def _is_vn_stock(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    if any(kw in text for kw in _EXCLUDED_KW):
        return False
    return any(kw in text for kw in _VN_STOCK_KW)


def _extract_symbols(title: str, summary: str) -> List[str]:
    text = f"{title} {summary}"
    found = _SYMBOL_RE.findall(text)
    return sorted({s for s in found if s in _KNOWN_SYMBOLS})


def _fetch_rss_source(source_name: str, urls: List[str]) -> List[dict]:
    """Fetch một nguồn RSS, trả về list article dict."""
    articles: List[dict] = []
    seen_urls: set[str] = set()

    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)

            for entry in feed.entries:
                link = getattr(entry, "link", "") or ""
                if link in seen_urls:
                    continue
                seen_urls.add(link)

                title = _decode_entities(getattr(entry, "title", "") or "")
                raw_summary = (
                    getattr(entry, "summary", "")
                    or getattr(entry, "description", "")
                    or ""
                )
                summary = BeautifulSoup(raw_summary, "html.parser").get_text(
                    " ", strip=True
                )
                summary = _decode_entities(summary)
                summary = summary[:500] + "..." if len(summary) > 500 else summary

                if not _is_vn_stock(title, summary):
                    continue

                published_at = _parse_date(entry)
                category = (
                    getattr(entry, "tags", [{}])[0].get("term")
                    if getattr(entry, "tags", None)
                    else None
                )

                articles.append(
                    {
                        "title": title,
                        "summary": summary,
                        "url": link,
                        "source": source_name,
                        "published_at": published_at,
                        "category": category,
                        "symbols": _extract_symbols(title, summary),
                    }
                )
        except Exception:
            continue

    return articles


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_all_sources(limit: int = 50) -> List[dict]:
    """
    Fetch từ tất cả RSS sources song song, deduplicate theo URL,
    sắp xếp giảm dần theo published_at.
    """
    all_articles: List[dict] = []
    seen_urls: set[str] = set()

    with ThreadPoolExecutor(max_workers=len(RSS_SOURCES)) as executor:
        futures = {
            executor.submit(_fetch_rss_source, name, urls): name
            for name, urls in RSS_SOURCES.items()
        }
        for future in as_completed(futures):
            try:
                articles = future.result()
                for art in articles:
                    url = art.get("url", "")
                    if url and url in seen_urls:
                        continue
                    if url:
                        seen_urls.add(url)
                    all_articles.append(art)
            except Exception:
                continue

    # Sắp xếp mới nhất trước
    all_articles.sort(
        key=lambda a: a.get("published_at") or datetime.min,
        reverse=True,
    )
    return all_articles[:limit]


def fetch_stock_news_from_sources(symbol: str, limit: int = 20) -> List[dict]:
    """
    Fetch RSS trực tiếp và lọc bài có nhắc đến symbol trong title/summary.
    Không dùng _is_vn_stock filter để tránh bỏ sót bài về mã cổ phiếu cụ thể.
    """
    symbol_upper = symbol.upper()
    articles: List[dict] = []
    seen_urls: set[str] = set()

    for source_name, urls in RSS_SOURCES.items():
        for url in urls:
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)

                for entry in feed.entries:
                    link = getattr(entry, "link", "") or ""
                    if not link:
                        continue
                    if link in seen_urls:
                        continue

                    title = _decode_entities(getattr(entry, "title", "") or "")
                    raw_summary = (
                        getattr(entry, "summary", "")
                        or getattr(entry, "description", "")
                        or ""
                    )
                    summary = BeautifulSoup(raw_summary, "html.parser").get_text(
                        " ", strip=True
                    )
                    summary = _decode_entities(summary)
                    summary = summary[:500] + "..." if len(summary) > 500 else summary

                    combined_lower = f"{title} {summary}".lower()
                    if any(kw in combined_lower for kw in _EXCLUDED_KW):
                        continue

                    title_up = title.upper()
                    summary_up = summary.upper()
                    extracted = _extract_symbols(title, summary)

                    if (
                        symbol_upper not in title_up
                        and symbol_upper not in summary_up
                        and symbol_upper not in extracted
                    ):
                        continue

                    seen_urls.add(link)
                    published_at = _parse_date(entry)
                    category = (
                        getattr(entry, "tags", [{}])[0].get("term")
                        if getattr(entry, "tags", None)
                        else None
                    )

                    articles.append(
                        {
                            "title": title,
                            "summary": summary,
                            "url": link,
                            "source": source_name,
                            "published_at": published_at,
                            "category": category,
                            "symbols": sorted(set(extracted) | {symbol_upper}),
                        }
                    )

            except Exception:
                continue

    articles.sort(
        key=lambda a: a.get("published_at") or datetime.min,
        reverse=True,
    )
    return articles[:limit]
