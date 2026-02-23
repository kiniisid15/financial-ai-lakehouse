from __future__ import annotations

import time
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree as ET

import requests
from requests import Response


def _request_news_feed(url: str, retries: int = 3, timeout: int = 20) -> Response | None:
    """Request Yahoo RSS feed with basic retry/backoff for transient errors."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)

            # Rate limiting/transient errors: retry with backoff.
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                return None

            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            return None

    return None


def fetch_news(ticker: str) -> list[dict[str, Any]]:
    """Fetch Yahoo Finance RSS news for a single ticker symbol."""
    symbol = ticker.strip().upper()
    if not symbol:
        return []

    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
    response = _request_news_feed(url)
    if response is None:
        return []

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return []

    items = root.findall(".//item")

    articles: list[dict[str, Any]] = []
    for item in items:
        title = item.findtext("title")
        url = item.findtext("link")
        pub_date = item.findtext("pubDate")
        description = item.findtext("description")
        source = item.findtext("source")

        published_at = None
        if pub_date:
            try:
                published_at = parsedate_to_datetime(pub_date.strip())
            except (TypeError, ValueError):
                published_at = None

        articles.append(
            {
                "ticker": symbol,
                "published_at": published_at,
                "title": title.strip() if title else None,
                "summary": description.strip() if description else None,
                "source": source.strip() if source else "Yahoo Finance",
                "url": url.strip() if url else None,
            }
        )

    return articles


if __name__ == "__main__":
    sample_ticker = "AAPL"
    news_items = fetch_news(sample_ticker)

    print(f"Fetched {len(news_items)} news items for {sample_ticker}")
    print("First 2 items:")
    for item in news_items[:2]:
        print(item)
