import asyncio
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import models  # noqa: F401
from app.database import AsyncSessionLocal, engine
from fetchers.news_fetcher import fetch_news
from fetchers.price_fetcher import fetch_prices


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def load_tickers() -> list[str]:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    raw = os.getenv("TICKERS", "")
    return [ticker.strip().upper() for ticker in raw.split(",") if ticker.strip()]


async def wait_for_db() -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("db ready")
            return
        except Exception as exc:
            logger.warning("db not ready (attempt %s): %s", attempt, exc)
            await asyncio.sleep(2)


async def run_fetchers(
    tickers: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    price_task = asyncio.to_thread(fetch_prices, tickers)
    news_tasks = [asyncio.to_thread(fetch_news, ticker) for ticker in tickers]

    gathered = await asyncio.gather(price_task, *news_tasks, return_exceptions=True)

    price_rows: list[dict[str, Any]] = []
    news_rows: list[dict[str, Any]] = []

    prices_result = gathered[0]
    if isinstance(prices_result, Exception):
        logger.error("price fetch failed: %s", prices_result)
    else:
        price_rows = list(prices_result)

    for ticker, result in zip(tickers, gathered[1:]):
        if isinstance(result, Exception):
            logger.error("news fetch failed for %s: %s", ticker, result)
            continue
        news_rows.extend(result)

    if price_rows and not any(
        ("ticker" in row or "symbol" in row) for row in price_rows
    ):
        if len(tickers) == 1:
            for row in price_rows:
                row["ticker"] = tickers[0]
        else:
            logger.info(
                "rows missing ticker → refetching per ticker"
            )
            per_ticker = await asyncio.gather(
                *(asyncio.to_thread(fetch_prices, [t]) for t in tickers),
                return_exceptions=True,
            )

            rebuilt: list[dict[str, Any]] = []

            for ticker, rows_or_exc in zip(tickers, per_ticker):
                if isinstance(rows_or_exc, Exception):
                    logger.error(
                        "per-ticker price fetch failed for %s: %s",
                        ticker,
                        rows_or_exc,
                    )
                    continue

                for row in rows_or_exc:
                    item = dict(row)
                    item["ticker"] = ticker
                    rebuilt.append(item)

            price_rows = rebuilt

    logger.info(
        "rows fetched | prices=%s news=%s",
        len(price_rows),
        len(news_rows),
    )

    return price_rows, news_rows


async def _get_table_columns(
    session: AsyncSession,
    table_name: str,
) -> set[str]:

    result = await session.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    )

    return {row[0] for row in result.fetchall()}


async def upsert_prices(
    session: AsyncSession,
    rows: list[dict[str, Any]],
    tickers: list[str],
) -> int:

    if not rows:
        return 0

    cols = await _get_table_columns(session, "prices")

    symbol_col = "ticker" if "ticker" in cols else "symbol"
    ts_col = "timestamp" if "timestamp" in cols else "ts"
    has_embedding = "embedding" in cols

    payload: list[dict[str, Any]] = []

    for row in rows:
        symbol = row.get("ticker") or row.get("symbol")

        if symbol is None:
            if len(tickers) == 1:
                symbol = tickers[0]
            else:
                continue

        ts_value = row.get("timestamp") or row.get("ts")
        close_price = row.get("close")

        # ---------- DATA VALIDATION LAYER ----------
        if (
            ts_value is None
            or close_price is None
            or close_price <= 0
        ):
            continue
        # -------------------------------------------

        item = {
            symbol_col: symbol,
            ts_col: ts_value,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": close_price,
            "volume": row.get("volume"),
        }

        if has_embedding:
            item["embedding"] = None

        payload.append(item)

    if not payload:
        return 0

    col_list = [
        symbol_col,
        ts_col,
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    if has_embedding:
        col_list.append("embedding")

    insert_cols = ", ".join(col_list)
    value_params = ", ".join(f":{c}" for c in col_list)

    sql = text(
        f"""
        INSERT INTO prices ({insert_cols})
        VALUES ({value_params})
        ON CONFLICT ({symbol_col}, {ts_col})
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )

    await session.execute(sql, payload)

    logger.info("rows inserted | prices=%s", len(payload))
    return len(payload)
async def upsert_news(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> int:

    if not rows:
        return 0

    cols = await _get_table_columns(session, "news")

    symbol_col = "ticker" if "ticker" in cols else "symbol"
    body_col = "summary" if "summary" in cols else "content"
    has_url = "url" in cols
    has_embedding = "embedding" in cols

    payload: list[dict[str, Any]] = []

    for row in rows:
        symbol = row.get("ticker") or row.get("symbol")
        title = row.get("title")
        published_at = row.get("published_at")

        if symbol is None or title is None or published_at is None:
            continue

        item = {
            symbol_col: symbol,
            "title": title,
            body_col: row.get("summary") or row.get("content") or "",
            "published_at": published_at,
            "source": row.get("source"),
        }

        if has_url:
            url = row.get("url")
            if url is None:
                continue
            item["url"] = url

        if has_embedding:
            item["embedding"] = None

        payload.append(item)

    if not payload:
        return 0

    col_list = [symbol_col, "title", body_col, "published_at", "source"]

    if has_url:
        col_list.append("url")

    if has_embedding:
        col_list.append("embedding")

    insert_cols = ", ".join(col_list)
    value_params = ", ".join(f":{c}" for c in col_list)

    conflict_clause = f"ON CONFLICT ({symbol_col}, title, published_at)"


    sql = text(
    f"""
    INSERT INTO news ({insert_cols})
    VALUES ({value_params})
    {conflict_clause}
    DO UPDATE SET
        title = EXCLUDED.title,
        {body_col} = EXCLUDED.{body_col},
        published_at = EXCLUDED.published_at,
        source = EXCLUDED.source
    """
)

    await session.execute(sql, payload)

    logger.info("rows inserted | news=%s", len(payload))
    return len(payload)


async def ingest_cycle(tickers: list[str]) -> None:

    price_rows, news_rows = await run_fetchers(tickers)

    async with AsyncSessionLocal() as session:
        try:
            await upsert_prices(session, price_rows, tickers)
            await upsert_news(session, news_rows)
            await session.commit()
            logger.info("cycle complete")
        except Exception:
            await session.rollback()
            logger.exception("errors during ingestion")


async def main() -> None:
    logger.info("startup")

    tickers = load_tickers()

    if not tickers:
        logger.error("no tickers configured in .env")
        return

    await wait_for_db()

    INGEST_INTERVAL_SECONDS = 60

    while True:
        try:
            await ingest_cycle(tickers)
        except Exception:
            logger.exception("cycle failed")

        await asyncio.sleep(INGEST_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())