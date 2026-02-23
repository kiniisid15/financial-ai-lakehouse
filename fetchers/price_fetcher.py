from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import yfinance as yf


def fetch_prices(
    tickers: list[str],
    period: str = "5d",
    interval: str = "1m",
) -> list[dict[str, Any]]:
    """Fetch OHLCV rows for one or more tickers via yfinance."""
    if not tickers:
        return []

    rows: list[dict[str, Any]] = []

    def _to_price(value: Any) -> float | None:
        if pd.isna(value):
            return None
        return round(float(value), 4)

    def _to_volume(value: Any) -> int | None:
        if pd.isna(value):
            return None
        return int(value)

    def _to_timestamp(value: Any) -> datetime:
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return pd.to_datetime(value).to_pydatetime()

    for ticker in tickers:
        df = yf.download(
            tickers=ticker,
            period=period,
            interval=interval,
            auto_adjust=False,
            group_by="ticker",
            progress=False,
            threads=True,
        )

        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(1)

        df.columns = [col.lower() for col in df.columns]
        df = df.dropna(subset=["open"])

        for dt, row in df.iterrows():
            rows.append(
                {
                    "timestamp": _to_timestamp(dt),
                    "open": _to_price(row["open"]),
                    "high": _to_price(row["high"]),
                    "low": _to_price(row["low"]),
                    "close": _to_price(row["close"]),
                    "volume": _to_volume(row["volume"]),
                }
            )

    return rows


if __name__ == "__main__":
    sample_tickers = ["AAPL"]
    ohlcv_rows = fetch_prices(sample_tickers, period="1mo", interval="1d")

    print(f"Fetched {len(ohlcv_rows)} OHLCV rows for {sample_tickers}")
    print("First 3 rows:")
    for item in ohlcv_rows[:3]:
        print(item)
