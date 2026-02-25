CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_upsert
ON prices (symbol, ts);

CREATE UNIQUE INDEX IF NOT EXISTS ux_news_symbol_title_published
ON news (symbol, title, published_at);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time
ON prices (symbol, ts DESC);