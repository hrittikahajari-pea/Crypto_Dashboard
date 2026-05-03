import logging
from db_connect import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

CREATE_CRYPTO_PRICES_TABLE = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_name TEXT NOT NULL,
    price DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    market_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_crypto_price_snapshot UNIQUE (coin_name, market_timestamp)
);
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_crypto_prices_market_timestamp
ON crypto_prices (market_timestamp);
"""

def create_crypto_prices_table():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(CREATE_CRYPTO_PRICES_TABLE)
        cur.execute(CREATE_INDEX)

        conn.commit()
        logger.info("Table ready")

    except Exception as e:
        logger.error("Error: %s", e)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_crypto_prices_table()