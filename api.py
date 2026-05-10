import logging

from fastapi import FastAPI, HTTPException, Query

from db_connect import get_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Pea's Crypto Intelligence API hueh!",
    description="API for serving live and historical cryptocurrency data ",
    version="1.0.0",
)


PRICE_COLUMNS = (
    "id",
    "coin_name",
    "price",
    "market_cap",
    "volume",
    "market_timestamp",
    "ingested_at",
)


def serialize_value(value):
    """Convert database values into JSON-friendly values."""
    if hasattr(value, "isoformat"):
        return value.isoformat()

    return value


def row_to_dict(row):
    """Convert a crypto_prices row tuple into a dictionary."""
    return {
        column: serialize_value(value)
        for column, value in zip(PRICE_COLUMNS, row)
    }


def fetch_all(query, params=None):
    """Execute a parameterized SELECT query and return rows as dictionaries."""
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query, params or ())

        return [row_to_dict(row) for row in cur.fetchall()]

    except Exception as error:
        logger.exception("Database query failed: %s", error)
        raise HTTPException(
            status_code=500,
            detail="Database query failed. Please check server logs.",
        ) from error

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/prices/latest")
def get_latest_prices():
    query = """
    SELECT DISTINCT ON (coin_name)
        id,
        coin_name,
        price,
        market_cap,
        volume,
        market_timestamp,
        ingested_at
    FROM crypto_prices
    ORDER BY coin_name, market_timestamp DESC;
    """

    logger.info("Fetching latest crypto price records.")
    return fetch_all(query)


@app.get("/prices/history/{coin_name}")
def get_price_history(
    coin_name: str,
    limit: int = Query(default=100, ge=1, le=5000),
):
    query = """
    SELECT
        id,
        coin_name,
        price,
        market_cap,
        volume,
        market_timestamp,
        ingested_at
    FROM crypto_prices
    WHERE coin_name = %s
    ORDER BY market_timestamp ASC
    LIMIT %s;
    """

    logger.info("Fetching historical price records for %s.", coin_name)
    return fetch_all(query, (coin_name, limit))