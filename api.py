import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Query

from db_connect import get_connection
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Welcome to Pea's Crypto Intelligence API hueh!",
    description="API for serving live and historical cryptocurrency data ",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
    ORDER BY coin_name, market_timestamp DESC NULLS LAST, ingested_at DESC;
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
    ORDER BY market_timestamp ASC NULLS LAST, ingested_at ASC
    LIMIT %s;
    """

    logger.info("Fetching historical price records for %s.", coin_name)
    return fetch_all(query, (coin_name, limit))
@app.get("/prices/ohlc/{coin_name}")
def get_ohlc_data(
    coin_name: str,
    interval_minutes: int = Query(default=30, ge=5, le=1440),
):
    query = """
    WITH price_windows AS (
        SELECT
            coin_name,
            price,
            market_timestamp,
            FLOOR(EXTRACT(EPOCH FROM market_timestamp) / (%s * 60)) AS window_id
        FROM crypto_prices
        WHERE coin_name = %s
          AND market_timestamp IS NOT NULL
    ),
    ranked_prices AS (
        SELECT
            *,
            FIRST_VALUE(price) OVER (
                PARTITION BY window_id
                ORDER BY market_timestamp ASC
            ) AS open_price,
            FIRST_VALUE(price) OVER (
                PARTITION BY window_id
                ORDER BY market_timestamp DESC
            ) AS close_price
        FROM price_windows
    )
    SELECT
        MIN(market_timestamp) AS candle_time,
        MAX(open_price) AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        MAX(close_price) AS close
    FROM ranked_prices
    GROUP BY window_id
    ORDER BY candle_time ASC;
    """

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query, (interval_minutes, coin_name))
        rows = cur.fetchall()

        return [
            {
                "time": row[0].isoformat() if row[0] else None,
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
            }
            for row in rows
        ]

    except Exception as error:
        logger.exception("OHLC query failed: %s", error)
        raise HTTPException(
            status_code=500,
            detail="OHLC query failed. Please check server logs.",
        ) from error

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()