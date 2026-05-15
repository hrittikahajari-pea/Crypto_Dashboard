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
def get_ohlc_prices(
    coin_name: str,
    interval_minutes: int = Query(default=30, ge=1, le=1440),
    period: str = Query(default="24h"),
):
    period_map = {
        "24h": "24 hours",
        "7d": "7 days",
        "30d": "30 days",
    }

    if period not in period_map:
        raise HTTPException(
            status_code=400,
            detail="Invalid period. Supported values are: 24h, 7d, 30d.",
        )

    selected_period = period_map[period]
    interval_seconds = interval_minutes * 60

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            WITH latest_timestamp AS (
                SELECT MAX(market_timestamp) AS latest_market_timestamp
                FROM crypto_prices
                WHERE LOWER(coin_name) = LOWER(%s)
            ),
            filtered_prices AS (
                SELECT
                    cp.price,
                    cp.market_timestamp,
                    cp.ingested_at,
                    (
                        TIMESTAMP 'epoch'
                        + (
                            FLOOR(EXTRACT(EPOCH FROM cp.market_timestamp) / %s)::BIGINT
                            * %s
                        ) * INTERVAL '1 second'
                    ) AS time_bucket
                FROM crypto_prices cp
                CROSS JOIN latest_timestamp lt
                WHERE LOWER(cp.coin_name) = LOWER(%s)
                  AND lt.latest_market_timestamp IS NOT NULL
                  AND cp.market_timestamp >= lt.latest_market_timestamp - (%s)::INTERVAL
                  AND cp.market_timestamp <= lt.latest_market_timestamp
            ),
            ohlc_window AS (
                SELECT
                    time_bucket,
                    FIRST_VALUE(price) OVER (
                        PARTITION BY time_bucket
                        ORDER BY market_timestamp ASC, ingested_at ASC
                    ) AS open_price,
                    MAX(price) OVER (
                        PARTITION BY time_bucket
                    ) AS high_price,
                    MIN(price) OVER (
                        PARTITION BY time_bucket
                    ) AS low_price,
                    FIRST_VALUE(price) OVER (
                        PARTITION BY time_bucket
                        ORDER BY market_timestamp DESC, ingested_at DESC
                    ) AS close_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY time_bucket
                        ORDER BY market_timestamp ASC, ingested_at ASC
                    ) AS row_number
                FROM filtered_prices
            )
            SELECT
                time_bucket,
                open_price,
                high_price,
                low_price,
                close_price
            FROM ohlc_window
            WHERE row_number = 1
            ORDER BY time_bucket ASC;
        """

        cur.execute(
            query,
            (
                coin_name,
                interval_seconds,
                interval_seconds,
                coin_name,
                selected_period,
            ),
        )

        rows = cur.fetchall()

        logger.info(
            "OHLC rows returned for %s, period=%s: %s",
            coin_name,
            period,
            len(rows),
        )

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