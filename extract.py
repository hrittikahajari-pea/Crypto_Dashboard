import logging
from datetime import datetime, timezone
import requests

from db_connect import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

URL = "https://api.coingecko.com/api/v3/coins/markets"
REQUEST_TIMEOUT = 10

INSERT_QUERY = """
INSERT INTO crypto_prices (
    coin_name,
    price,
    market_cap,
    volume,
    market_timestamp,
    ingested_at
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (coin_name, market_timestamp) DO NOTHING;
"""

def fetch_data():
    try:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 10,
            "page": 1,
            "sparkline": False
        }

        response = requests.get(URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        return response.json()

    except Exception as e:
        logger.error("Fetch failed: %s", e)
        return None


def parse_timestamp(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except:
        return None


def insert_data(data):
    try:
        conn = get_connection()
        cur = conn.cursor()

        ingested_at = datetime.now(timezone.utc)

        for coin in data:
            market_timestamp = parse_timestamp(coin.get("last_updated"))

            if not market_timestamp:
                continue

            cur.execute(
                INSERT_QUERY,
                (
                    coin.get("name"),
                    coin.get("current_price"),
                    coin.get("market_cap"),
                    coin.get("total_volume"),
                    market_timestamp,
                    ingested_at
                )
            )

        conn.commit()
        logger.info("Inserted successfully")

    except Exception as e:
        logger.error("Insert failed: %s", e)
        conn.rollback()

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def run_etl():
    data = fetch_data()
    if data:
        insert_data(data)


if __name__ == "__main__":
    run_etl()