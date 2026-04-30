import logging
import requests
from db_connect import get_connection

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
REQUEST_TIMEOUT = 10


def fetch_top_cryptocurrencies():
    """Fetch top 10 cryptocurrencies by market cap from CoinGecko."""
    
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false",
    }

    try:
        response = requests.get(
            COINGECKO_MARKETS_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        crypto_data = response.json()

        logger.info("Successfully fetched top 10 cryptocurrencies from CoinGecko.")

        return crypto_data

    except requests.exceptions.Timeout:
        logger.error("Request timed out after %s seconds.", REQUEST_TIMEOUT)

    except requests.exceptions.HTTPError as error:
        logger.error("HTTP error occurred: %s", error)

    except requests.exceptions.RequestException as error:
        logger.error("Request failed: %s", error)

    except ValueError as error:
        logger.error("JSON parsing failed: %s", error)

    return None

def insert_crypto_data(data):
    try:

        conn = get_connection()
        cur = conn.cursor()

        for coin in data:
            cur.execute("""
            INSERT INTO crypto_prices (coin_name, price, market_cap, volume)
            VALUES (%s, %s, %s, %s)
        """, (
            coin['name'],
            coin['current_price'],
            coin['market_cap'],
            coin['total_volume']
        ))

        conn.commit()
        logger.info("Data inserted into database successfully")
    except Exception as e:
        logger.error("Data insertion failed: %s",e)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
# MAIN EXECUTION
def run_etl():
    data = fetch_top_cryptocurrencies()
    if data:
        insert_crypto_data(data)
        logger.info("ETL process successful")
    else:
        logger.error("No data fetched.")
if __name__ == "__main__":
    run_etl()