from sklearn.linear_model import LinearRegression
import numpy as np
from db_connect import get_connection


def generate_forecast(coin_name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT market_timestamp, price
            FROM crypto_prices
            WHERE LOWER(coin_name) = LOWER(%s)
            ORDER BY market_timestamp ASC;
            """,
            (coin_name,),
        )

        rows = cur.fetchall()

        if len(rows) < 5:
            return None

        prices = np.array([float(row[1]) for row in rows])
        x = np.arange(len(prices)).reshape(-1, 1)
        y = prices

        model = LinearRegression()
        model.fit(x, y)

        next_index = np.array([[len(prices)]])
        predicted_price = float(model.predict(next_index)[0])

        current_price = float(prices[-1])

        change_percent = (
            (predicted_price - current_price) / current_price
        ) * 100

        if change_percent > 0.5:
            trend = "Bullish"
        elif change_percent < -0.5:
            trend = "Bearish"
        else:
            trend = "Neutral"

        confidence = float(model.score(x, y)) * 100

        return {
            "coin_name": coin_name,
            "current_price": current_price,
            "predicted_price": predicted_price,
            "predicted_change_percent": change_percent,
            "predicted_trend": trend,
            "confidence_score": confidence,
        }

    finally:
        cur.close()
        conn.close()