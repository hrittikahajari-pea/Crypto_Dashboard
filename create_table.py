import psycopg2

conn = psycopg2.connect(
    dbname="crypto_db",
    user="postgres",
    password="hrih",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_name TEXT,
    price FLOAT,
    market_cap FLOAT,
    volume FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
cur.close()
conn.close()

print("Table created successfully!")