import psycopg2

try:
    conn = psycopg2.connect(
        dbname="crypto_db",
        user="postgres",
        password="hrih",
        host="localhost",
        port="5432"
    )

    print("Connected to database successfully!")

    conn.close()

except Exception as e:
    print("Error:", e)