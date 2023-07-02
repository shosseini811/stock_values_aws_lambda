import yfinance as yf
import psycopg2
from datetime import datetime

def lambda_handler(event, context):
    # Define the ticker symbol for Apple
    ticker_symbol = 'AAPL'

    # Get the data of the stock
    apple_stock = yf.Ticker(ticker_symbol)

    # AWS RDS PostgreSQL details
    host = "your_host"
    port = "5432"
    user = "your_username"
    password = "your_password"  # Replace this with your actual password
    database = "stocks"

    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Create table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id SERIAL PRIMARY KEY,
                price NUMERIC NOT NULL,
                time TIMESTAMP NOT NULL
            );
        """)
        conn.commit()

        # Get the historical prices for Apple stock
        historical_prices = apple_stock.history(period='1d', interval='1m')

        # Get the latest price and time
        latest_price = historical_prices['Close'].iloc[-1]
        latest_time = historical_prices.index[-1]

        # Insert the latest price and time into the table
        cursor.execute(
            "INSERT INTO stock_prices (price, time) VALUES (%s, %s)",
            (latest_price, latest_time)
        )

        # Commit the transaction
        conn.commit()

        # Retrieve and print the last row inserted into the table
        cursor.execute("SELECT * FROM stock_prices ORDER BY id DESC LIMIT 1;")
        result = cursor.fetchone()
        print(f"Last inserted row: {result}")

    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: Unexpected error:", error)
    finally:
        if conn is not None:
            conn.close()

# This is for local testing, you can remove it before deploying to AWS Lambda
if __name__ == "__main__":
    lambda_handler(None, None)
