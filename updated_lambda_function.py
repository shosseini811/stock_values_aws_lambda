import yfinance as yf
import psycopg2
import boto3
from datetime import datetime, timedelta
import pytz

def lambda_handler(event, context):
    # Define the ticker symbol for Apple
    ticker_symbol = 'AAPL'

    # Get the data of the stock
    apple_stock = yf.Ticker(ticker_symbol)

    # AWS RDS PostgreSQL details
    host = "your_host"
    port = "5432"
    user = "your_user"
    password = "your_password"  # Replace this with your actual password
    database = "stocks"

    # AWS SNS Topic ARN
    sns_topic_arn = "your_arn"

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
            CREATE TABLE IF NOT EXISTS stock_prices1 (
                id SERIAL PRIMARY KEY,
                price NUMERIC NOT NULL,
                time TIMESTAMP NOT NULL,
                last_email_sent_date TIMESTAMP
            );
        """)
        conn.commit()

        # Get the historical prices for Apple stock
        historical_prices = apple_stock.history(period='1d', interval='1m')

        # Get the latest price and time
        latest_price = historical_prices['Close'].iloc[-1]
        latest_time = historical_prices.index[-1]
        
        # Insert the latest price, time and last_email_sent_date into the table
        epoch_time = datetime.now(pytz.utc)
        cursor.execute(
            "INSERT INTO stock_prices1 (price, time, last_email_sent_date) VALUES (%s, %s, %s)",
            (latest_price, latest_time, epoch_time)
        )
        conn.commit()

        # Commit the transaction
        conn.commit()

        # Retrieve and print the last row inserted into the table
        cursor.execute("SELECT * FROM stock_prices1 ORDER BY id DESC LIMIT 1;")
        result = cursor.fetchone()
        print(f"Last inserted row: {result}")

        # Get the current time in UTC timezone
        current_time = datetime.now(pytz.utc)

        print(f"last_email_sent_date: {result[3]}")
        print(f"latest_price: {latest_price}")

        # Publish to SNS Topic if last_email_sent_date is more than an hour ago and price is less than 200
        if (result[3] is None or current_time - result[3].replace(tzinfo=pytz.utc) > timedelta(hours=1)) and latest_price < 200:
            # Send an SNS message
            sns_client = boto3.client('sns')
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=f'New stock data inserted: {result}',
                Subject='Stock Data Update'
            )
            ## Update the last_email_sent_date in the database
            cursor.execute(
                "UPDATE stock_prices1 SET last_email_sent_date = %s WHERE id = %s",
                (current_time, result[0])
            )
            conn.commit()
            print("SNS notification sent and last_email_sent_date updated.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': 'Stock data processed successfully!'
    }