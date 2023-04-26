from psycopg2 import sql, extensions, connect
from shared import settings
import sys
import csv
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description="Process the time window for the query.")
parser.add_argument("time_window", choices=["daily", "hourly","agg_hourly","last10min"], help="Time window can be either 'daily' or 'hourly'.")
args = parser.parse_args()
time_window = args.time_window


def close_db_connection():
    cur.close()
    conn.close()


try:
    conn = connect(
        dbname='postgres',
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Create a new cursor
    cur = conn.cursor()
    cur.execute(f"SET TIME ZONE '{settings.timezone}';")
except Exception as err:
    print (f"An exception has occured during session initialization: {err =}")
    close_db_connection()
    sys.exit(1)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

if time_window == "daily":
    filename = f'{timestamp}_daily.csv'
    query = """
    WITH daily_prices AS (
        SELECT
            symbol,
            date_trunc('day', event_time) AS day,
            price,
            FIRST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('day', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_price_of_day,
            LAST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('day', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price_of_day
        FROM
            stock
    )
    SELECT
        symbol,
        day,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,
        MAX(first_price_of_day) AS first_price_of_day,
        MAX(last_price_of_day) AS last_price_of_day,
        MAX(((last_price_of_day - first_price_of_day)/first_price_of_day) * 100) as difference

    FROM
        daily_prices
    GROUP BY
        symbol,
        day
    ORDER BY
        symbol,
        day;
    """
elif time_window == "hourly":
    filename = f'{timestamp}_hourly.csv'
    query = """
    WITH hourly_prices AS (
        SELECT
            symbol,
            date_trunc('hour', event_time) AS day,
            price,
            FIRST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('hour', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_price_of_hour,
            LAST_VALUE(price) OVER (PARTITION BY symbol, date_trunc('hour', event_time) ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price_of_hour
        FROM
            stock
    )
    SELECT
        symbol,
        day,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,
        MAX(first_price_of_hour) AS first_price_of_hour,
        MAX(last_price_of_hour) AS last_price_of_hour,
        MAX(((last_price_of_hour - first_price_of_hour)/first_price_of_hour) * 100) as difference

    FROM
        hourly_prices
    GROUP BY
        symbol,
        day
    ORDER BY
        symbol,
        day;
    """
elif time_window == "agg_hourly":
    filename = f'{timestamp}_aggregated_hourly.csv'
    query = """
    SELECT
        symbol,
        date_trunc('hour', event_time) AS hour,
        MIN(min) AS min_price,
        MAX(max) AS max_price,
        AVG(avg) AS avg_price
    FROM
        aggregated_stock
    GROUP BY
        symbol, hour
    ORDER BY
        symbol, hour;
    """
else:
    filename = f'{timestamp}_last_10_minutes.csv'
    query = """
    SELECT
     symbol,
     MIN(price) AS min_price,
     MAX(price) AS max_price,
     AVG(price) AS avg_price
    FROM
        stock
    WHERE
       event_time >= (CURRENT_TIMESTAMP - INTERVAL '10 minutes')
    GROUP BY symbol;
    """

cur.execute(query)
result = cur.fetchall()

column_names = [desc[0] for desc in cur.description]
with open(filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(column_names)
    csv_writer.writerows(result)

print(f"File {filename} generated.")
close_db_connection()