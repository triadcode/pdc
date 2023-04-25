import faust
from datetime import datetime
from psycopg2 import sql, extensions, connect
from shared import settings
from shared.stock_variation import StockVariation
import atexit
import sys


insert_query = sql.SQL("INSERT INTO aggregated_stock (event_time, symbol, min, max, avg, count) VALUES (%s, %s, %s, %s, %s, %s)")

def close_db_connection():
    cur.close()
    conn.close()


# Define the Faust application
app = faust.App('random_stock_app_consumer2', broker=settings.broker_address)

# Define the Kafka topic
topic = app.topic(settings.topic_name, value_type=StockVariation)


aggregated_values = app.Table('aggregated_values', default=int)

# Declare empty dictionary
key_stats = {}

def bucketize_timestamp(timestamp: datetime, bucket_size_seconds: int = 5) -> datetime:
    seconds_since_epoch = timestamp.timestamp()
    bucket_seconds = int(seconds_since_epoch // bucket_size_seconds) * bucket_size_seconds
    return datetime.fromtimestamp(bucket_seconds)


@app.agent(topic)
async def consume_messages(variations):
    async for variation in variations:

        #print(f'Variation received at {variation.timestamp} for {variation.symbol} with price {variation.price}')

        bucket_time = bucketize_timestamp(datetime.strptime(variation.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"))

        if variation.symbol not in key_stats:
            key_stats[variation.symbol] = {}

        if bucket_time not in key_stats[variation.symbol]:
            key_stats[variation.symbol][bucket_time] = {
                'values': [],
            }

        key_stats[variation.symbol][bucket_time]['values'].append(variation.price)


@app.timer(interval=10.0)
async def process_buckets():
    for symbol, buckets in key_stats.items():
        for bucket_time, data in buckets.items():
            values = data['values']
            total = sum(values)
            count = len(values)
            avg = total / count if count > 0 else 0
            min_value = min(values) if count > 0 else None
            max_value = max(values) if count > 0 else None

            print(f"Time: {bucket_time}, Symbol: {symbol}, Min Price: {min_value}, Max Price: {max_value}, Avg Price: {avg}, Count: {count} ")
            try:
                cur.execute(insert_query, (bucket_time, symbol,  min_value, max_value, avg, count))
                conn.commit()
            except Exception as err:
                print (f"An exception has occured while writing on the database {err = }")
                close_db_connection()
                sys.exit(1)



    # Clear the key_stats for the next cycle
    key_stats.clear()


# Run the Faust application
if __name__ == '__main__':
    try:
        # Connect to the PostgreSQL server and initialize session
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

    atexit.register(close_db_connection)
    app.main()
    # worker = faust.Worker(app)
    # worker.execute_from_commandline()
