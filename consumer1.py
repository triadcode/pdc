import faust
from datetime import datetime, timezone
from psycopg2 import sql, extensions, connect
from shared import settings
from shared.stock_variation import StockVariation
import atexit
import sys

insert_query = sql.SQL("INSERT INTO stock (event_time, symbol, price) VALUES (%s, %s, %s)")


def close_db_connection():
    cur.close()
    conn.close()


# Define the Faust application
app = faust.App('random_stock_app_consumer', broker=settings.broker_address)

# Define the Kafka topic
topic = app.topic(settings.topic_name, value_type=StockVariation)

# Consume messages
@app.agent(topic)
async def process_message(variations):
    async for variation in variations:
        #dt = datetime.fromisoformat(variation.timestamp.strip()) #datetime.strptime(variation.timestamp, "%Y-%m-%d %H:%M:%S%z").replace(tzinfo=settings.timezone)
        #dt = dateutil.parser.parse(variation.timestamp)
        print(f'Variation received at {variation.timestamp} for {variation.symbol} with price {variation.price}')
        cur.execute(insert_query, (variation.timestamp, variation.symbol,  variation.price))
        conn.commit()


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
