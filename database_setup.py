from psycopg2 import sql, extensions, connect

# Configure PostgreSQL connection parameters
DB_USER = 'postgres'
DB_PASSWORD = 'postgres-password'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Connect to the PostgreSQL server with the maintenance database
conn = connect(
    dbname='postgres',
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)

# Create a new cursor
cur = conn.cursor()

# Check if the `pdc` database exists and create it if it doesn't
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'pdc'")
exists = cur.fetchone()
if not exists:
    cur.execute('CREATE DATABASE pdc')
    print("Database `pdc` created successfully.")
else:
    print("Database `pdc` already exists.")


# Create the `stock` table if it doesn't exist
create_table_stock_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS stock (
        id SERIAL PRIMARY KEY,
        event_time TIMESTAMPTZ NOT NULL,
        symbol VARCHAR(15) NOT NULL,
        price DOUBLE PRECISION NOT NULL
    )
""")
create_table_aggregated_stock_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS aggregated_stock (
        id SERIAL PRIMARY KEY,
        event_time TIMESTAMPTZ NOT NULL,
        symbol VARCHAR(15) NOT NULL,
        min DOUBLE PRECISION NOT NULL,
        max DOUBLE PRECISION NOT NULL,
        avg DOUBLE PRECISION NOT NULL,
        count INTEGER NOT NULL
    )
""")
cur.execute(create_table_stock_query)
cur.execute(create_table_aggregated_stock_query)


# Commit the transaction
conn.commit()
# Close the cursor and connection
cur.close()
conn.close()

print("Tables `stock` and `aggregated_stock` created if they didn't exist.")





