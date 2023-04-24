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
    # Create the `stock` table if it doesn't exist
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS stock (
            id SERIAL PRIMARY KEY,
            event_time TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(15) NOT NULL,
            price DOUBLE PRECISION NOT NULL
        )
    """)

    cur.execute(create_table_query)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


    print("Table `stock` created successfully.")

else:
    print("Database `pdc` already exists.")


