from datetime import timezone

# Configure PostgreSQL connection parameters
DB_USER = 'postgres'
DB_PASSWORD = 'postgres-password'
DB_HOST = 'localhost'
DB_PORT = '5432'

topic_name = 'random_messages'
broker_address = 'kafka://localhost:9092'
timezone = timezone.utc
