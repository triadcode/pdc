from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time

# Producer configuration
def produce_messages(topic):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for i in range(5):
        message = {'message_id': i, 'content': f'Test message {i}'}
        producer.send(topic, message)
        print(f"Produced: {message}")

    producer.flush()
    producer.close()

# Consumer configuration
def consume_messages(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=None,
    )

    for msg in consumer:
        print(f"Consumed: {msg.value}")

if __name__ == "__main__":
    topic = 'test_topic'
    produce_messages(topic)

    # Allow some time for the messages to be produced before consuming them
    time.sleep(2)

    consume_messages(topic)
