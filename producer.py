import faust
from random import uniform, choice
from datetime import datetime
from shared import settings
from shared.stock_variation import StockVariation


# Define the Faust application
app = faust.App('random_stock_app_producer', broker=settings.broker_address)

# Define the Kafka topic
topic = app.topic(settings.topic_name, value_type=StockVariation)

stock_symbols = ['LOL','RHT','STT','DOL']

# Generate random messages with timestamp
# @app.agent(topic)
@app.timer(interval=0.3)
async def generate_random_messages():
    await topic.send(value=StockVariation(timestamp=datetime.now(settings.timezone), symbol = choice(stock_symbols), price=uniform(1, 100) ))


# Run the Faust application
if __name__ == '__main__':
    app.main()
