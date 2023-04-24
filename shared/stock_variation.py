import faust
import datetime

# Define the Faust schema
class StockVariation(faust.Record, serializer='json'):
    timestamp: datetime.datetime
    symbol: str
    price: float