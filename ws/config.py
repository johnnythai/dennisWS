import alpaca_trade_api as api
import os


ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY')
ALPACA_URL = 'https://paper-api.alpaca.markets'

conn = api.StreamConn(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_URL)