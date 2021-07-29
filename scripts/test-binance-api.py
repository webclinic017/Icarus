import time
import json
from binance.client import Client
from binance.enums import *

# Global Variables
credential_file = r'C:\Users\bilko\PycharmProjects\trading-bot\test_credentials.json'
with open(credential_file, 'r') as cred_file:
    cred_info = json.load(cred_file)

client = Client(api_key=cred_info['Binance']['Production']['PUBLIC-KEY'], api_secret=cred_info['Binance']['Production']['SECRET-KEY'])

# Place the break point here
pass

response = client.create_oco_order(
    symbol='BTCUSDT',
    side=SIDE_SELL,
    quantity=0.005063,
    price=80750.17,
    stopPrice=19759,
    stopLimitPrice=19750.17,
    stopLimitTimeInForce=TIME_IN_FORCE_GTC)

order1 = client.get_order(symbol='BTCUSDT', orderId=response['orderReports'][0]['orderId'])

order2 = client.get_order(symbol='BTCUSDT', orderId=response['orderReports'][1]['orderId'])

cancel_order1 = client.cancel_order(symbol='BTCUSDT', orderId=response['orderReports'][0]['orderId'])

cancel_order2 = client.cancel_order(symbol='BTCUSDT', orderId=response['orderReports'][1]['orderId'])

# TODO: NEXT: Place the test OCO order here / then cancel it and see responses