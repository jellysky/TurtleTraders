import requests
import pandas as pd

def get_crypto_price(symbol):
    api_key = 'YOUR API KEY'
    api_url = f'https://cloud.iexapis.com/stable/crypto/{symbol}/price?token={api_key}'
    raw = requests.get(api_url).json()
    price = raw['price']
    return float(price)

btc = get_crypto_price('ethusd')
print('Price of 1 Bitcoin: {} USD'.format(btc))