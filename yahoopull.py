import pandas_datareader as pdr
import datetime as dtt
import pandas as pd

dtTickers = pd.read_csv('nifty50.csv')
stDate = dtt.datetime(2005,1,1)
edDate = dtt.datetime.today()

for ticker in dtTickers['Symbol']:
    print(ticker.strip('\u200b'))
    data = pdr.DataReader(ticker.strip('\u200b'), 'yahoo', stDate, edDate)
    data.to_csv('Nifty50 Select/' + ticker.strip('\u200b') + ' ' + edDate.strftime('%m.%d.%Y') + '.csv')


# Just for BTC-USD
data = pdr.DataReader('BTC-USD', 'yahoo', dtt.datetime(2010,11,24), dtt.datetime.today())
data.to_csv('Inputs/Historical Data/Crypto/BTC-USD ' + dtt.datetime.today().strftime('%m.%d.%Y') + '.csv')




