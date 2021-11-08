# __author__ = 'koushik'

import numpy as np
import pandas as pd
import pandas_datareader as pdr
import datetime as dtt
import pyexcelerate as pe

import ibapi
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from threading import Thread
import os
import queue
import pyEX

class const():
    @staticmethod
    def quandlKey():
        return 'oJQ8bM4ArnvxyLUSNjgf'
    @staticmethod
    def postCrisis():
        return dtt.datetime(2020, 1, 1)
    @staticmethod
    def percPos():
        return .01
    @staticmethod
    def initCap():
        return 10000
    @staticmethod
    def stopTolerance():
        return 1
    @staticmethod
    def contractSize():
        return 100  # 100 shares of SPY
    @staticmethod
    def maxUnits():
        return 5
    @staticmethod
    def txCost():
        return 0.01
    @staticmethod
    def tradeCols():
        return ['T Ret','T <=2019', 'T >=2020','T Sharpe', 'T Mean','T Std',
                #'P Ret','P 2019', 'P 2020','P Mean','P Std','P Var','P Sharpe',
                'T-P Ret','T-P <=2019', 'T-P >=2020','T-P Sharpe','T-P Mean','T-P Std',
                'Trades','Start','End']
    @staticmethod
    def dailyCols():
        return ['SumT:All','SumT:<=2019','SumT:>=2020','SumT:Longs','SumT:Shorts',
                'SumT-P:All','SumT-P:<=2019','SumT-P:>=2020','SumT-P:Longs','SumT-P:Shorts',
                'SharpeT:All', 'SharpeT:<=2019','SharpeT:>=2020','SharpeT:Longs','SharpeT:Shorts',
                'SharpeT-P:All', 'SharpeT-P:<=2019', 'SharpeT-P:>=2020', 'SharpeT-P:Longs', 'SharpeT-P:Shorts',
                '%Win:All', '%Win:<=2019', '%Win:>=2020', '%Win:Longs', '%Win:Shorts',
                'Mean:All', 'Mean:<=2019', 'Mean:>=2020', 'Mean:Longs', 'Mean:Shorts',
                'Std:All', 'Std:<=2019', 'Std:>=2020', 'Std:Longs', 'Std:Shorts',
                'No:All', 'No:<=2019', 'No:>=2020', 'No:Longs', 'No:Shorts',
                'Stops','Start','End']
    @staticmethod
    def backtestCols():
        return ['Trade', 'Unit', 'Date', 'LastPrice', 'CurrentPrice', 'EntryPrice', 'ExitPrice', 'StopPrice', 'StopInd','N', 'Direction',
                'StartPos', 'PNL', 'TxCost', 'EndPos','Return', 'RisktoEq%', 'PrevTradeLoser','PostCrisis','S1orS2']
    @staticmethod
    def dailyUnitCols():
        return ['SumT:','SumT-P:','SharpeT:','SharpeT-P:','%Win:','Mean:','Std:','No:']
    @staticmethod
    def chartCols():
        return ['Passive','Turtle','Stock','PrevTradeLoser']
    @staticmethod
    def xlsTabsSN():
        return ['Metrics','PNL','Price','Chart']
    @staticmethod
    def xlsTabsPort():
        return ['By Trade', 'By Dailies']
    @staticmethod
    def dateFormat():
        return '%m.%d.%Y'
    @staticmethod
    def default_historic_data_id():
        return 50
    @staticmethod
    def default_get_contract_id():
        return 43
    @staticmethod
    def avAPIKey():
        return 'BMTRGCRI3Q8YOM9G'
    @staticmethod
    def iexAPIKey():
        return 'pk_f4dbd6160fdd42b4b6775b3b30515a97'

#DEFAULT_HISTORIC_DATA_ID=50
#DEFAULT_GET_CONTRACT_ID=43


class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT


        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT

class TestWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}

    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)


    ## get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    ## Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue


    def historicalData(self, tickerid , bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata=(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict=self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start:str, end:str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)

class TestClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)


    def resolve_ib_contract(self, ibcontract, reqId=const.default_get_contract_id()):

        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        ## Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details)==0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details)>1:
            print("got multiple contracts using first one")

        new_contract_details=new_contract_details[0]

        resolved_ibcontract=new_contract_details.contract

        return resolved_ibcontract


    def get_IB_historical_data(self, ibcontract, durationStr="10 Y", barSizeSetting="1 day",
                               tickerid=const.default_historic_data_id()):

# this is where you control the startDate
        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """


        ## Make a place to store the data we're going to return
        historic_data_queue = finishableQueue(self.init_historicprices(tickerid))

        # Request some historical data. Native method in EClient
        self.reqHistoricalData(
            tickerid,  # tickerId,
            ibcontract,  # contract,
            dtt.datetime.today().strftime("%Y%m%d %H:%M:%S %Z"),  # endDateTime,
            durationStr,  # durationStr,
            barSizeSetting,  # barSizeSetting,
            "TRADES",  # whatToShow,
            1,  # useRTH,
            1,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            [] ## chartoptions not used
        )

        ## Wait until we get a completed data, an error, or get bored waiting
        MAX_WAIT_SECONDS = 10
        print("Getting historical data from the server... could take %d seconds to complete " % MAX_WAIT_SECONDS)

        historic_data = historic_data_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if historic_data_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        self.cancelHistoricalData(tickerid)


        return historic_data

class TestApp(TestWrapper, TestClient):

    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()


def get_singlename_iblegacy_data(ticker):

    print(ticker)
    dtPrice = pd.read_csv('Inputs/Historical Data/IB Legacy/' + ticker + '_IB20161031.csv')
    dtPrice.columns = dtPrice.columns.str.strip()
    dtPrice.index = pd.to_datetime(dtPrice['Date'])

    return dtPrice

def get_singlename_ib_data(ticker):

    #print(ticker)
    if (os.path.exists('Inputs/Historical Data/IB/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')):
        dtPrice = pd.read_csv('Inputs/Historical Data/IB/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        dtPrice.index = pd.to_datetime(dtPrice['Date'], infer_datetime_format=True)
        dtPrice = dtPrice.drop(labels=['Date'], axis=1)
        print('Up to date data for ticker: %s from source: IB exists ... ' % ticker)
    else:
        # currently pulls 10y worth of data, so startDate is not used although it is passed
        print('Up to date data for ticker: %s does not exist, pulling from IB ... ' %ticker)
        #app = TestApp("127.0.0.1", 7496, 123)

        ibcontract = IBcontract()
        ibcontract.symbol = ticker
        ibcontract.secType = "STK"
        ibcontract.exchange="SMART"
        ibcontract.currency="USD"

        resolved_ibcontract = app.resolve_ib_contract(ibcontract)
        historic_data = app.get_IB_historical_data(resolved_ibcontract)

        #app.disconnect()
        dtPrice = pd.DataFrame(data=historic_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        dtPrice.index = pd.to_datetime(dtPrice['Date'], infer_datetime_format=True)
        dtPrice = dtPrice.drop(labels=['Date'], axis=1)
        dtPrice['Adj Close'] = dtPrice['Close']
        dtPrice.columns = dtPrice.columns.str.strip()

        dtPrice['Year'] = dtPrice.index.year
        dtPrice['Series'] = dtPrice['Adj Close'].pct_change()
        dtPrice.at[dtPrice.index[0], 'Series'] = dtPrice['Adj Close'].iloc[0] / dtPrice['Open'].iloc[0] - 1
        dtPrice['Ticker'] = ticker

        dtPrice.to_csv('Inputs/Historical Data/IB/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        print('Pulled and wrote historical data for ticker: %s on date: %s for source: IB successfully ...' % (ticker, dtt.datetime.today().strftime(const.dateFormat())))

    return dtPrice

def get_singlename_yahoo_data(ticker):

    if (os.path.exists('Inputs/Historical Data/Yahoo/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')):
        print('Up to date data for ticker: %s from source: yahoo exists ... ' %ticker)
        dtPrice = pd.read_csv('Inputs/Historical Data/Yahoo/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        dtPrice.index = pd.to_datetime(dtPrice['Date'], infer_datetime_format=True)
        dtPrice = dtPrice.drop(labels=['Date'], axis=1)
    else:
        print('Up to date data for ticker: %s does not exist, pulling from source: yahoo ... ' %ticker)
        yf.pdr_override()
        dtPrice = pdr.get_data_yahoo(ticker, 'yahoo', dtt.datetime.today() - dtt.timedelta(days=10*365), dtt.datetime.today())
        dtPrice.index = pd.to_datetime(dtPrice.index,infer_datetime_format=True)
        dtPrice['Year'] = dtPrice.index.year
        dtPrice['Series'] = dtPrice['Adj Close'].pct_change()
        dtPrice.at[dtPrice.index[0],'Series'] = dtPrice['Adj Close'].iloc[0] / dtPrice['Open'].iloc[0] - 1
        dtPrice['Ticker'] = ticker

        dtPrice.to_csv('Inputs/Historical Data/Yahoo/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        print('Pulled and wrote historical data for ticker: %s on date: %s for source: yahoo successfully ...' %(ticker,dtt.datetime.today().strftime(const.dateFormat())))

    return dtPrice

def get_singlename_quandl_data(ticker):

    if (os.path.exists('Inputs/Historical Data/Quandl/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')):
        print('Up to date data for ticker: %s from source: quandl exists ... ' %ticker)
        dtPrice = pd.read_csv('Inputs/Historical Data/Quandl/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        dtPrice.index = pd.to_datetime(dtPrice['Date'], infer_datetime_format=True)
        dtPrice = dtPrice.drop(labels=['Date'], axis=1)
    else:
        print('Up to date data for ticker: %s does not exist, pulling from source: quandl ... ' %ticker)
        dtPrice = qd.get('BTER/' + ticker, start = dtt.datetime.today() - dtt.timedelta(days=10*365), end = dtt.datetime.today())
        dtPrice.index = pd.to_datetime(dtPrice.index,infer_datetime_format=True)
        dtPrice['Year'] = dtPrice.index.year
        dtPrice['Series'] = dtPrice['Last'].pct_change()
        # dtPrice.at[dtPrice.index[0],'Series'] = dtPrice['Adj Close'].iloc[0] / dtPrice['Open'].iloc[0] - 1
        dtPrice['Ticker'] = ticker

        dtPrice.to_csv('Inputs/Historical Data/Quandl/' + ticker + ' ' + dtt.datetime.today().strftime(const.dateFormat()) + '.csv')
        print('Pulled and wrote historical data for ticker: %s on date: %s for source: yahoo successfully ...' %(ticker,dtt.datetime.today().strftime(const.dateFormat())))

    return dtPrice

def print_singlename_outputs(dts):

    wb = pe.Workbook()
    list_dts = dts
    ticker = dts[1]['Ticker'].iloc[0]
    for i, dt in enumerate(list_dts):
        # list_dts[i].to_csv(const.xlsTabs()[i] + '.csv', index=True)
        # print(i)

        if (const.xlsTabsSN()[i] == 'PNL'):
            dt['Trade'] = dt['Trade'] + 1
            dt = dt.fillna(0)

        data = [dt.columns.tolist(), ] + dt.values.tolist()
        indx = [''] + dt.index.tolist()
        data = [[index] + row for index, row in zip(indx, data)]
        wb.new_sheet(const.xlsTabsSN()[i], data=data)
        wb.save('Outputs/Single Name/SN Out ' + ticker + dtt.datetime.today().strftime(const.dateFormat()) + '.xlsx', )

def print_portfolio_outputs(dts):

    wb = pe.Workbook()
    list_dts = dts
    for i, dt in enumerate(list_dts):
        # list_dts[i].to_csv(const.xlsTabs()[i] + '.csv', index=True)
        # print(i)

        data = [dt.columns.tolist(), ] + dt.values.tolist()
        indx = [''] + dt.index.tolist()
        data = [[index] + row for index, row in zip(indx, data)]
        wb.new_sheet(const.xlsTabsPort()[i], data=data)
        wb.save('Outputs/Portfolio/Port Out ' + dtt.datetime.today().strftime(const.dateFormat()) + '.xlsx', )


def compute_ATR(dtPrice, mavg):
    # mavg=20
    dtN = np.zeros(shape=(dtPrice.shape[0], 6), dtype=float)

    # N1: Today's high less low, should always be +
    dtN[:, 0] = dtPrice['High'] - dtPrice['Low']
    # N2: Yesterday's close to today's high
    dtN[1:, 1] = np.absolute(dtPrice['Close'].iloc[0:-1].values - dtPrice['High'].iloc[1:].values)

    # N3: Yesterday's close to today's low
    dtN[1:, 2] = np.absolute(dtPrice['Close'].iloc[0:-1].values - dtPrice['Low'].iloc[1:].values)

    dtN[:, 3] = np.amax(dtN[:, 0:3], axis=1)

    dtN = pd.DataFrame(data=dtN, index=dtPrice.index, columns=['TR1', 'TR2', 'TR3', 'TRmax', 'N1', 'N2'])
    dtN['N1'] = dtN['TRmax'].rolling(center=False, window=mavg).mean()

    dtN['N2'] = np.nan
    for i in range(mavg, dtN.shape[0]):
        dtN.at[dtN.index[i],'N2'] = ((mavg - 1) * dtN['N1'].iloc[i - 1] + dtN['TRmax'].iloc[i]) / mavg
    # replacement of for loop - dtN.at[dtN.index[mavg:dtN.shape[0]], 'N2'] = ((mavg - 1) * dtN['N1'].iloc[mavg-1:dtN.shape[0]-1] + dtN['TRmax'].iloc[mavg:dtN.shape[0]]) / mavg


    # print('Finished computing N for Mavg = %d...'%mavg)

    return pd.concat([dtPrice, dtN], axis=1)

def compute_breakouts(dt):
    mavgs = [20, 10, 55, 20]
    dtMavg = np.zeros(shape=(dt.shape[0], len(mavgs)))

    for i, m in enumerate(mavgs):
        maxIndx = np.where(dt['Close'].rolling(center=False, window=m).max() == dt['Close'])
        minIndx = np.where(dt['Close'].rolling(center=False, window=m).min() == dt['Close'])
        dtMavg[maxIndx, i] = 1
        dtMavg[minIndx, i] = -1
        # print('\nCompleted computation of rolling maxes and mins for m = %d...'%m)

    dtMavg[dt.shape[0] - 1, 0] = 0
    dtMavg[dt.shape[0] - 1, 2] = 0
    dtMavg = pd.DataFrame(data=dtMavg, index=dt.index, columns=['MavgB1', 'MavgS1', 'MavgB2', 'MavgS2'])
    return pd.concat([dt, dtMavg], axis=1)

def calculate_chart_data(dt):

    dtOut = pd.DataFrame(data=np.zeros(shape=(dt.index.shape[0],len(const.chartCols()))),index=dt.index,columns=const.chartCols())
    dtOut['Passive'] = dt['Series'].cumsum()
    dtOut['Turtle'] = dt['Return'].fillna(0).cumsum()
    dtOut['Stock'] = dt['Adj Close']
    dtOut['PrevTradeLoser'] = dt['PrevTradeLoser']

    return dtOut

def calculate_trade_pnl(dtRow, entryThresholds):
    # calculates trade PNL for one trade, must track PNL for both PrevTradeLoser and PrevTradeWinner or else you don't know
    # if last trade was a winner or loser

    for i in range(0, dtRow.shape[0]):
        # print(i)
        # sets unit
        if (len(np.where(dtRow['EntryPrice'].iloc[i] * dtRow['Direction'].iloc[i] >= entryThresholds * dtRow['Direction'].iloc[i])[0]) == 0):
            dtRow.at[dtRow.index[i],'Unit'] = dtRow['Unit'].iloc[i - 1]
        else:
            dtRow.at[dtRow.index[i],'Unit'] = max(np.where(dtRow['EntryPrice'].iloc[i] * dtRow['Direction'].iloc[i] >= entryThresholds * dtRow['Direction'].iloc[i])[0].max() + 1, dtRow['Unit'].iloc[i - 1])
            # print('\nMax unit is: %d' %dtRow['Unit'].iloc[i])
        dtRow.at[dtRow.index[i],'StopPrice'] = dtRow['EntryPrice'].iloc[i] - const.stopTolerance() * dtRow['Direction'].iloc[i] * dtRow['N'].iloc[i]
        # tx costs incurred and entry price is a member of entryThresholds when unit changes (pyramiding)
        if (i > 0):
            if (dtRow['Unit'].iloc[i] - dtRow['Unit'].iloc[i - 1] > 0):
                dtRow.at[dtRow.index[i],'TxCost'] = dtRow['TxCost'].iloc[0]  # txCost is initialized in backtest_data
                dtRow.at[dtRow.index[i],'EntryPrice'] = entryThresholds[dtRow['Unit'].iloc[i].astype(int) - 1]
            # current StartPos is last EndPos
            dtRow.at[dtRow.index[i],'StartPos'] = dtRow['EndPos'].iloc[i - 1] + (dtRow['Unit'].iloc[i] - dtRow['Unit'].iloc[i - 1]) * dtRow['StartPos'].iloc[0]
        # if there is a stop, compute stops using EOD prices and see if it works
        if (dtRow['Direction'].iloc[i] * dtRow['StopPrice'].iloc[i] >= dtRow['Direction'].iloc[i] * dtRow['CurrentPrice'].iloc[i]) and (dtRow['StopInd'].iloc[i] == 1):
            dtRow.at[dtRow.index[i + 1:dtRow.shape[0]],'StopInd'] = 0
            dtRow.at[dtRow.index[i],'ExitPrice'] = dtRow['CurrentPrice'].iloc[i]
#dtRow['StopPrice'].iloc[i]
            dtRow.at[dtRow.index[i],'TxCost'] = dtRow['TxCost'].iloc[0]  # txCost is initialized in backtest_data
        # calculate PNL by row
        dtRow.at[dtRow.index[i],'PNL'] = (dtRow['ExitPrice'].iloc[i] / dtRow['EntryPrice'].iloc[i] - 1) * dtRow['Direction'].iloc[i] * dtRow['StartPos'].iloc[i] * dtRow['StopInd'].iloc[i]
        # if there is no stop before the end
        if (i == dtRow.shape[0] - 1) and (dtRow['StopInd'].iloc[i] == 1):
            dtRow.at[dtRow.index[i],'TxCost'] = dtRow['TxCost'].iloc[0]  # txCost is initialized in backtest_data
        dtRow.at[dtRow.index[i],'EndPos'] = dtRow['StartPos'].iloc[i] + dtRow['PNL'].iloc[i] - dtRow['TxCost'].iloc[i]
    dtRow['Return'] = dtRow['PNL'] / dtRow['StartPos']
    dtRow['RisktoEq%'] = dtRow['StartPos'] / (dtRow['StartPos'] + const.initCap())

    # print('\nCalculating PNL for entry: %s and exit: %s'%(dtRow['Date'].iloc[0].strftime('%Y-%m-%d'),
    #                                                     dtRow['Date'].iloc[i].strftime('%Y-%m-%d')))
    return dtRow[dtRow['StopInd'] == 1]

def calculate_unit_backtest(dt):
    # runs through PNL for all trades for one singlename
    #dt = dtPrice

    prevTradeLoser = 1
    dtPNL = pd.DataFrame(data=np.zeros(shape=(0, len(const.backtestCols()))), columns=const.backtestCols())
    entryIndxB1 = np.where(dt['MavgB1'] != 0)
    # entryIndxB2 = np.where(dt['MavgB2'] != 0)
    trade = 0
    endDate = dt.index[0]

    while trade < len(entryIndxB1[0]):

        entry = entryIndxB1[0][trade]
        unitPos = const.percPos() * const.initCap() / (dt['N2'].iloc[entry] * const.contractSize())
        entryString = 'MavgB1'
        exitString = 'MavgS1'

        if (prevTradeLoser == False):  # if previous trade is a winner then evaluate the next S1 entry
            # find the set of entries using the S2 series
            entryS2 = np.where(np.logical_and(dt['MavgS2'] != 0, dt['MavgS2'].index > dt.index[entry]))[0]
            # if the next entry is in S1 series then prevTradeLoser stays as is, if not then make prevTradeLoser True
            if (np.append(entryS2, entry).min() != entry):
                prevTradeLoser = True
                entry = np.append(entryS2, entry).min()
                entryString = 'MavgB2'
                exitString = 'MavgS2'

        exit = np.where(np.logical_and(dt[exitString] == -1 * dt[entryString].iloc[entry],
                                       dt[exitString].index > dt[entryString].index[entry]))[0]
        exit = np.append(exit, dt.shape[0] - 1).min()

        # print('\ntrade: %d, entry: %d, exit: %d, entryString: %s, exitString: %s' %(trade,entry,exit,entryString,exitString))
        # initialize dtRow and set entire column
        dtRow = pd.DataFrame(data=np.zeros(shape=(exit - entry, len(const.backtestCols()))), columns=const.backtestCols())

        dtRow['Trade'] = trade # tradeCounter
        dtRow['Date'] = dt.index[entry + 1:exit + 1]
        dtRow['LastPrice'] = dt['Close'].iloc[entry:exit].values
        dtRow['CurrentPrice'] = dt['Close'].iloc[entry + 1:exit + 1].values
        dtRow['EntryPrice'] = dt['Close'].iloc[entry:exit].values
        dtRow['ExitPrice'] = dt['Close'].iloc[entry + 1:exit + 1].values
        dtRow['N'] = dt['N2'].iloc[entry:exit].values
        dtRow['Direction'] = dt[entryString].iloc[entry]
        dtRow['StopInd'] = 1
        dtRow['PrevTradeLoser'] = prevTradeLoser
        if (dt.index[entry + 1] > const.postCrisis()):
            dtRow['PostCrisis'] = 1

        # initializing first value of column
        dtRow.at[0,'EntryPrice'] = dt['Close'].iloc[entry]
        dtRow.at[0,'StartPos'] = unitPos
        dtRow.at[0,'TxCost'] = const.txCost() * unitPos

        entryThresholds = np.arange(0, const.maxUnits()) * 0.5 * dtRow['N'].iloc[0] * dt[entryString].iloc[entry] + dt['Close'].iloc[entry]
        dtRow = calculate_trade_pnl(dtRow, entryThresholds)

        if (dtRow['PNL'].sum() > 0):
            prevTradeLoser = False
        else:
            prevTradeLoser = True

        if (entryString=='MavgB2'):
            dtRow['S1orS2'] = 2
        else:
            dtRow['S1orS2'] = 1

        # dtRow.to_csv('row.csv')
        dtPNL = pd.concat([dtPNL, dtRow], axis=0)
        endDate = dtPNL['Date'].iloc[-1]
        trade = np.where(dt.index[entryIndxB1[0]] > endDate)[0]
        trade = np.append(trade, len(entryIndxB1[0])).min()

    dtPNL.index = dtPNL['Date'] #range(0, dtPNL.shape[0])
    dtPNL = dtPNL.drop(columns=['Date'],axis=1)
    #dtPNL.to_csv('PNL.csv')
    return dtPNL

def calculate_singlename_backtest(s, source):

#s = 'USDT-USD', source = 'Yahoo'

    if (source == 'IB'):
        dtPrice = get_singlename_ib_data(s)
    elif (source == 'Yahoo'):
        dtPrice = get_singlename_yahoo_data(s)
    elif (source == 'IB Legacy'):
        dtPrice = get_singlename_iblegacy_data(s)

    dtPrice = compute_ATR(dtPrice, 20) # compute the ATR
    dtPrice = compute_breakouts(dtPrice) # compute breakouts
    dtPNL = calculate_unit_backtest(dtPrice) # compute breakouts
    dtPNL = dtPrice.merge(dtPNL, how='outer', left_index=True, right_index=True) # merge breakouts with prices
    dtChart = calculate_chart_data(dtPNL)
    dtMetrics = calculate_singlename_trade_outputs(dtPNL) # calculate returns: by year, active or passive strategy, sharpe ratio

    return dtMetrics,dtPNL,dtPrice,dtChart


def calculate_singlename_trade_outputs(dtPNL):
    # Change to by trade - include mean returns, mean stdev,
    # calculates returns per year for one singlename

    dtOut = pd.DataFrame(data=np.zeros(shape=(1, len(const.tradeCols()))), columns=const.tradeCols(),index=[dtPNL['Ticker'].iloc[0]])
    prevIndx = np.where(dtPNL['PrevTradeLoser'] == 1)
    prev2019Indx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1,dtPNL['Year'] < 2020))
    post2019Indx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1, dtPNL['Year'] >= 2020))
    ticker = dtPNL['Ticker'].iloc[0]
    timeScalar = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].count().mean()

    dtOut.at[ticker,'T Ret'] = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().sum()
    dtOut.at[ticker,'T <=2019'] = dtPNL.iloc[prev2019Indx].groupby(['Trade'])['Return'].sum().sum()
    dtOut.at[ticker, 'T >=2020'] = dtPNL.iloc[post2019Indx].groupby(['Trade'])['Return'].sum().sum()
    #dtOut.at[ticker,['T 2019','T 2020']] = dtPNL.iloc[prevIndx].groupby(['Year'])['Return'].sum().values
    dtOut.at[ticker, 'T Sharpe'] = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().mean() / dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().std()
    dtOut.at[ticker,'T Mean'] = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().mean()
    dtOut.at[ticker,'T Std'] = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().std()
    #dtOut.at[ticker,'T Var'] = dtPNL.iloc[prevIndx].groupby(['Trade'])['Return'].sum().var()
    #dtOut.at[ticker, 'P Ret'] = dtPNL['Series'].sum()
    #dtOut.at[ticker, ['P 2019', 'P 2020']] = dtPNL.groupby(['Year'])['Series'].sum().values
    #dtOut.at[ticker, 'P Mean'] = dtPNL['Series'].mean() * timeScalar
    #dtOut.at[ticker, 'P Std'] = dtPNL['Series'].std() * np.sqrt(timeScalar)
    #dtOut.at[ticker, 'P Var'] = dtPNL['Series'].var() * timeScalar
    #dtOut.at[ticker, 'P Sharpe'] = dtPNL['Series'].mean() * timeScalar / (dtPNL['Series'].std() * np.sqrt(timeScalar))

    dtOut.at[ticker, 'T-P Ret'] = dtOut['T Ret'].iloc[0] - dtPNL['Series'].sum()
    dtOut.at[ticker, 'T-P <=2019'] = dtOut['T <=2019'].iloc[0] - dtPNL['Series'].iloc[np.where(dtPNL['Year'] < 2020)].sum()
    dtOut.at[ticker, 'T-P >=2020'] = dtOut['T >=2020'].iloc[0] - dtPNL['Series'].iloc[np.where(dtPNL['Year'] >= 2020)].sum()
    dtOut.at[ticker, 'T-P Sharpe'] = dtOut['T Sharpe'].iloc[0] - dtPNL['Series'].mean() * timeScalar / (dtPNL['Series'].std() * np.sqrt(timeScalar))
    dtOut.at[ticker, 'T-P Mean'] = dtOut['T Mean'].iloc[0] - dtPNL['Series'].mean() * timeScalar
    dtOut.at[ticker, 'T-P Std'] = dtOut['T Std'].iloc[0] - dtPNL['Series'].std() * np.sqrt(timeScalar)
    #dtOut.at[ticker, 'T-P Var'] = dtOut['T Var'].iloc[0] - dtOut['P Var'].iloc[0]

    dtOut.at[ticker,'Trades'] = dtPNL['Trade'].nunique()
    dtOut.at[ticker,'Start'] = dtPNL.index[0]
    dtOut.at[ticker,'End'] = dtPNL.index[-1]

    return dtOut

def calculate_unit_daily_outputs(dtPNL,stratIndx,passIndx,colStr):
# Need to add prevIndx to screen out PrevTradeWinner trades
#['SumT:','SumT-P:','SharpeT:','SharpeT-P:','%Win:','Mean:','Std:','No:']

    cols = [col + colStr for col in const.dailyUnitCols()]
    ticker = dtPNL['Ticker'].iloc[0]
    dtOut = pd.DataFrame(data=np.zeros(shape=(1, len(const.dailyUnitCols()))),index=[ticker],columns=cols)
    numIndx = np.where(dtPNL['Trade'].notnull())

    if len(numIndx[0]) > 0:
        # sets cols equal to [T Ret Sum, T - P Ret Sum, T Sharpe, T-P Sharpe, T % Wins, T Ret Mean, T Ret Std, T # of Trades]
        dtOut.at[ticker, cols] = [dtPNL['Return'].iloc[stratIndx].sum(),
                                  dtPNL['Return'].iloc[stratIndx].sum() - dtPNL['Series'].iloc[passIndx].sum(),
                                  dtPNL['Return'].iloc[stratIndx].mean() / dtPNL['Return'].iloc[stratIndx].std(),
                                  dtPNL['Return'].iloc[stratIndx].mean() / dtPNL['Return'].iloc[stratIndx].std() - dtPNL['Series'].iloc[passIndx].mean() / dtPNL['Series'].iloc[passIndx].std(),
                                  dtPNL.iloc[stratIndx][dtPNL['Return'].iloc[stratIndx] > 0].shape[0] / len(stratIndx[0]),
                                  dtPNL['Return'].iloc[stratIndx].mean(),dtPNL['Return'].iloc[stratIndx].std(),
                                  dtPNL['Trade'].iloc[stratIndx].nunique()]

    return dtOut

def calculate_singlename_daily_outputs(dtPNL):
    #dtOut = pd.DataFrame(data=np.zeros(shape=(0,len(const.dailyCols()))),columns=const.dailyCols())

    ticker = dtPNL['Ticker'].iloc[0]
    prevIndx = np.where(dtPNL['PrevTradeLoser'] == 1)
    oldIndx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1,dtPNL['Year'] <= 2019))
    newIndx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1,dtPNL['Year'] >= 2020))
    longIndx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1,dtPNL['Direction'] == 1))
    shortIndx = np.where(np.logical_and(dtPNL['PrevTradeLoser'] == 1,dtPNL['Direction'] == -1))
    endVals = [np.logical_and(dtPNL['StopInd'].iloc[1:].values == 0,dtPNL['StopInd'].iloc[:-1].values == 1).sum() / dtPNL['Trade'].nunique(),
               dtPNL.index[0],dtPNL.index[-1]]

    dtOut = pd.concat([calculate_unit_daily_outputs(dtPNL, prevIndx, range(0,dtPNL.shape[0]), 'All'),
                       calculate_unit_daily_outputs(dtPNL, oldIndx, np.where(dtPNL['Year'] <= 2019), '<=2019'),
                       calculate_unit_daily_outputs(dtPNL, newIndx, np.where(dtPNL['Year'] >= 2020), '>=2020'),
                       calculate_unit_daily_outputs(dtPNL,longIndx, longIndx, 'Longs'), # not sure what to compare it to for longs
                       calculate_unit_daily_outputs(dtPNL, shortIndx, shortIndx, 'Shorts'),
                       pd.DataFrame(data=np.expand_dims(endVals,axis=0),index=[ticker],columns=['Stops','Start','End'])],axis=1)

    return dtOut[const.dailyCols()]

def calculate_portfolio_outputs(outStr):

    dtNames = pd.read_csv('Inputs/Components/tradeNames.csv')

    if (outStr in ['trades','trade']):

        dtOut = pd.DataFrame(data=np.zeros(shape=(0, len(const.tradeCols()))), columns=const.tradeCols())
        for i in range(0, dtNames.shape[0]):
            dtMetrics, dtPNL, dtPrice, dtChart = calculate_singlename_backtest(dtNames['Ticker'].iloc[i],dtNames['Source'].iloc[i])
            dtOut = pd.concat([dtOut, dtMetrics], axis=0)
            print('Calculated trade outputs for ticker: %s ...' %dtNames['Ticker'].iloc[i])

            if (dtPNL['Ticker'].iloc[0] in ['TATAMOTORS.NS','BTC-USD']):
                print_singlename_outputs([dtMetrics, dtPNL.fillna(0), dtPrice, dtChart])

    elif (outStr in ['daily','dailies']):

        dtOut = pd.DataFrame(data=np.zeros(shape=(0, len(const.dailyCols()))), columns=const.dailyCols())
        for i in range(0, dtNames.shape[0]):
            dtMetrics, dtPNL, dtPrice, dtChart = calculate_singlename_backtest(dtNames['Ticker'].iloc[i],dtNames['Source'].iloc[i])
            dtOut = pd.concat([dtOut, calculate_singlename_daily_outputs(dtPNL)], axis=0)
            print('Calculated daily outputs for ticker: %s ...' % dtNames['Ticker'].iloc[i])
    else:
        print('Out string was not recognized ... ')
        dtOut = []

    return dtOut

def generate_trades():

    dtNames = pd.read_csv('Inputs/Components/tradeNames.csv')

    for i in range(0, dtNames.shape[0]):
        dtMetrics, dtPNL, dtPrice, dtChart = calculate_singlename_backtest(dtNames['Ticker'].iloc[i],dtNames['Source'].iloc[i])
        print('\nFinished calculating %s PNL for %s...' % (dtPNL.index[-1].strftime('%Y-%m-%d'), dtNames['Ticker'].iloc[i]))

        if (dtPNL['Direction'].iloc[-1].is_integer() == True):
            print('Put on units: %.2f with change: %s of stock: %s on date: %s with stop: %.2f...'
                  % ((dtPNL['Direction'].iloc[-1] * 500 / dtPNL['N1'].iloc[-1]) / dtPNL['CurrentPrice'].iloc[-1],
                     str(dtPNL['Unit'].iloc[-1] - dtPNL['Unit'].iloc[-2]),
                     dtNames['Ticker'].iloc[i],
                     dtPNL.index[-1].strftime('%Y-%m-%d'),dtPNL['StopPrice'].iloc[-1]))
            dtPNL.to_csv('Outputs/Trades/' + dtNames['Ticker'].iloc[i] + ' ' + dtt.datetime.today().strftime('%Y-%m-%d') + '.csv')

        if (dtPNL['S1orS2'].iloc[-1] == 1):
            stopCol = 'MavgS1'
        else: #(dtOut['S1orS2'].iloc[-1] == 2):
            stopCol = 'MavgS2'

        if (dtPNL['Direction'].iloc[-1] * -1 == dtPNL[stopCol].iloc[-1]):
            print('Get out of trade on stock: %s on date: %s...' % (dtNames['Ticker'].iloc[i], dtPNL.index[-1].strftime('%Y-%m-%d')))
            # return dtOut


def main(argv = sys.argv):

# connect to IB
## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()
app = TestApp('127.0.0.1', 7496, 123)
generate_trades()

dtTrade = calculate_portfolio_outputs('trade')
dtDaily = calculate_portfolio_outputs('daily')
print_portfolio_outputs([dtTrade,dtDaily])

dtMetrics, dtPNL, dtPrice, dtChart = calculate_singlename_backtest('TTM','IB')
print_singlename_outputs([dtMetrics,dtPNL,dtPrice,dtChart])
dtMetrics, dtPNL, dtPrice, dtChart = calculate_singlename_backtest('ETH-USD','Yahoo')
print_singlename_outputs([dtMetrics,dtPNL,dtPrice,dtChart])

gg = pdr.DataReader('btc', 'stooq', dtt.datetime.today() - dtt.timedelta(days=10*365), dtt.datetime.today())

gg = pdr.get_data_alphavantage(symbols='AAPL', function='TIME_SERIES_DAILY_ADJUSTED', start=dtt.datetime.today() - dtt.timedelta(days=10*365), end=dtt.datetime.today(), api_key = const.avAPIKey())
gg = pdr.get_data_alphavantage(function='DIGITAL_CURRENCY_DAILY', start=dtt.datetime.today() - dtt.timedelta(days=10*365), end=dtt.datetime.today(), api_key = const.avAPIKey())
iexClient = pyEX.Client(api_token=const.iexAPIKey(), version='stable')
df = iexClient.chartDF(symbol='ETHUSD', timeframe='5d')
eth = yf.download('ETH', start=dtt.datetime.today() - dtt.timedelta(days=10*365), end=dtt.datetime.today())

if __name__ == "__main__":
    sys.exit(main())
