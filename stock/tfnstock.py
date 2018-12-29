# Get stock price from Yahoo
import pandas as pd
import datetime as dt
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
import sqlite3
from typing import Union


class GetStock:

    __symbol: Union[pd.Series, pd.DataFrame]
    __start_date: str
    __end_date: str

    def __init__(self, symbol: Union[pd.Series, pd.DataFrame], start_date: str, end_date: str) -> None:
        # 对象内的私有变量
        self.__symbol = symbol
        self.__start_date = start_date
        self.__end_date = end_date

    def py_print(self) -> None:
        # print("this is: %s " % self.__symbol)
        print("Variable is ('%s', %s ,%s)" % (self.__symbol, self.__start_date, self.__end_date))

    def get_price(self) -> pd.DataFrame:
        yf.pdr_override()
        stockdata = pd.DataFrame()
        try:
            stockdata = pdr.get_data_yahoo(self.__symbol, self.__start_date, self.__end_date)
            # stockdata['Symbol'] = self.__symbol
        except ValueError:
            pass
        return stockdata

    def get_price_m(self):
        # 获取多个股票的单天价格
        yf.pdr_override()
        data = pd.DataFrame()
        for items in self.__symbol:
            stockdata = pdr.get_data_yahoo(items, self.__start_date, self.__end_date)
            # print(stockdata)
            # stockdata['Symbol'] = items
            data = data.append(stockdata)
        return data


class RwDatabase:
    def __init__(self, db,):
        self.__db = db
        # self.__sqlflt = sqlflt  # SQL

    # get data from sqlite database
    def get_sqldata(self, sqlflt):
        conn = sqlite3.connect(self.__db)  # Using the connect function which returns a Connection object
        cur = conn.cursor() # create a cursor object which allow to execute SQL queries against a databse
        cur.execute(sqlflt)
        results = cur.fetchall()
        data_results = pd.DataFrame(results)
        cur.close()
        conn.close()
        return data_results

    # Use DataFrame to Fetch data from sqlite database
    def read_sqldata(self, sqlflt):
        conn = sqlite3.connect(self.__db)  # Using the connect function which returns a Connection object
        cur = conn.cursor()  # create a cursor object which allow to execute SQL queries against a databse
        results = pd.read_sql(sqlflt, conn)
        cur.close()
        conn.close()
        return results

    # get data from sqlite database
    def get_sqltable(self, tblname):
        conn = sqlite3.connect(self.__db)  # Using the connect function which returns a Connection object
        cur = conn.cursor()  # create a cursor object which allow to execute SQL queries against a databse
        # cur.execute(sqlflt)
        # results = cur.fetchall()
        data_results = pd.read_sql_table(tblname, conn)
        cur.close()
        conn.close()
        return data_results

    # write data into sqlite database
    def write_sqldata(self, tblname, tbldata):
        conn = sqlite3.connect(self.__db)  # Using the connect function which returns a Connection object
        cur = conn.cursor()  # create a cursor object which allow to execute SQL queries against a databse
        # 执行一条SQL语句，插入一条记录:
        # "insert into catalog values (?,?,?,?)", t
        # df1.to_sql('users', con=engine, if_exists='append')
        # _wsql = "insert into " + tblname + " values (?,?,?,?)"
        df1 = tbldata
        df1.to_sql(tblname, con=conn, if_exists='append', index=False)
        # cur.execute(_wsql, tbldata)
        # cur.commit()
        cur.close()
        conn.close()
        return cur.lastrowid


class FormatStockData:
    def __init__(self, db: str):
        self._db = db

    # Return stock data with 5, 10, 20 days moving average data
    def ma_stockdata(self, symbol):

        dbc = RwDatabase(self._db)

        sqlflt_str = "select * from " + symbol + " Order By Date"
        sql_data = dbc.read_sqldata(sqlflt_str)

        df = pd.DataFrame()
        df['date'] = sql_data['Date']
        df['open'] = sql_data['Open']
        df['high'] = sql_data['High']
        df['low'] = sql_data['Low']
        df['close'] = sql_data['Close']
        df['volume'] = sql_data['Volume']
        df['change'] = df['close'] - df['close'].shift(1)
        df['ma5'] = df['close'].rolling(window=5, center=False).mean()
        df['ma10'] = df['close'].rolling(window=10, center=False).mean()
        df['ma20'] = df['close'].rolling(window=20, center=False).mean()
        df['v_ma5'] = df['volume'].rolling(window=5, center=False).mean()
        df['v_ma10'] = df['volume'].rolling(window=10, center=False).mean()
        df['v_ma20'] = df['volume'].rolling(window=20, center=False).mean()
        df['symbol'] = symbol

        df = df.dropna()
        ma_data = df

        return ma_data



if __name__ == "__main__":

    # 获取公司的股价
    lstNoData = []
    symbol = "GET"
    symbol_array = symbol + ".CN"
    sDate = "2018-01-01"
    eDate = dt.datetime.today().strftime("%Y-%m-%d")
    getStock = GetStock(symbol_array, sDate, eDate)
    getStock.py_print()
    stockdata = getStock.get_price()
    stockdata.reset_index(inplace=True)  # set date as a column
    if stockdata.empty:
        lstNoData.append(symbol)
    else:
        stockdata["Date"] = stockdata["Date"].dt.strftime("%Y-%m-%d")

    # print(stockdata[stockdata.Volume > 500000])
    print(stockdata.tail(1))
    print("No Data list: ", lstNoData)

    # write data:  SQL语句，插入记录:
    # db = "../findata/cse.db"
    # tblname = symbol
    # tbldata = stockdata
    # wrData = RwDatabase(db)
    # wrData.write_sqldata(tblname, tbldata)

