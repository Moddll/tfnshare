# Get Stock daily share price and update into database
# import pandas as pd
import datetime as dt
from multiprocessing import Pool
from multiprocessing.dummy import Process
from itertools import repeat
from queue import Queue
import sqlite3

from stock.tfnstock import RwDatabase
from stock.tfnstock import GetStock


def main():
    # db = '../findata/cse.db'
    # db = '../findata/nasdaq.db'
    db = '../findata/nyse.db'
    # db = '../findata/amex.db'
    # db = '../findata/tsxv.db'
    sqlflt = 'select * from amex;'

    sDate = '2015-01-01'
    eDate = dt.datetime.today().strftime('%Y-%m-%d')
    wrData = RwDatabase(db)

    # Get all companies' symbol
    lstCompany = wrData.read_sqldata(sqlflt)
    symbol_arry = lstCompany['symbol']

    # # Write data into Database
    # lst_nodata = []
    # i = 0
    # for item in symbol_arry:
    #     i = i+1
    #     # symbol = item + '.CN'
    #     # symbol = item + '.TO'
    #     read_symbol(item, 'V', sDate, eDate, wrData)
    n = len(symbol_arry)
    q = Queue()
    # process = Process(target=write_data, args=(q, db))
    # process.start()

    with Pool(processes=32) as pool:
        pool.starmap(read_symbol, zip(symbol_arry, repeat('', n), repeat(sDate, n), repeat(eDate, n), repeat(q, n)))

    q.put(('end process', 'a'))
    # process.join()


def write_data(q: Queue, db: str):
    conn = sqlite3.connect(db)  # Using the connect function which returns a Connection object
    cur = conn.cursor()  # create a cursor object which allow to execute SQL queries against a databse
    # cur.execute(_wsql, tbldata)
    # cur.commit()
    while True:
        tblname, tbldata = q.get()
        if tblname == 'end process':
            break
        print(f"Writing {tblname}")
        tbldata.to_sql(tblname, con=conn, if_exists='replace', index=False)
    cur.close()
    conn.close()


def read_symbol(symbol: str, ext: str, sDate: str, eDate: str, q: Queue) -> None:
    for i in range(10):
        try:
            getStock = GetStock(symbol + ('.' + ext if ext else ''), sDate, eDate)
            stockdata = getStock.get_price()
            print("Current item: ", symbol)
            if not stockdata.empty:
                stockdata.reset_index(inplace=True)  # set date as a column
                stockdata['Date'] = stockdata['Date'].dt.strftime('%Y-%m-%d')
                # tblname = symbol
                # tbldata = stockdata
                # wrData.write_sqldata(tblname, tbldata)
                q.put((symbol, stockdata))
            break
        except:
            continue
    else:
        print(f"{symbol} Failed")


if __name__ == "__main__":
    import timeit
    print(timeit.timeit('main()', globals=globals(), number=1))



