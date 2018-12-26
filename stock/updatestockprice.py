# Get Stock daily share price and update into database
# import pandas as pd
import datetime as dt

from stock.tfnstock import RwDatabase
from stock.tfnstock import GetStock


# db = '../findata/cse.db'
# db = '../findata/nasdaq.db'
# db = '../findata/nyse.db'
# db = '../findata/amex.db'
db = '../findata/tsxv.db'
sqlflt = 'select * from companylist;'

sDate = '2015-01-01'
eDate = dt.datetime.today().strftime('%Y-%m-%d')
wrData = RwDatabase(db)

# Get all companies' symbol
lstCompany = wrData.read_sqldata(sqlflt)
symbol_arry = lstCompany['symbol']

# Write data into Database
lst_nodata = []
i = 0
for item in symbol_arry:
    i = i+1
    # symbol = item + '.CN'
    # symbol = item + '.TO'
    symbol = item + '.V'
    getStock = GetStock(symbol, sDate, eDate)
    stockdata = getStock.get_price()
    if stockdata.empty:
        lst_nodata.append(item)
        print("Company has no Data:", item, symbol)
    else:
        stockdata.reset_index(inplace=True)  # set date as a column
        stockdata['Date'] = stockdata['Date'].dt.strftime('%Y-%m-%d')
        tblname = item
        tbldata = stockdata
        wrData.write_sqldata(tblname, tbldata)
        print("Current item: ", i, symbol)
        print(stockdata.tail(1))


if __name__ == "__main__":
    print()

