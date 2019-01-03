# Policy:  Check Unusual Daily Volume
import pandas as pd

from stock.tfnstock import RwDatabase
from stock.tfnstock import FormatStockData

# set database
db = '../../findata/cse.db'
dbc = RwDatabase(db)

# Get Company List
sqlflt = "select * from companylist;"
lstCompany = dbc.read_sqldata(sqlflt)
symbol_array = lstCompany['symbol']

# Get stock data from database
data = pd.DataFrame()
formatdata = FormatStockData(db)
for index, row in lstCompany.iterrows():

    madata = formatdata.ma_stockdata(row['symbol']).tail(1)
    madata['company'] = row['company']
    madata['list_date'] = row['list_date']
    madata['v_change'] = madata['volume'] - madata['v_ma5']
    # madata['v_change'] = madata['v_change'].map('{:,.2f}'.format)
    madata.round(2)
    data = data.append(madata)

data = data.round(2)

file_name = 'madata.csv'
data.to_csv(file_name, sep='\t', encoding='utf-8', index=False)

updateVolTbl = RwDatabase(db)
updateVolTbl.write_sqldata('daily_ma', data)


if __name__ == "__main__":
    print(data.tail(10))

