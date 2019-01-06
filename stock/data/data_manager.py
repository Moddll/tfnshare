from typing import Iterable, Dict, Union, Tuple
import pandas as pd
from stock.data.database import MetadataDatabase, ExchangeDatabase
import atexit

databases: Dict[str, Union[MetadataDatabase, ExchangeDatabase]] = {}
data: Dict[str, Union[tuple, pd.DataFrame]] = {}


def get_data(exchange: str, symbol: str, start_date: str = '0000-00-00', end_date: str = '9999-99-99') -> pd.DataFrame:
    """
    Returns the stock data of symbol in exchange from start_date to end_date inclusive
    :return:
    A pandas DataFrame containing the data
    """
    if exchange not in databases:
        databases[exchange] = ExchangeDatabase(exchange, check_same_thread=False)
    if exchange + '/' + symbol not in data:
        data[exchange + '/' + symbol] = databases[exchange].read_stock_data(symbol)
    df = data[exchange + '/' + symbol]
    return df[(start_date <= df.index) & (df.index <= end_date)]


def get_data_multi(symbols: Dict[str, Iterable[str]], start_date: str = '0000-00-00', end_date: str = '9999-99-99') -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Returns the stock data of symbols from start_date to end_date
    :param symbols:
    A dictionary mapping exchanges to lists of symbols from that exchange
    :return:
    A dictionary mapping exchange to data frames
    """
    res = {}
    for exchange, symbol_list in symbols.items():
        res[exchange] = {symbol: get_data(exchange, symbol, start_date, end_date) for symbol in symbol_list}
    return res


def get_exchange_list() -> Tuple[str]:
    """
    Return a tuple containing the name of all exchanges
    """
    if 'meta' not in databases:
        databases['meta'] = MetadataDatabase()
    if 'cplist' not in data:
        data['cplist'] = databases['meta'].get_exchange_list()
    return data['cplist']


def get_company_list(exchange: str) -> pd.DataFrame:
    """
    Return the companylist table from the database
    If database is closed, ValueError is raised
    :return:
    A pandas DataFrame containing the companylist table
    """
    if 'meta' not in databases:
        databases['meta'] = MetadataDatabase(check_same_thread=False)
    if 'cplist' + exchange not in data:
        data['cplist' + exchange] = databases['meta'].get_company_list(exchange)
    return data['cplist' + exchange]


@atexit.register
def _cleanup():
    """
    Closes all databases. Should not be called manually and only invoked on exit
    """
    for db in databases.values():
        db.close()


if __name__ == '__main__':
    print(get_data('nyse', 'DDD', '2015-02-13', '2018-11-21'))
