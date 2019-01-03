from __future__ import annotations
from typing import Optional, Union, Tuple, Dict, Iterable
from itertools import repeat
import sqlite3
import pandas as pd
import re
import os


class RwDatabase:
    """
    Manages dataIO to database files

    === Representation Invariants ===
    _conn is None iff no connection is open
    _cur is None iff _conn is None
    """
    _db: str
    _conn: sqlite3.Connection
    _cur: sqlite3.Cursor

    def __init__(self, db_path: str, db_name: str, open_db: bool = True):
        """
        Creates a base database object
        :param db_path:
        Path to database
        :param db_name:
        Name of database
        :param open_db:
        Auto-open the database on creation. Default True.
        """
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self._db = db_path + db_name
        self._conn = None
        self._cur = None

        if open_db:
            self.open()

    def open(self) -> None:
        """
        Opens the connection to database. If a connection is already open,
        nothing is done.
        """
        if not self.is_open():
            self._conn = sqlite3.connect(self._db)
            self._cur = self._conn.cursor()

    def close(self, commit=True, close=True) -> None:
        """
        If a connection to the database is open, it is closed.
        If commit, changes will be committed before closing. Default True.
        If close, connection object will be closed. Default True
        """
        if self.is_open():
            if commit:
                self._conn.commit()

            if close:
                self._conn.close()

            self._conn = None
            self._cur = None

    @property
    def cursor(self) -> sqlite3.Cursor:
        """
        Provides the cursor for this database
        """
        return self._cur

    def commit(self) -> None:
        """
        Commits the changes made.
        """
        self._ensure_open()
        self._conn.commit()

    def _ensure_open(self) -> None:
        """
        Raises ValueError iff database is closed
        """
        if not self.is_open():
            raise ValueError('Connection Already Open')

    def have_column(self, table: str, colname: str) -> bool:
        """
        Return True iff column colname exists in table
        """
        self._ensure_open()
        self._cur.execute(f'PRAGMA table_info("{table}");')
        res = zip(*self._cur.fetchall())
        next(res)
        return colname in next(res)

    def ensure_column(self, table: str, colname: str, type: str) -> None:
        """
        Creates column colname of type type if it does it does not exist in table
        """
        self._ensure_open()
        if not self.have_column(table, colname):
            self._cur.execute('alter table \"{}\" add \"{}\" \"{}\";'.format(table, colname, type))

    def write_columns(self, table: str, data: pd.DataFrame) -> None:
        """
        Writes data into the database

        Precondition:
        Table table must exist
        """
        self.ensure_table('temp', {'Date': 'TEXT'})
        self._cur.executemany("INSERT INTO  temp (Date) VALUES (?);", zip(data.index))
        self._cur.execute("INSERT INTO \"{0}\" (Date) "
                          "SELECT temp.Date "
                          "FROM temp "
                          "LEFT JOIN \"{0}\" on \"{0}\".Date = temp.Date "
                          "WHERE \"{0}\".Date is null;".format(table))
        labels = list(data.columns.values)
        for column in labels:
            self.ensure_column(table, column, 'REAL')
        self._cur.executemany(f"update \"{table}\" set  ({' '.join(labels)}) = ({' '.join(repeat('?', len(labels)))}) where Date = ?;", (map(lambda x: x[1:] + (x[0],), data.itertuples())))
        self._cur.execute("DROP TABLE temp")

    def read_column(self, table: str, columns: Union[str, Iterable[str]]) -> pd.DataFrame:
        """
        Reads columns from table. If columns is a string, then only the corresponding
        column is read
        :return:
        A pandas DataFrame containing the data
        """
        if isinstance(columns, str):
            return pd.read_sql(f"select {columns} from \"{table}\"", self._conn)
        else:
            return pd.read_sql(f"select {', '.join(columns)} from \"{table}\";", self._conn)

    def have_table(self, table: str) -> bool:
        """
        Returns True iff table exists in this database
        """
        self._ensure_open()
        self._cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND UPPER(name) LIKE UPPER(?);", (table,))
        return self._cur.fetchone() is not None

    def ensure_table(self, table: str, cols: Dict[str, str]) -> None:
        """
        Ensures table exists in the database. If it does not exist, it is created
        """
        self._ensure_open()
        if not self.have_table(table):
            if not cols:
                print(f"create table \"{table}\";")
                self._cur.execute(f"create table \"{table}\";")
            else:
                self._cur.execute(f"create table \"{table}\" ({', '.join(map(' '.join, cols.items()))});")


    def is_open(self) -> bool:
        """
        :return:
        If this database has a open connection
        """
        return self._conn is not None

    def __enter__(self) -> RwDatabase:
        self.open()
        self._conn.__enter__()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._conn.__exit__(*args, **kwargs)
        self.close(commit=False, close=False)


class MetadataDatabase(RwDatabase):

    def __init__(self, db_path: str = 'findata', db_name = 'metadata.db', open_db: bool = True):
        """
        Creates a base database object
        :param db_path:
        Path to database. Default
        :param db_name:
        Name of database
        :param open_db:
        Auto-open the database on creation. Default True.
        """
        RwDatabase.__init__(self, db_path, db_name, open_db)

    def get_company_list(self, exchange: str) -> pd.DataFrame:
        """
        Return the companylist table from the database
        If database is closed, ValueError is raised
        :return:
        A pandas DataFrame containing the companylist table
        """
        self._ensure_open()
        if not exchange.isalpha():
            raise ValueError("Exchange Must Be Alphabetic")
        return pd.read_sql('select * from \"{}\";'.format(exchange.lower()), self._conn)

    def get_symbols(self, exchange: str, df: pd.DataFrame = None) -> pd.Series:
        """
        Return the companylist table from the database
        If database is closed, ValueError is raised

        If data is specified, the symbols from data is returned instead. If data is specified
        no error is raised if the database is closed.
        :return:
        A pandas Series containing the symbols in companylist table
        """
        if df is None:
            df = self.get_company_list(exchange)
        return df['symbol']

    def get_exchange_list(self) -> Tuple[str]:
        """
        Returns a list of exchanges
        :return:
        A pandas DataFrame containing the data.
        """
        self._ensure_open()
        self._cur.execute('select Name from exchange_list')
        res = self._cur.fetchall()
        return next(zip(*res)) if res else []

    def write_exchange_update_date(self, exchange: str, date: Optional[str]) -> None:
        """
        Updates the last update entry in the database
        :param exchange:
        Exchange for which metadata should be obtained
        :param date:
        Last update date
        """
        self._ensure_open()
        if not exchange.isalpha():
            raise ValueError("Exchange Must Be Alphabetic")
        self._cur.execute("update exchange_list set last_update=? where Name=?", (date, exchange))

    def get_exchange_metadata(self, exchange: str = None) -> Union[pd.DataFrame, tuple]:
        """
        Returns the metadata associated with exchange, or with all exchanges
        if exchange is not specified
        :param exchange:
        Exchange for which metadata should be obtained
        :return:
        A tuple containing the metadata if exchange is specified
        or a pandas DataFrame containing the metadata of all exchanges
        """
        self._ensure_open()
        if exchange is None:
            return pd.read_sql('select * from exchange_list', self._conn)
        else:
            if not exchange.isalpha():
                raise ValueError("Exchange Must Be Alphabetic")
            self._cur.execute('select * from exchange_list where Name = ?;', (exchange,))
            return self._cur.fetchone()


class ExchangeDatabase(RwDatabase):
    def __init__(self, exchange: str, open_db: bool = True, path='findata/'):
        """
        Creates a exchange database object

        :param exchange
        Name of the exchange
        :param path:
        Path to database. Default: 'findata/
        :param open_db:
        Auto-open the database on creation. Default True.
        """
        RwDatabase.__init__(self, path, exchange.lower() + '.db', open_db)

    def ensure_table(self, table: str, cols: Dict[str, str] = {'Date': 'TEXT', 'Open': 'REAL', 'High': 'REAL', 'Low': 'REAL', 'Close': 'REAL', 'Adj Close': 'REAL', 'Volume': 'INTEGER'}) -> None:
        """
        Ensures table exists in the database. If it does not exist, it is created
        """
        RwDatabase.ensure_table(self, table, cols)

    def read_stock_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Obtains the stock data from symbol
        :param symbol:
        The symbol for which data to be obtained
        :param start_date:
        The start date from which to obtain data. If unspecified defaults to oldest entry
        :param end_date:
        The end date from which to obtain data. If unspecified defaults to earliest entry
        :return:
        A pandas DataFrame containing the symbol data
        """
        self._ensure_open()
        if not re.fullmatch('[a-zA-Z0-9.]+', symbol):
            raise ValueError("Symbol Must Be Alphanumeric or '.' and non empty")
        if start_date is not None and not re.fullmatch(r'\d{4}-\d{2}-\d{2}', start_date):
            raise ValueError("Start Date must be in the format yyyy-mm-dd")
        if end_date is not None and not re.fullmatch(r'\d{4}-\d{2}-\d{2}', end_date):
            raise ValueError("End Date must be in the format yyyy-mm-dd")

        if start_date is None and end_date is None:
            res = pd.read_sql('select * from \"{}\";'
                               .format(symbol), self._conn)
        elif start_date is not None and end_date is None:
            res =  pd.read_sql('select * from \"{}\" WHERE date >= ?;'
                               .format(symbol), self._conn, params=(start_date,))
        elif start_date is None and end_date is not None:
            res =  pd.read_sql('select * from \"{}\" WHERE date <= ?;'
                               .format(symbol), self._conn, params=(end_date,))
        else:
            res =  pd.read_sql('select * from \"{}\" WHERE date BETWEEN ? and ?;'
                               .format(symbol), self._conn, params=(start_date, end_date))
        res.set_index('Date', inplace=True)
        return res

    def write_stock_data(self, symbol: str, df: pd.DataFrame, commit: bool = True) -> None:
        """
        Write the new entries from data into the database
        :param symbol:
        The symbol to write into
        :param df:
        Data to write
        :param commit:
        Whether or not to auto commit
        """
        if df.empty:
            return
        self._ensure_open()
        if not symbol.isalnum():
            raise ValueError("Symbol Must Be Alphanumeric")
        self._ensure_table(symbol)
        self._cur.execute('SELECT max(date) from \"{}\"'.format(symbol))
        last_date = self._cur.fetchone()[0]
        self._cur.execute('SELECT min(date) from \"{}\"'.format(symbol))
        first_date = self._cur.fetchone()[0]
        # print(list(data[(data.index >= last_date)].itertuples()))
        if first_date is not None and last_date is not None:
            self._cur.executemany('insert into \"{}\" values  (?,?,?,?,?,?,?)'.format(symbol),
                                  df[(df.index < first_date)].itertuples())
            self._cur.executemany('insert into \"{}\" values  (?,?,?,?,?,?,?)'.format(symbol),
                                  df[(df.index > last_date)].itertuples())
        else:
            self._cur.executemany('insert into \"{}\" values  (?,?,?,?,?,?,?)'.format(symbol), df.itertuples())

        if commit:
            self._conn.commit()


if __name__ == '__main__':
    pass
    # from stock.processers.ma_processor import MovingAverageProcessor
    # with RwDatabase('comdata/', 'Processor.db') as db:
    #     db.ensure_table('A', {'Date': 'TEXT'})
    #     db.write_columns('A', MovingAverageProcessor(5, 'Close').get_data('nyse', 'A'))
