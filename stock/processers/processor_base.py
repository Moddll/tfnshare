from __future__ import annotations
from stock.data import data_manager
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Dict, Iterable, Optional
from stock.data.database import RwDatabase
import pandas as pd
import atexit


class ProcessorBase(ABC):
    """
    Base for a Processor. Objects of subclasses created with the same parameters
    for __init__ will refer to the same object.

    Initializing Code in __init__ should only be ran if initialized == False
    """
    objs: Dict[tuple, ProcessorBase] = None
    data: Dict[str, pd.DataFrame]
    initialized: bool

    def __init__(self):
        if not self.initialized:
            self.initialized = True
            self.data = {}
            cls = self.__class__
            if not hasattr(cls, 'registered_cleanup') or not cls.registered_cleanup:
                cls.registered_cleanup = True
                atexit.register(cls._clean_up)

    def get_data(self, exchange: str, symbol: str, start_date: str = '0000-00-00', end_date: str = '9999-99-99') -> pd.DataFrame:
        """
        Get the computed data of symbol at exchange. Stock data is directly obtained from data_manager
        :param start_date: Start of data
        :param end_date: End of data
        :return:
        A pandas DataFrame containing the data
        """
        tblname = exchange + '/' + symbol
        if tblname not in self.data:
            self.data[tblname] = self.compute(data_manager.get_data(exchange, symbol))
        df = self.data[tblname]
        return df[(start_date <= df.index) & (df.index <= end_date)]

    def get_data_multi(self, symbols: Dict[str, Iterable[str]], start_date: str = '0000-00-00',
                       end_date: str = '9999-99-99') -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get the computed data of symbols. Stock data is directly obtained from data_manager
        :param symbols:
        A dictionary mapping exchanges to lists of symbols from that exchange
        :param start_date: Start of data
        :param end_date: End of data
        :return:
        A dictionary mapping exchange to data frames
        """
        res = {}
        for exchange, symbol_list in symbols.items():
            res[exchange] = {symbol: self.get_data(exchange, symbol, start_date, end_date) for symbol in symbol_list}
        return res

    @abstractmethod
    def compute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Computes on data
        :return:
        A pandas DataFrame containing the result
        """
        pass

    def __new__(cls, *args, **kwargs) -> ProcessorBase:
        """
        If an object with the same hashable args have not been created before, it is created. Otherwise
        the same object is returned.
        """
        hashable_args = tuple(map(lambda x: x if isinstance(x, Hashable) else None, args))
        if cls.objs is None:
            cls.objs = {}
        if hashable_args not in cls.objs:
            cls.objs[hashable_args] = object.__new__(cls)
            cls.objs[hashable_args].initialized = False
        return cls.objs[hashable_args]

    @classmethod
    def _clean_up(cls) -> None:
        """
        Can be overwritten to customize behavior on program exit
        """
        pass


class BufferedProcessorBase(ProcessorBase, ABC):
    """
    Variant of Processor Base that will store data to file on program exit to
    avoid recomputation
    """
    database: RwDatabase = None

    def __init__(self, db_path: str = 'comdata/', db_name: str = None):
        """
        Creates a BufferedProcessorBase
        :param db_path:
        Path to database. Defaults to 'comdata/'.
        :param db_name:
        Name of  database. Defaults to name of the class.
        """
        if not self.initialized:
            ProcessorBase.__init__(self)
            if not db_name:
                db_name = self.__class__.__name__ + '.db'
            cls = self.__class__
            cls.database = RwDatabase(db_path, db_name)

    def get_data(self, exchange: str, symbol: str, start_date: str = '0000-00-00', end_date: str = '9999-99-99') -> pd.DataFrame:
        """
        Get the computed data of symbol at exchange. Stock data is directly obtained from data_manager
        :param start_date: Start of data
        :param end_date: End of data
        :return:
        A pandas DataFrame containing the data
        """
        tblname = exchange + '/' + symbol
        if tblname not in self.data:
            if self.database.have_table(tblname):
                self.data[tblname] = self._read(self.database, tblname)
            if tblname not in self.data or self.data[tblname] is None:
                self.data[tblname] = self.compute(data_manager.get_data(exchange, symbol))
        df = self.data[tblname]
        return df[(start_date <= df.index) & (df.index <= end_date)]

    @abstractmethod
    def _read(self, db: RwDatabase, tblname: str) -> Optional[pd.DataFrame]:
        """
        Read the stored data from db and table tblname
        :return:
        A pandas DataFrame containing the data or None if the table
        does not have the data
        """
        pass

    @abstractmethod
    def _write(self, db: RwDatabase, tblname: str, data: pd.DataFrame) -> None:
        """
        Write data into the table tblname of db.

        Precondition:
        table tblname exists in db
        """
        pass

    def _write_all(self, db: RwDatabase) -> None:
        """
        Write all data that need to be stored into db
        """
        for tblname, data in self.data.items():
            db.ensure_table(tblname, {'Date': 'TEXT'})
            self._write(db, tblname, data)

    @classmethod
    def _clean_up(cls) -> None:
        """
        Can be overwritten to customize behavior on program exit
        """
        for a in cls.objs.values():
            obj: BufferedProcessorBase = a
            obj._write_all(cls.database)
        if hasattr(cls, 'database') and isinstance(cls.database, RwDatabase):
            cls.database.close()


if __name__ == '__main__':
    pass
