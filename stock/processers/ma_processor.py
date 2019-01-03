from stock.processers.processor_base import BufferedProcessorBase
import pandas as pd
from typing import Union, List, Any, Dict, Optional
from stock.data.database import RwDatabase


class MovingAverageProcessor(BufferedProcessorBase):
    days: Union[str, int]
    column: str
    args: List[Any]
    kwargs: Dict[str, Any]

    def __init__(self, days: Union[str, int], column: str, *args, **kwargs):
        """
        Calculates the moving average on column
        Additional Arguments/Keyword Arguments are passed to pd.DataFrame.rolling
        :param days: Window for average
        :param column: The column to computer for. Must be one of
        Open
        High
        Low
        Close
        Adj. Close
        Volume
        """
        if not self.initialized:
            BufferedProcessorBase.__init__(self)
            self.days = days
            self.column = column
            self.args = args
            self.kwargs = kwargs

    def compute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Computes on data
        :return:
        A pandas DataFrame containing the result
        """
        return data[self.column].rolling(self.days, *self.args, **self.kwargs).mean().to_frame().rename(columns={"Close": f"ma{self.days}_{self.column.lower()}"})

    def _read(self, db: RwDatabase, tblname: str) -> Optional[pd.DataFrame]:
        """
        Read the stored data from db and table tblname
        :return:
        A pandas DataFrame containing the data or None if the table
        does not have the data
        """
        if db.have_column(tblname, f"ma{self.days}_{self.column.lower()}"):
            res = db.read_column(tblname, ['Date', f"ma{self.days}_{self.column.lower()}"])
            res.set_index('Date', inplace=True)
            return res

    def _write(self, db: RwDatabase, tblname: str, data: pd.DataFrame) -> None:
        """
        Write data into the table tblname of cur if needed

        Precondition:
        table tblname exists
        """
        db.write_columns(tblname, data)


if __name__ == '__main__':
    print(MovingAverageProcessor(5, 'Close').get_data('nyse', 'A'))
    MovingAverageProcessor(5, 'Close').get_data('nyse', 'A')
