from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime


class PolicyBase(ABC):

    def fit_batch(self, data: pd.DataFrame, start_date: datetime, end_date: datetime):
        for i in range(*data.index.splice_locs(start_date, end_date)):
            print(i)


    def fit_day(self, data: pd.DataFrame, today: datetime):
        pass
