from stock.database import MetadataDatabase, ExchangeDatabase
from typing import Optional
import fix_yahoo_finance as yf
from itertools import repeat
import datetime as dt
import time
import pandas as pd
from multiprocessing.dummy import Pool
from multiprocessing import Process, Manager, Queue


def update_database(exchange: str, start_date: str = None, threads: int = 1, multiprocess_write: bool = False, verbose: bool = False) -> None:
    with MetadataDatabase() as metadb:
        metadata = metadb.get_exchange_metadata(exchange)
        if metadata is None:
            raise ValueError("Exchange Not Found In Database")
        ext = metadata[1]
        if start_date is None:
            start_date = metadata[2]
        if start_date is None:
            start_date = '1970-01-01'
        end_date = dt.datetime.today().strftime('%Y-%m-%d')
        start = int(time.mktime(time.strptime(str(start_date), '%Y-%m-%d')))
        end = int(time.mktime(time.strptime(str(end_date), '%Y-%m-%d')))
        if start >= end:
            return
        symbols = metadb.get_symbols(exchange)
        if threads is None or threads < 2:
            with ExchangeDatabase(exchange) as exdb:
                for item in zip(symbols, map(lambda *args: _download(*args, start=start, end=end, verbose=verbose), map(''.join, zip(symbols, repeat('.' + ext))) if ext else list(symbols))):
                    exdb.write_stock_data(*item, commit=False)
        else:
            q = Manager().Queue()
            if multiprocess_write:
                writer = Process(target=_write_queue, args=(q, exchange, ext, verbose))
                writer.start()
            with Pool(processes=threads) as pool:
                pool.starmap(lambda *args: _download(*args, start=start, end=end, q=q, verbose=verbose), zip(map(''.join, zip(symbols, repeat('.' + ext))) if ext else list(symbols)))
                pool.close()
                pool.join()
            q.put(('Task Done', ''))

            if multiprocess_write:
                writer.join()
            else:
                _write_queue(q, exchange, ext, verbose)

        metadb.write_exchange_update_date(exchange, dt.datetime.today().strftime('%Y-%m-%d'))


def _write_queue(q: Queue, exchange: str, ext: str, verbose: bool = False) -> None:
    with ExchangeDatabase(exchange) as exdb:
        while True:
            # try:
                symbol, data = q.get()
                if symbol == 'Task Done':
                    break
                if ext:
                    symbol = symbol[:-len(ext)-1]
                if verbose:
                    print(f"Writing {symbol}")
                exdb.write_stock_data(symbol, data, commit=False)
                q.task_done()
            # except Exception as e:
            #     print(e)
        if verbose:
            print("Committing")


def _download(symbol: str, start: int, end: int, q: Queue = None,
              interval: str = '1d', attempts: int = 5, verbose: bool = False) -> Optional[pd.DataFrame]:
    force = False
    if verbose:
        print(f"Downloading {symbol}")
    for i in range(attempts):
        try:
            yf.get_yahoo_crumb(force=force)
            hist = yf.download_one(symbol, start, end, interval)
            if isinstance(hist, pd.DataFrame):
                if not hist.empty:
                    hist.reset_index(inplace=True)
                    hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
                    hist.set_index('Date', inplace=True)
                if q is not None:
                    q.put((symbol, hist))
                    return
                else:
                    return hist
        except:
            force = True
            if verbose:
                print(f"Reattempting {symbol}. Attempt {i+1}")
            continue


if __name__ == '__main__':
    import timeit
    print(timeit.timeit("update_database('nyse', start_date=None, threads=20, multiprocess_write=True, verbose=True)", number=1, globals=globals()))
