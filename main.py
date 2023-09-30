import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance 
import pytz
import datetime
import threading
from utils import save_pickle, load_pickle


def get_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content,'lxml')
    table = soup.find_all('table')[0] 
    df = pd.read_html(str(table))
    tickers = list(df[0]['Symbol'])
    return tickers


def get_ticker_data(ticker, start, end, granularity='1d', tries=0):

    try:
        df = yfinance.Ticker(ticker).history(

            start=start,
            end=end,
            interval=granularity,
            auto_adjust=True

        ).reset_index()
    except Exception as err:
        if tries < 5:
            return get_ticker_data(ticker, start, end, granularity, tries+1)
        return pd.DataFrame()

    df = df.rename(columns={

        "Open":"open",
        "High":"high",
        "Low":"low",
        "Date":"datetime",
        "Close":"close",
        "Volume":"volume"
    })

    if df.empty:
        return pd.DataFrame()
    
    df['datetime'] = df['datetime'].dt.tz_convert('UTC')
    df = df.drop(columns=['Dividends', 'Stock Splits'])
    df = df.set_index('datetime', drop=True)
    
    return df


def get_tickers_data(tickers, starts, ends, granularity='1d'):
    dfs = [None] * len(tickers)
    def _get_hist(i):
        print(tickers[i])
        df = get_ticker_data(tickers[i], starts[i], ends[i], granularity)
        dfs[i] = df
    
    threads = [threading.Thread(target=_get_hist, args=(i,)) for i in range(len(tickers))]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]

    return tickers, dfs

def get_ticker_dfs(start, end):
    try:
        tickers, ticker_dfs = load_pickle("dataset.obj")
    except Exception as err:    
        tickers = get_tickers()
        starts = [start]*len(tickers)
        ends = [end]*len(tickers)
        tickers, dfs = get_tickers_data(tickers, starts, ends, granularity="1d")
        ticker_dfs = {ticker:df for ticker,df in zip(tickers, dfs)}
        save_pickle("dataset.obj", (tickers, ticker_dfs))

    return tickers, ticker_dfs

start_date = datetime.datetime(2010,1,1, tzinfo=pytz.utc)
end_date = datetime.datetime.now(pytz.utc)

dfs = get_ticker_dfs(start_date, end_date)
print(len(dfs))

