import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance 
import pytz
import datetime
import threading


def get_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content,'lxml')
    table = soup.find_all('table')[0] 
    df = pd.read_html(str(table))
    tickers = list(df[0]['Symbol'])
    return tickers


def get_ticker_data(ticker, start, end, granularity='1d'):
    df = yfinance.Ticker(ticker).history(

        start=start,
        end=end,
        interval=granularity,
        auto_adjust=True

    ).reset_index()

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
    input(df)


def get_tickers_data(tickers, starts, ends, granularity='1d'):
    dfs = [None] * len(tickers)
    def _get_hist(i):
        df = get_ticker_data(tickers[i], starts[i], ends[i], granularity)
        dfs[i] = df
    
    for i in range(len(tickers)):
        _get_hist(i)
    
    return dfs


tickers = get_tickers()

start_date = datetime.datetime(2010,1,1, tzinfo=pytz.utc)
end_date = datetime.datetime(2020,1,1, tzinfo=pytz.utc)

dfs = get_tickers_data(tickers[:5], [start_date]*5, [end_date]*5)
