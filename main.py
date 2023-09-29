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



tickers = get_tickers()

start_date = datetime.datetime(2010,1,1, tzinfo=pytz.utc)
end_date = datetime.datetime(2020,1,1, tzinfo=pytz.utc)
print(get_ticker_data(tickers[0], start_date, end_date))