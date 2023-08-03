import pandas as pd
import yfinance as yf
import requests
import pandas as pd

def access_url(url, logger):
    logger.info("Accessing BankOfEngland endpoint ...")
    headers = {
        'User-Agent': 'Mozilla'
    }
    s = requests.Session()
    s.headers.update(headers)
    r = s.get(url)
    if r.status_code >= 300:
        logger.error(f"Unable to access BankOfEngland endpoint. Status code {r.status_code} .")
        raise Exception(r.reason)

def get_data_from_csv(url):
    return pd.read_csv(url)

def get_interest_rates(previous_date, date_today, logger):
    logger.info("Converting dates to get data from BankOfEngland endpoint ...")
    prev_month = previous_date.strftime("%b")
    prev_day = f"{previous_date.day:02d}"
    prev_year = previous_date.year
    curr_month = date_today.strftime("%b")
    curr_day = f"{date_today.day:02d}"
    curr_year = date_today.year

    url = rf"https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes&Datefrom={prev_day}%2F{prev_month}%2F{prev_year}&Dateto={curr_day}%2F{curr_month}%2F{curr_year}&SeriesCodes=IUMSOIA&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
    access_url(url, logger)
    logger.info("Getting interest rate data from BankOfEngland endpoint and zipping into tuples ...")
    interest_rates = get_data_from_csv(url)
    return list(zip(interest_rates.DATE, interest_rates.IUMSOIA))
    
def get_spy_price(logger):
    logger.info("Getting SPY data from the last 5 days using yfinance ...")
    ticker = yf.Ticker("SPY")
    sp500 = ticker.history(period="5d")

    logger.info("Selecting the most recent closing price and zipping into a tuple ...")
    sp500_now = sp500[sp500.index == max(sp500.index)]
    return [(date_time.strftime("%Y-%m-%d"), close) for (date_time, close) in zip(sp500_now.index.date, round(sp500_now['Close'], 5))]