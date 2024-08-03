import yfinance as yf
import logging
import polars as pl
from datetime import datetime, timedelta

log = logging.getLogger()

def get_historical_data(ticker, period):
    try:
        yf.Ticker(ticker).info
    except Exception as e:
        log.error(f"Ticker was not found {ticker}, please check if value is correct")
        log.error(f"ERROR MESSAGE: \n{e}")
        return None
    df = pl.from_pandas(yf.Ticker(ticker).history(period=period).reset_index())
    df = df.with_columns(
        pl.col("Date").dt.replace_time_zone(None)
    ).rename(
        {"Open": "Adj Open", "Close": "Adj Close", "High": "Adj High", "Low": "Adj Low"}
    )
    df = df.select(["Adj Open", "Adj Close", "Adj High", "Adj Low", "Date", "Dividends", "Stock Splits"])
    df1 = get_data_adj(ticker, period)
    df = df.join(df1, on="Date", how="inner")

    return df

def get_data_adj(ticker, period):
    period = period_to_days(period)
    start_date = (datetime.now() - timedelta(days=period)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    df = pl.from_pandas(yf.download(ticker, start=start_date, end=end_date).reset_index())

    # df = df.with_columns(
    #     pl.col("Close")
    #     .truediv(pl.col("Adj Close"))
    #     .alias("factor_adj")
    # )
    # df = df.with_columns(
    #     pl.col("Open")
    #     .mul(pl.col("factor_adj"))
    #     .alias("Adj Open")
    # )
    # df = df.with_columns(
    #     pl.col("High")
    #     .mul(pl.col("factor_adj"))
    #     .alias("Adj High")
    # )
    # df = df.with_columns(
    #     pl.col("Low")
    #     .mul(pl.col("factor_adj"))
    #     .alias("Adj Low")
    # )

    return df

def get_factor_adj(column, column_close_adj):
    return column_close_adj / column

def set_price_with_factor_adj(column, factor_adj):
    return column * factor_adj

def days_to_period(day):
    days = [1, 5, 30, 90, 180, 365, 730, 1825, 3650, 0]
    period = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"]

    closest_day = min(days, key=lambda x:abs(x-day))
    return period[days.index(closest_day)]

def period_to_days(period):
    if period == "1d":
        return 1
    elif period == "5d":
        return 5
    elif period == "1mo":
        return 30
    elif period == "3mo":
        return 90
    elif period == "6mo":
        return 180
    elif period == "1y":
        return 365
    elif period == "2y":
        return 730
    elif period == "5y":
        return 1825
    elif period == "10y":
        return 3650
    elif period == "ytd":
        return (datetime.now() - datetime(datetime.now().year, 1, 1)).days
    elif period == "max":
        return 3650*10
    else:
        return None
        
