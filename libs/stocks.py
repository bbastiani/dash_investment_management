# Description: This file contains the Stocks class which is used to interact with the database to get stock data.
from datetime import datetime, timedelta
from libs.finance import get_historical_data, period_to_days, days_to_period
from libs.price_prediction import StockForecast
import polars as pl
import logging

log = logging.getLogger()

class Stocks():
    def __init__(self, db, output_path):
        self.db = db
        self.db.create_tables()
        self.all_stocks = None
        self.forecast = StockForecast(output_path)

    def add_stocks(self, ticker):
        log.info(f"Add {ticker} to database")
        _ = self.get_data_from_api(ticker, 'max')

    def list_stocks(self) -> list:
        stocks = self.db.get_stocks_ticker()
        log.info(f"List all stocks in database: {stocks}")
        return stocks
    
    def update_stocks(self):
        stocks = self.list_stocks()
        for i, stock in enumerate(stocks):
            _ = self.get_data_from_api(stock, 'max')
            yield i+1, len(stocks)

    def get_stocks(self, ticker, period=0) -> pl.DataFrame:
        if isinstance(ticker, list) or isinstance(ticker, tuple):
            df = [self.get_stock(t, period) for t in ticker]
            return pl.concat(df, how="vertical")
        
        return self.get_stock(ticker, period)
    
    def insert_portifolio(self, ticker, number_of_stocks, price_at_buy, date) -> None:
        log.info(f"Add {ticker} to portifolio")
        self.db.insert_portifolio(ticker, number_of_stocks, price_at_buy, date)
    
    def get_portifolio(self) -> pl.DataFrame:
        log.info(f"Get portifolio")
        return self.db.get_portifolio()
    
    def delete_stock_from_portifolio(self, ticker) -> None:
        log.info(f"Delete {ticker} from portifolio")
        self.db.delete_from_portifolio(ticker)

    def get_stock(self, ticker, period=0, search_api=True) -> pl.DataFrame:
        log.info(f"Get stock {ticker} for period {period}")
        min_date, _ = self.db.get_min_max_date(ticker)
        min_period_date = datetime.now() - timedelta(days=period_to_days(period)) 
        
        download_info = self.db.get_stock_download_info(ticker)

        if download_info is None or download_info.is_empty():
            log.info(f"Stock {ticker} not found in database, fetch data from yfinance api")
            return self.get_data_from_api(ticker, period)        
        
        if (min_date is None or min_period_date < min_date) and download_info.select("Download All Period").item() == "NO":
            if search_api:
                log.info(f"Stock {ticker} no found in database, fetch data from yfinance api")
                self.get_data_from_api(ticker, period)
            else:
                log.info(f"Stock {ticker} not found in database,  search in API is disable, return database values")

        return self.db.get_stock(ticker, min_period_date.strftime("%Y-%m-%d"))
    
    def get_all_stocks(self) -> pl.DataFrame:
        return self.db.get_all_stocks()
    
    def get_stocks_by_timerange(self, ticker: str, min_date: datetime, max_date: datetime) -> pl.DataFrame:
        log.info(f"Get stock {ticker} for period {min_date} -> {max_date}")
        min_date_db, _ = self.db.get_min_max_date(ticker)
        if min_date_db is None or min_date_db > min_date:
            log.info(f"Stock {ticker} no found in database, fetch data from yfinance api")
            _ = self.get_data_from_api(ticker, period='max')
        
        return self.db.get_stocks_by_timerange(ticker, min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'))

    def get_data_from_api(self, ticker: str, period: str, insert_db=True) -> pl.DataFrame:
        data = get_historical_data(ticker, period)
        if data is not None and not data.is_empty():
            df = data.with_columns(
                pl.lit(ticker).alias("Ticker")
            ).with_columns(
                pl.col("Date").dt.strftime("%Y-%m-%d %H:%M:%S")
            ).select(["Ticker", "Open", "Close", "High", "Low", "Adj Open", "Adj Close", "Adj High", "Adj Low", "Dividends", "Volume", "Stock Splits", "Date"]).fill_null(0)
            if insert_db:
                data = df.to_numpy()
                self.db.bulk_insert(data.tolist())
                self.db.insert_stock_download_info(
                    ticker, 
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "YES" if period == "max" else "NO"
                )
            return df
        else:
            log.warn(f"Stock {ticker} not found, database doens't contains this stock or yfinance api can not find this ticker")
            return None

    def _get_statistics(self, data) -> dict:
        if data.is_empty() or data is None:
            return {
                "Start_date": 0,
                "End_date": 0,
                "Dividends": 0,
                "Volume": 0,
                "High": 0,
                "Low": 0,
                "Open": 0,
                "Close": 0,
                "Dividend_yield": 0,
                "Price_variation": 0
            }        

        close = data.select(pl.last("Close")).item()
        dividend_yield = data.select('Dividends').sum().item() / data.select('Close').mean().item()
        if close != 0:
            price_variation = ((close - data.select(pl.first("Close")).item()) / close )* 100
        else: 
            price_variation = 0
        return {
            "Start_date": data.select(pl.first("Date")).item(),
            "End_date": data.select(pl.last("Date")).item(),
            "Dividends": data.select('Dividends').sum().item(),
            "Volume": data.select('Volume').mean().item(),
            "High": data.select('High').max().item(),
            "Low": data.select('Low').min().item(),
            "Open": data.select(pl.first("Open")).item(),
            "Close": close,
            "Dividend_yield": dividend_yield,
            "Price_variation": price_variation
        }

    def get_statistics_all_periods(self, ticker,  periods=["3mo", "6mo", "1y", "2y", "5y"], return_dict=False) -> pl.DataFrame:
        statistics = []
        if self.all_stocks is None:
            self.all_stocks = self.get_all_stocks()
        for period in periods:
            min_period_date = datetime.now() - timedelta(days=period_to_days(period)) 
            df = self.all_stocks.filter((pl.col("Date") > min_period_date) & (pl.col('Ticker') == ticker))
            stats = self._get_statistics(df)
            stats['Period'] = period
            statistics.append(stats)

        if return_dict:
            return statistics
        return pl.from_dicts(statistics)

    def get_statistics_by_period(self, ticker, period) -> pl.DataFrame:
        data = self.get_stock(ticker, period)
        return self._get_statistics(data)
    
    def get_statistics_by_year(self, ticker):
        end_date = datetime.now().replace(month=12, day=31, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        start_time = end_date - timedelta(days=365*6)
        time_range = pl.date_range(start=start_time, end=end_date, freq='1y', eager=True).to_numpy('datetime64[ns]').tolist()
        statistics = []

        for start, end in zip(time_range, time_range[1:]):
            data = self.get_stocks_by_timerange(ticker, start, end)
            statistics.append(self._get_statistics(data))

        return pl.from_dicts(statistics)
    
    def get_statistics_by_buy_date(self, ticker, buy_date, price_at_buy=None, return_dict=False) -> pl.DataFrame:
        end_date = datetime.now().replace(month=12, day=31, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        start_time = buy_date
        
        data = self.get_stocks_by_timerange(ticker, start_time, end_date)
        stats = self._get_statistics(data)
        # replace start_date with buy_date and price_variation with price_variation from buy_date
        stats["Date"] = buy_date
        if price_at_buy is not None:
            stats["Price_variation"] = ((stats["Close"] - price_at_buy) / price_at_buy )
        if return_dict:
            return stats
        return pl.from_dicts(stats)
    
    def get_monthly_portifolio_statistics(self) -> pl.DataFrame:
        portifolio = self.get_portifolio()
        statistics = []
        for stock in portifolio.iter_rows(named=True):
            days = (datetime.now() - stock['Date']).days
            dividend = self.get_monthly_dividends(stock['Ticker'], days_to_period(days))
            close = self.get_monthly_close_price(stock['Ticker'], days_to_period(days))
            
            close[0, "Close"] = stock['Price at Buy'] # add buy price to take a diff
            close = close.with_columns(
                pl.col("Close").diff(null_behavior="ignore").alias("Close Diff")
            ).fill_null(0)          

            data = close.join(dividend, on="Date", how="left")
            data = data.with_columns(
                pl.col("Close").sub(pl.lit(stock['Price at Buy'])).mul(stock['Number of Stocks']).alias("Price Variation")
            ).with_columns(
                pl.col("Close Diff").mul(stock['Number of Stocks']).alias("Price Variation Diff")
            )
            data = data.with_columns(
                pl.lit(stock['Number of Stocks']).mul(pl.col("Dividends")).alias("Dividends")
            )
            data = data.with_columns(
                pl.lit(stock['Ticker']).alias("Ticker")
            )
            statistics.append(data)

        return pl.concat(statistics, how="vertical")


    def get_monthly_dividends(self, ticker, period)  -> pl.DataFrame:
        data = self.get_stock(ticker, period)
        if data is None or data.is_empty():
            return 0
        return data.group_by_dynamic(
            "Date", every="1mo", period="1mo", closed="right"
        ).agg(pl.sum("Dividends")).select("Date", "Dividends")
    
    def get_monthly_close_price(self, ticker, period)  -> pl.DataFrame:
        data = self.get_stock(ticker, period)
        if data is None or data.is_empty():
            return 0
        return data.group_by_dynamic(
            "Date", every="1mo", period="1mo", closed="right"
        ).agg(pl.last("Close")).select("Date", "Close")

    def adj_stock_price(self, df) -> pl.DataFrame:
        # https://www.bussoladoinvestidor.com.br/ajustar-o-historico-de-precos-de-acoes/
        # O valor do dividendo é subtraído do valor do dia anterior,
        # O resultado é dividido pelo preço do dia anterior
        # Os preços históricos são multiplicados por este fator
        
        df = df.with_columns(
            pl.when(pl.col("Dividends") > 0)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .shift(-1, fill_value=0)
            .alias("Mask")
        )
        df = df.with_columns(
            pl.col("Close")
            .sub(pl.col("Dividends")
            .shift(-1, fill_value=0))
            .truediv(pl.col("Close"))
            .mul(pl.col("Mask"))
            .alias("Ajuste")
        )
        df = df.with_columns(
            pl.when(pl.col("Ajuste") > 0)
            .then(pl.col("Ajuste"))
            .otherwise(None)
            .backward_fill()
        )
        df = df.with_columns(
            pl.when(pl.col("Ajuste").is_not_null())
            .then(pl.col("Close").mul(pl.col("Ajuste")))
            .otherwise(pl.col("Close"))
            .alias("Close_Ajustado")
        )
        return df
    
    def train_models(self):
        portifolio = self.get_portifolio()
        for stock in portifolio.iter_rows(named=True):
            data = self.get_stock(stock['Ticker'], 'max')
            series = self.forecast.timeseries(data)
            self.forecast.train(series)

    def forecast_stock(self, ticker, days=12) -> dict:
        self.forecast.ticker = ticker
        pred = self.forecast.predict(days)
        print(pred)
        return pred
    
    def load_forecast(self, ticker):
        return self.db.get_forecast_by_ticker(ticker)

