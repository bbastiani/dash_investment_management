import sqlite3
import logging
import polars as pl
from datetime import datetime

log = logging.getLogger()

class DB():
    def __init__(self, filename):
        self.filename = filename
        self.conn = sqlite3.connect(self.filename)
        self.cursor = self.conn.cursor()
        log.info(f"Initialize database to file: {self.filename}")

    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                open_price REAL NOT NULL,
                close_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                adj_open_price REAL NOT NULL,
                adj_close_price REAL NOT NULL,
                adj_high_price REAL NOT NULL,
                adj_low_price REAL NOT NULL,
                dividends REAL NOT NULL,
                volume REAL NOT NULL,
                stock_splits REAL NOT NULL,
                date TEXT NOT NULL
            )
        ''')
        self.conn.commit()
        log.info("Created table stocks")
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS portifolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                number_of_stocks INTEGER NOT NULL,
                price_at_buy REAL NOT NULL,
                date TEXT NOT NULL
            )
        ''')
        self.conn.commit()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_download (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                download_date TEXT NOT NULL,
                last_update TEXT NOT NULL,
                download_all_period TEXT NOT NULL
            )
        ''')
        self.conn.commit()
        log.info("Created table stocks")
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS forecast (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                forecast_date TEXT NOT NULL,
                price REAL NOT NULL
            )
        ''')
        self.conn.commit()
        log.info("Created table forecast")

    def insert_stock(self, ticker, open_price, close_price, high_price, low_price, dividend, date):
        self.cursor.execute('''
            INSERT INTO stocks (ticker, open_price, close_price, high_price, low_price, adj_open_price, adj_close_price, adj_high_price, adj_low_price, dividends, volume, stock_splits, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ticker, open_price, close_price, high_price, low_price, dividend, date))
        self.conn.commit()
        log.info(f"Inserted stock {ticker} into database")

    def bulk_insert(self, data):
        self.cursor.executemany('''
            INSERT INTO stocks (ticker, open_price, close_price, high_price, low_price, adj_open_price, adj_close_price, adj_high_price, adj_low_price, dividends, volume, stock_splits, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
        self.conn.commit()
        log.info(f"Inserted {len(data)} stocks into database")

        self.remove_duplicates()

    def get_stock(self, ticker, min_date):
        self.cursor.execute('''
            SELECT * FROM stocks WHERE ticker = ? AND date > ?
        ''', (ticker, min_date))
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Open", pl.Float64), ("Close", pl.Float64), ("High", pl.Float64), 
                    ("Low", pl.Float64), ("Adj Open", pl.Float64), ("Adj Close", pl.Float64), ("Adj High", pl.Float64), 
                    ("Adj Low", pl.Float64), ("Dividends", pl.Float64), ("Volume", pl.Float64), ("Stock Splits", pl.Float64), ("Date", pl.Utf8)]                 
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)
    
    def get_all_stocks(self):
        self.cursor.execute('''
            SELECT * FROM stocks
        ''')
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Open", pl.Float64), ("Close", pl.Float64), ("High", pl.Float64), 
                    ("Low", pl.Float64), ("Adj Open", pl.Float64), ("Adj Close", pl.Float64), ("Adj High", pl.Float64), 
                    ("Adj Low", pl.Float64), ("Dividends", pl.Float64), ("Volume", pl.Float64), ("Stock Splits", pl.Float64), ("Date", pl.Utf8)]                 
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)
    
    def get_stocks_by_timerange(self, ticker, min_date, max_date):
        self.cursor.execute('''
            SELECT * FROM stocks WHERE ticker = ? AND date >= ? AND date <= ?
        ''', (ticker, min_date, max_date))
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Open", pl.Float64), ("Close", pl.Float64), ("High", pl.Float64), 
                    ("Low", pl.Float64), ("Adj Open", pl.Float64), ("Adj Close", pl.Float64), ("Adj High", pl.Float64), 
                    ("Adj Low", pl.Float64), ("Dividends", pl.Float64), ("Volume", pl.Float64), ("Stock Splits", pl.Float64), ("Date", pl.Utf8)]                 
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)

    def get_min_max_date(self, ticker=None):
        if ticker is not None:
            self.cursor.execute('''
                SELECT MIN(date), MAX(date) FROM stocks WHERE ticker = ?
            ''', (ticker,))
        else:
            self.cursor.execute('''
                SELECT MIN(date), MAX(date) FROM stocks
            ''')
        min_date, max_date = self.cursor.fetchone()
        log.info(f"Get min and max date in database: {min_date}, {max_date}")
        return datetime.strptime(min_date, "%Y-%m-%d %H:%M:%S"), datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
    
    def get_stocks_ticker(self):
        self.cursor.execute('''
                SELECT DISTINCT(ticker) FROM stocks
            ''')
        return  self.cursor.fetchall()
    
    def get_stock_download_info(self, ticker):
        self.cursor.execute('''
                SELECT * FROM stock_download WHERE ticker = ?
            ''', (ticker,))
        return  pl.DataFrame(self.cursor.fetchall(), schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Download Date", pl.Utf8), ("Last Update", pl.Utf8), ("Download All Period", pl.Utf8)])

    def get_stocks_download_info(self):
        self.cursor.execute('''
                SELECT * FROM stock_download
            ''')
        return  pl.DataFrame(self.cursor.fetchall(), schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Download Date", pl.Utf8), ("Last Update", pl.Utf8), ("Download All Period", pl.Utf8)])

    def update_stock_download_info(self, ticker, download_date, last_update, download_all_period):
        self.cursor.execute('''
            INSERT INTO stock_download (ticker, download_date, last_update, download_all_period)
            VALUES (?, ?, ?, ?)
        ''', (ticker, download_date, last_update, download_all_period))
        self.conn.commit()
        log.info(f"Updated stock {ticker} download info")

    def insert_stock_download_info(self, ticker, download_date, last_update, download_all_period):
        #check if already exists
        self.cursor.execute('''
            SELECT * FROM stock_download WHERE ticker = ?
        ''', (ticker,))
        data = self.cursor.fetchall()
        if len(data) > 0:
            self.update_stock_download_info(ticker, download_date, last_update, download_all_period)
            return
        
        self.cursor.execute('''
            INSERT INTO stock_download (ticker, download_date, last_update, download_all_period)
            VALUES (?, ?, ?, ?)
        ''', (ticker, download_date, last_update, download_all_period))
        self.conn.commit()
        log.info(f"Inserted stock {ticker} download info")

    def insert_forecast(self, ticker, date, price):
        forecast_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cursor.execute('''
            INSERT INTO forecast (ticker, date, forecast_date, price)
        ''', (ticker, date, forecast_date, price))
        self.conn.commit()
        log.info(f"Inserted stock {ticker} into forecast database")

    def bulk_insert_forecast(self, data):
        self.cursor.executemany('''
            INSERT INTO forecast (ticker, date, forecast_date, price)
            VALUES (?, ?, ?, ?)
        ''', data)
        self.conn.commit()
        log.info(f"Inserted {len(data)} stocks into forecast database")

    def get_all_forecast(self):
        self.cursor.execute('''
            SELECT * FROM forecast
        ''')
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Date", pl.Utf8), ("Forecast Date", pl.Utf8), ("Price", pl.Float64)]
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("Forecast Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)
    
    def get_forecast_by_ticker(self, ticker, last_forecast=True):
        if last_forecast:
            self.cursor.execute('''
                SELECT * FROM forecast WHERE ticker = ? AND
                date = (SELECT MAX(date) FROM forecast WHERE ticker = ?)
            ''', (ticker,))
        else:
            self.cursor.execute('''
                SELECT * FROM forecast WHERE ticker = ?
            ''', (ticker,))
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Date", pl.Utf8), ("Forecast Date", pl.Utf8), ("Price", pl.Float64)]
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("Forecast Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)


    def sort_by_date(self, df):
        return df.sort("Date", descending=False)

    def remove_duplicates(self):
        self.cursor.execute('''
            DELETE FROM stocks WHERE id NOT IN (
                SELECT MAX(id) FROM stocks GROUP BY ticker, date
            )
        ''')
        self.conn.commit()
        log.info("Removed duplicates from database")

    def insert_portifolio(self, ticker, number_of_stocks, price_at_buy, date):
        self.cursor.execute('''
            INSERT INTO portifolio (ticker, number_of_stocks, price_at_buy, date)
            VALUES (?, ?, ?, ?)
        ''', (ticker, number_of_stocks, price_at_buy, date))
        self.conn.commit()
        log.info(f"Inserted stock {ticker} into portifolio")

    def delete_from_portifolio(self, ticker):
        self.cursor.execute('''
            DELETE FROM portifolio WHERE ticker = ?
        ''', (ticker,))
        self.conn.commit()
        log.info(f"Deleted stock {ticker} from portifolio")

    def get_portifolio(self):
        self.cursor.execute('''
            SELECT * FROM portifolio
        ''')
        data = self.cursor.fetchall()
        df = pl.DataFrame(data, 
            schema=[("id", pl.Int64), ("Ticker", pl.Utf8), ("Number of Stocks", pl.Int64), ("Price at Buy", pl.Float64), ("Date", pl.Utf8)]
        )
        df = df.with_columns(
            pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S")
        )
        return self.sort_by_date(df)

    def close(self):
        self.conn.close()
        log.info("Closed database connection")

