import pandas as pd
from darts import TimeSeries
from darts.models import RNNModel, TCNModel, TransformerModel, NBEATSModel, TiDEModel, TBATS, FFT
from darts.utils.missing_values import fill_missing_values

import os
import logging

log = logging.getLogger()

class StockForecast():
    def __init__(self, output_path):
        self.output_path = output_path
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        self.ticker = None

    def timeseries(self, df):
        df = df.to_pandas()
        df.index = pd.to_datetime(df['Date'])   
        series = TimeSeries.from_dataframe(df, 'Date', 'Adj Close', freq='D', fill_missing_dates=True)
        series = fill_missing_values(series)
        return series
    
    def train(self, time_series):
        for model_name, model in self.models.items():
            model.fit(time_series)
            model.save(os.path.join(self.output_path, f"model_{model_name}_{self.ticker}.pt"))

    def predict(self, predict_days=12):
        predictions = {}
        for model_name, model in self.models.items():
            prediction = model.predict(predict_days)
            predictions[model_name] = prediction
        return predictions
    
    def try_load(self):
        try:
            self.models = {
                "rnn": RNNModel.load(os.path.join(self.output_path, f"model_RNN_{self.ticker}.pt")),
                "tcn": TCNModel.load(os.path.join(self.output_path, f"model_TCN_{self.ticker}.pt")),
                "transformer": TransformerModel.load(os.path.join(self.output_path, f"model_Transformer_{self.ticker}.pt")),
                "nbeats": NBEATSModel.load(os.path.join(self.output_path, f"model_NBEATS_{self.ticker}.pt")),
                "tide": TiDEModel.load(os.path.join(self.output_path, f"model_TiDE_{self.ticker}.pt")),
                "tbats": TBATS.load(os.path.join(self.output_path, f"model_TBATS_{self.ticker}.pt")),
                "fft": FFT.load(os.path.join(self.output_path, f"model_FFT_{self.ticker}.pt")),
            }
        except Exception as e:
            log.error(f"Error loading models: {e}")
            self.create_models()

    def create_models(self):
        self.models = {
            "rnn": RNNModel(input_chunk_length=48, model="LSTM", dropout=0.2, n_epochs=50, random_state=0,
                n_rnn_layers=5,
                training_length=100,
                force_reset=True,
                pl_trainer_kwargs={
                "accelerator": "gpu",
                "devices": [0]
            }),
            "tcn": TCNModel(input_chunk_length=48, output_chunk_length=12, n_epochs=50, random_state=0,
                force_reset=True,
                pl_trainer_kwargs={
                "accelerator": "gpu",
                "devices": [0]
            }),
            "transformer": TransformerModel(input_chunk_length=48, output_chunk_length=12, n_epochs=50, random_state=0,
                d_model=120, nhead=8, num_encoder_layers=4, num_decoder_layers=4, dim_feedforward=1024,
                force_reset=True,
                pl_trainer_kwargs={
                "accelerator": "gpu",
                "devices": [0]
            }),
            "nbeats": NBEATSModel(input_chunk_length=48, output_chunk_length=12, n_epochs=50, random_state=0,
                force_reset=True,
                pl_trainer_kwargs={
                "accelerator": "gpu",
                "devices": [0]
            }),
            "tide": TiDEModel(input_chunk_length=48, output_chunk_length=12, n_epochs=50, random_state=0,
                force_reset=True,
                pl_trainer_kwargs={
                "accelerator": "gpu",
                "devices": [0]
            }),
            "tbats": TBATS(use_trend=True),
            "fft": FFT(trend= "poly",trend_poly_degree=3),
        }

    def plot(self, series, predictions):
        series.plot()
        for model_name, prediction in predictions.items():
            prediction.plot(label=model_name, low_quantile=0.05, high_quantile=0.95)
        plt.legend()
        plt.show()
        
if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from time import sleep
    from libs.db import DB
    from libs.stocks import Stocks
    from libs.config import DATABASE_PATH
    import matplotlib.pyplot as plt

    stocks = Stocks(DB(DATABASE_PATH), "output")
    stocks.get_data_from_api(ticker="PETR3.SA", period='max', insert_db=True)
    df = stocks.get_stock("PETR3.SA", '5y')

    sf = StockForecast("output")
    sf.ticker = "PETR3.SA"
    series = sf.timeseries(df)
    sf.try_load()
    # sf.train(series)
    predictions = sf.predict(12)
    print(predictions)
    sf.plot(series, predictions)
    sleep(1000000)



        
