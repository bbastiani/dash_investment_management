from sklearnex import patch_sklearn
patch_sklearn()

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from datetime import timedelta
import polars as pl
import numpy as np
import pickle as pkl
import os

class LinearRegressionModel():
    def __init__(self, regression_model, model_path, predict_days, **kwargs):
        if regression_model == "linear":
            self.model = LinearRegression()
        elif regression_model == "ridge":
            self.model = Ridge(alpha=kwargs.get('alpha', 0.1))
        elif regression_model == "lasso":
            self.model = Lasso(alpha=kwargs.get('alpha', 0.1))
        
        self.scaler = StandardScaler()
        self.model_path = model_path
        self.predict_days = predict_days
        self.mse = None

    def scale(self, X, return_dataframe=False):
        if isinstance(X, pl.DataFrame):
            X = X.with_columns(
                pl.col("Date").dt.epoch("d").alias("Date")
            ).to_numpy()
        
        X = self.scaler.transform(X)
        if return_dataframe:
            return pl.DataFrame({"Date": X[:,0], "Adj Close": X[:,1]})
        return X
    
    def preprocess_data(self, df: pl.DataFrame):
        X, y = self.generate_train_data(df)
        if os.path.exists(self.model_path):
            self.load_model()
            X = self.scaler.transform(X)
            y = self.scaler.transform(np.array([[_y[0],_y[0]] for _y in y]))
            y = y[:,1]
        else:
            X = self.scaler.fit_transform(X)
            y = self.scaler.transform(np.array([[_y[0],_y[0]] for _y in y]))
            y = y[:,1]

        return X, y
    
    def inverse_transform(self, y):
        y = self.scaler.inverse_transform(np.array([[_y,_y] for _y in y]))
        return y[:,1]

    def generate_train_data(self, df: pl.DataFrame):
        '''
        Generate data to train model.
        We adopt the sigle output model, in this case, the model takes the date and the close price
        and predicts the price for next day.
            date[n], Close[n] -> Close[n+predict_days]
        '''
        df = df.with_columns(
            pl.col("Adj Close").shift(-1).alias("Prediction")
        ).with_columns(
            pl.col("Date").dt.epoch("d").alias("Date")
        )

        X = df.select("Date", "Adj Close")[:-1]
        y = df.select("Prediction")[:-1]

        return X.to_numpy(), y.to_numpy()
    
    def train(self, df):
        self.last_data = df.select("Date", "Adj Close")[-1] # store last data to predict future values
        X, y = self.preprocess_data(df)
        metrics = self.metrics(X, y)
        self.model.fit(X, y)
        self.compute_residuals(X, y)
        self.mse = metrics['mse']

    def compute_residuals(self, X, y):
        y_pred = self.inverse_transform(self.model.predict(X))
        y = self.inverse_transform(y)
        self.residuals = y - y_pred

    def predict_steps(self, X: pl.DataFrame, steps, inv_transform=True):
        prediction = [] 
        dates = []
        pred_date = X.select("Date").item()
        date, x = self.scale(X).squeeze()

        for _ in range(steps):
            x = self.model.predict([[date, x]]).squeeze().item()
            # update variables
            pred_date += timedelta(days=1)
            date = self.scaler.transform(
                pl.DataFrame({"Date": [pred_date], "Prediction": [x]})
                .with_columns(pl.col("Date").dt.epoch("d")).to_numpy()
            ).squeeze()[0].item()
            # store data
            dates.append(pred_date)
            prediction.append(x)

        if inv_transform:
            prediction = self.inverse_transform(prediction)
        return pl.DataFrame({"Date": dates, "Prediction": prediction})
    
    def predict(self, n_days, **kwargs):
        return_interval = kwargs.get("return_interval", False)
        confidence = kwargs.get("confidence", 0.05)
        n_boot = kwargs.get("n_boot", 250)

        pred = self.predict_steps(self.last_data.clone(), n_days)
        if return_interval:
            bootstrapping_resampling = self.bootstrapping_resampling(self.last_data.clone(), n_boot, n_days)
            pred_intervals = pl.DataFrame({
                "Date": pred.select("Date"),
                "Prediction": pred.select("Prediction"),
                "Lower": bootstrapping_resampling.quantile(confidence/2).transpose().to_numpy().squeeze(),
                "Upper": bootstrapping_resampling.quantile(1-confidence/2).transpose().to_numpy().squeeze(),
            })
            return pred_intervals
        return pred

    def metrics(self, X, y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        self.model.fit(X_train, y_train)
        y_pred = self.model.predict(X_test)
        return {
            "mse": mean_squared_error(y_test, y_pred),
            "mae": mean_absolute_error(y_test, y_pred)
        }
    
    def bootstrapping_resampling(self, X: pl.DataFrame, n_boot, steps):
        # https://github.com/JoaquinAmatRodrigo/skforecast/blob/master/skforecast/ForecasterAutoreg/ForecasterAutoreg.py#L849
        boot_predictions = np.full(shape=(steps, n_boot), fill_value=np.nan, dtype=float)
        for i in range(n_boot):
            input = X.clone()
            sampled_residuals = np.random.choice(a=self.residuals, size=steps, replace=True)
            for step in range(steps):
                result = self.predict_steps(input, 1)
                prediction = result.select("Prediction").item()
                date = result.select("Date").item()
                boot_predictions[step,i] = prediction + sampled_residuals[step]
                input = pl.DataFrame({"Date": date, "Adj Close": [boot_predictions[step,i]]})
        return pl.DataFrame({f"boot_{i}": boot_predictions[i] for i in range(steps)})
    
    def save_model(self):
        with open(self.model_path, "wb") as f:
            pkl.dump([self.scaler, self.model], f)

    def load_model(self):
        with open(self.model_path, "rb") as f:
            self.scaler, self.model = pkl.load(f)

if __name__ == "__main__":
    # m = LinearRegressionModel("linear", "model.pkl", 10)
    # m.train(df)
    # pred_linear = m.predict(10, return_interval=True, n_boot=250)
    
    # m = LinearRegressionModel("ridge", "model.pkl", 10,)
    # m.train(df)
    # pred_ridge = m.predict(10, return_interval=True, n_boot=250)

    # plt.plot(df.select("Date").to_numpy(), df.select("Adj Close").to_numpy())
    # plt.plot(pred_linear.select("Date").to_numpy(), pred_linear.select("Prediction").to_numpy(), label="Linear")
    # plt.plot(pred_ridge.select("Date").to_numpy(), pred_ridge.select("Prediction").to_numpy(), label="Ridge")
    # plt.fill_between(
    #     pred_linear.select("Date").to_numpy().squeeze(), 
    #     pred_linear.select("Lower").to_numpy().squeeze(), 
    #     pred_linear.select("Upper").to_numpy().squeeze(), 
    #     alpha=0.5
    # )
    # plt.show()
    # sleep(10000)
    pass