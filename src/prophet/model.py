import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from prophet import Prophet


def clean_data(df : pd.DataFrame) -> pd.DataFrame:
    """CLEAN DATA

    Parameters
    ----------
    df : pd.DataFrame
        Data to be cleaned
    Returns
    -------
    pd.DataFrame
        Cleaned data
    """
    df = df[['timestamp', 'data.raw_cur_flow']]
    df.rename(columns={'data.raw_cur_flow': 'value'}, inplace=True)
    df = df[~df['value'].isna()]
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df.rename(columns={'timestamp': 'ds', 'value': 'y'}, inplace=True)

    return df

def train_model(df : pd.DataFrame, period : int = 2016) -> tuple:
    """TRAIN MODEL

    Parameters
    ----------
    df : pd.DataFrame
        Data to be trained
    period : int
        Period to be forecasted
    Returns
    -------
    tuple
        Model and future data
    """
    m = Prophet(changepoint_prior_scale=0.01).fit(df)
    future = m.make_future_dataframe(periods=period, freq='5min')

    return m, future

def predict(m, future) -> pd.DataFrame:
    """PREDICT

    Parameters
    ----------
    m : Prophet
        Model
    future : pd.DataFrame
        Future data
    Returns
    -------
    pd.DataFrame
        Forecasted data
    """
    forecast = m.predict(future)
    forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    forecast['ds'] = pd.to_datetime(forecast['ds'])
    forecast['ds'] = forecast['ds'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return forecast

def main() -> pd.DataFrame:
    """MAIN FUNCTION

    Parameters
    ----------
    None
    Returns
    -------
    pd.DataFrame
        Forecasted data
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    data_path = f"{current_dir}/data/messages.csv"
    df = pd.read_csv(data_path)
    df = clean_data(df)
    m, future = train_model(df)
    forecast = predict(m, future)
    return forecast

if __name__ == "__main__":
    main()