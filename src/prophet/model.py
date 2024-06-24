import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from prophet import Prophet
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from collect_data.collector import collect_data

def get_data() -> pd.DataFrame:
    """GET DATA

    Parameters
    ----------
    None
    Returns
    -------
    pd.DataFrame
        Data
    """
    data = collect_data()
    df = pd.DataFrame(data, columns=['value', 'timestamp'])
    return df

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
    df = df[~df['value'].isna()]
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['timestamp'] = df[(df['timestamp'] >= datetime.now() - timedelta(days=365)) & (df['timestamp'] <= datetime.now() - timedelta(days=365) + timedelta(days=30))] # 24-06-2023 -> 24-07-2023 : 24-06-2024 -> 24-07-2024
    # mudar ano dos daods de 2023 para 2024
    df['timestamp'] = df['timestamp'].apply(lambda x: x.replace('2023', '2024'))
    # mudar os dados para ficarem menores que a data atual
    df['timestamp'] = df['timestamp'].apply(lambda x: x - timedelta(days=datetime.now().day) + timedelta(days=1))
    print(df)
    df.rename(columns={'timestamp': 'ds', 'value': 'y'}, inplace=True)

    return df

def get_last_date(df : pd.DataFrame) -> str:
    """GET LAST DATE

    Parameters
    ----------
    df : pd.DataFrame
        Data
    Returns
    -------
    str
        Last date
    """
    last_date = df['ds'].max()

    return last_date

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
    df = get_data()
    df = clean_data(df)
    last_date = get_last_date(df)
    m, future = train_model(df)
    forecast = predict(m, future)
    forecast = forecast[forecast['ds'] > last_date]
    return forecast

if __name__ == "__main__":
    main()