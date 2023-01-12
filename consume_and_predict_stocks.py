"""
Authors: Romain Bernard, Vincent Tchoumba, Guillaume Philippe
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import json
from kafka import KafkaConsumer
from river import time_series, metrics
import os


def plot_results(date, y_true, y_pred_holt, y_pred_snarimax):
    plt.figure(figsize=(15, 8))
    plt.plot(y_pred_holt, "r-", label="Predictions HoltWinters")
    plt.plot(y_pred_snarimax, "y-", label="Predictions SNARIMAX")
    plt.plot(y_true, "g-", label="True Values")
    # to avoid flat plot if forecast goes to far
    # plt.ylim(bottom=0, top=min(10_000, np.max([y_pred_holt, y_pred_snarimax]) + 500))
    plt.legend()
    plt.grid()
    plt.savefig(base_plot_file + f"_{date:%Y-%m-%d}.png")
    plt.close()


def plot_scores(forecast_dates, rmse_holt_list, rmse_snarimax_list):
    # Plot to see the impact of training the model on MSE
    plt.figure()
    plt.title(f"MSE through time")
    plt.yscale("log")
    plt.plot(forecast_dates, rmse_holt_list, label="HoltWinters")
    plt.plot(forecast_dates, rmse_snarimax_list, label="SNARIMAX")
    plt.xlabel("Date")
    plt.ylabel("MSE")
    plt.legend()
    plt.savefig(mse_plot_file)
    plt.close()


def predict(holt_model, snarimax_model):
    """

    :param holt_model:
    :param snarimax_model:
    :return:
    """
    forecast_holt = holt_model.forecast(horizon=horizon)
    forecast_snarimax = snarimax_model.forecast(horizon=horizon)

    # A stock can't go below 0
    forecast_snarimax = np.maximum(forecast_snarimax, 0)
    forecast_holt = np.maximum(forecast_holt, 0)

    return forecast_holt, forecast_snarimax


def calculate_scores(y_true, y_holt, y_snarimax):
    """

    :param y_true:
    :param y_holt:
    :param y_snarimax:
    :return:
    """
    rmse_holt = metrics.RMSE()
    rmse_snarimax = metrics.RMSE()

    for yt, y_holt, y_snarimax in zip(y_true, y_holt, y_snarimax):
        rmse_holt.update(yt, y_holt)
        rmse_snarimax.update(yt, y_snarimax)

    return rmse_holt, rmse_snarimax


def make_predictions():
    consumer_indexes = KafkaConsumer(
        sp500_topic,
        cac40_topic,
        nikkei225_topic,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode()),
    )

    # Define 1st model
    holt_model = time_series.HoltWinters(alpha=0.3, beta=0.1, gamma=0.6, seasonality=24)

    # Define 2nd model
    # If d=0, ARMA, else ARIMA
    snarimax_model = time_series.SNARIMAX(p=2, d=1, q=1)

    # to plot evolution of MSE through time
    forecast_dates = []  # array for dates of forecast
    rmse_holt_list = []
    rmse_snarimax_list = []

    idx_sp500 = 0
    y_true = None
    for message in consumer_indexes:
        date = pd.to_datetime(message.timestamp, unit="s").date()  # get date
        close = message.value["Close"]

        if not close:
            continue

        if message.topic == sp500_topic:  # If message comes from S&P500 topic
            holt_model = holt_model.learn_one(close)
            snarimax_model = snarimax_model.learn_one(close)

            # we need retain in memory the true values in order to calculate metrics
            if y_true is not None:
                if len(y_true) < horizon:
                    y_true.append(close)
                else:
                    mse_holt, mse_snarimax = calculate_scores(
                        y_true, forecast_holt, forecast_snarimax
                    )
                    rmse_holt_list.append(mse_holt.get())
                    rmse_snarimax_list.append(mse_snarimax.get())
                    plot_scores(forecast_dates, rmse_holt_list, rmse_snarimax_list)
                    plot_results(
                        forecast_dates[-1], y_true, forecast_holt, forecast_snarimax
                    )
                    y_true = None

            # no predict before 365
            # we will predict each 120 days
            if idx_sp500 >= 365 and idx_sp500 % forecast_delta == 0:
                forecast_dates.append(date)
                forecast_holt, forecast_snarimax = predict(holt_model, snarimax_model)
                y_true = []

            idx_sp500 += 1  # update index
        else:  # TODO: implement models for other topics
            pass


if __name__ == "__main__":
    # Parameters
    sp500_topic = "sp500"
    cac40_topic = "cac40"
    nikkei225_topic = "nikkei225"

    horizon = 30  # forecast of horizon days
    forecast_delta = 365  # days between 2 forecast
    assert forecast_delta > horizon, "forecast_delta should be superior to horizon"

    if not os.path.exists("stocks_forecast"):
        os.mkdir("stocks_forecast")

    base_plot_file = f"stocks_forecast/{horizon}_days_forecast"
    mse_plot_file = "mse_through_time.png"

    make_predictions()
