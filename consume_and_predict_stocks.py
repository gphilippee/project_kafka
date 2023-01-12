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

    return rmse_holt.get(), rmse_snarimax.get()


def consumer_model(models, metrics, forcasts, value, forcast_dates, y_true, idx=0):
    """

    :param models (list): list of the models to train
    :param metrics (list): Metrics to use to evaluate each model, len(metrics) == len(models)
    :param forcasts (list):
    :param idx (int): 
    :param value (float64):
    :return:
    """
    models[0] = models[0].learn_one(value)
    models[1] = models[1].learn_one(value)

    # we need retain in memory the true values in order to calculate metrics
    if y_true is not None:
        if len(y_true) < horizon:
            y_true.append(value)
        else:
            score_holt, score_snarimax = calculate_scores(
                y_true, forcasts[0], forcasts[1]
            )
            metrics[0].append(score_holt)
            metrics[1].append(score_snarimax)
            plot_scores(forcast_dates, metrics[0], metrics[1])
            plot_results(
                forcast_dates[-1], y_true, forcasts[0], forcasts[1]
            )
            y_true = None

    # no predict before 365
    # we will predict each 120 days
    if idx >= 365 and idx % forecast_delta == 0:
        forcast_dates.append(date)
        forcasts[0], forcasts[1] = predict(models[0], models[1])
        y_true = []

    idx += 1  # update index



def make_predictions():
    consumer_indexes = KafkaConsumer(
        sp500_topic,
        cac40_topic,
        nikkei225_topic,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode()),
    )

    # Define 1st model
    holt_model_sp500 = time_series.HoltWinters(alpha=0.3, beta=0.1, gamma=0.6, seasonality=24)
    holt_model_cac40 = time_series.HoltWinters(alpha=0.3, beta=0.1, gamma=0.6, seasonality=24)
    holt_model_nikkei225 = time_series.HoltWinters(alpha=0.3, beta=0.1, gamma=0.6, seasonality=24)


    # Define 2nd model
    # If d=0, ARMA, else ARIMA
    snarimax_model_sp500 = time_series.SNARIMAX(p=2, d=1, q=1)
    snarimax_model_cac40 = time_series.SNARIMAX(p=2, d=1, q=1)
    snarimax_model_nikkei225 = time_series.SNARIMAX(p=2, d=1, q=1)

    idx_sp500 = 0
    idx_cac40 = 0
    idx_nikkei225 = 0

    for message in consumer_indexes:
        date = pd.to_datetime(message.timestamp, unit="s").date()  # get date
        close = message.value["Close"]

        if not close:
            continue

        if message.topic == sp500_topic:  # If message comes from S&P500 topic

            y_true_sp500 = None
            metrics_sp500 = [[],[]]
            forecasts_sp500 = [np.array([]), np.array([])]
            forecast_dates_sp500 = list()

            consumer_model(
                models = [holt_model_sp500, snarimax_model_sp500],
                metrics = metrics_sp500,
                forcasts = forecasts_sp500,
                value = close,
                forcast_dates = forecast_dates_sp500,
                y_true = y_true_sp500,
                idx = idx_sp500
            )

        elif message.topic == cac40_topic:  # TODO: implement models for other topics
            
            y_true_cac40  = None
            metrics_cac40 = [[],[]]
            forecasts_cac40 = [np.array([]), np.array([])]
            forecast_dates_cac40 = list()

            consumer_model(
                models = [holt_model_cac40, snarimax_model_cac40],
                metrics = metrics_cac40,
                forcasts = forecasts_cac40,
                value = close,
                forcast_dates = forecast_dates_cac40,
                y_true = y_true_cac40,
                idx = idx_cac40
            )

        else:

            y_true_nikkei225 = None
            metrics_nikkei225 = [[],[]]
            forecasts_nikkei225 = [np.array([]), np.array([])]
            forecast_dates_nikkei225 = list()

            consumer_model(
                models = [holt_model_nikkei225, snarimax_model_nikkei225],
                metrics = metrics_nikkei225,
                forcasts = forecasts_nikkei225,
                value = close,
                forcast_dates = forecast_dates_nikkei225,
                y_true = y_true_nikkei225,
                idx = idx_nikkei225
            )


if __name__ == "__main__":
    # Parameters
    sp500_topic = "sp500"
    cac40_topic = "cac40"
    nikkei225_topic = "nikkei225"

    horizon = 30  # forecast of horizon days
    forecast_delta = 120  # days between 2 forecast
    assert forecast_delta > horizon, "forecast_delta should be superior to horizon"

    if not os.path.exists("stocks_forecast"):
        os.mkdir("stocks_forecast")

    base_plot_file = f"stocks_forecast/{horizon}_days_forecast"
    mse_plot_file = "mse_through_time.png"

    make_predictions()
