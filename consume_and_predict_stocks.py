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
from typing import List, Union
import datetime


def plot_metrics_models(models):
    nb_models = len(models)
    fig, ax = plt.subplots(nb_models, 1, figsize=(15, 10))
    for i in range(nb_models):
        ax[i].set_title(f"Metrics through time")
        ax[i].plot(
            models[i].forecast_dates, models[i].rmse_holt_list, label="HoltWinters"
        )
        ax[i].plot(
            models[i].forecast_dates, models[i].rmse_snarimax_list, label="SNARIMAX"
        )
        ax[i].plot(models[i].forecast_dates, models[i].rmse_cst_list, label="Constant")
        ax[i].set_title(models[i].topic)
        ax[i].legend()
        ax[i].set_xlabel("Date")
        ax[i].set_ylim(0, models[i].max_value * 1.1)
    plt.tight_layout()
    plt.savefig(metrics_plot_file)
    plt.close()


class ConstantModel:
    def learn_one(self, x):
        return self

    def forecast(self, horizon, x):
        return [x for _ in range(horizon)]


class StreamModels:
    def __init__(self, topic, horizon, forecast_delta):
        self.topic: str = topic
        self.idx: int = 0
        self.horizon: int = horizon
        self.forecast_delta: int = forecast_delta
        self.max_value: int = 0
        # define models
        self.cst_model = ConstantModel()
        self.snarimax_model = time_series.SNARIMAX(p=2, d=1, q=1)
        self.holt_model = time_series.HoltWinters(
            alpha=0.3, beta=0.1, gamma=0.6, seasonality=24
        )
        # define memory
        self.y_true_memory: Union[List, None] = None
        # TODO: dict
        self.forecast_dates: List = []
        self.forecast_holt: List = []
        self.forecast_snarimax: List = []
        self.forecast_cst: List = []
        # TODO: dict
        self.rmse_holt_list: List[float] = []
        self.rmse_snarimax_list: List[float] = []
        self.rmse_cst_list: List[float] = []

    def _forecast(self, x):
        forecast_holt = self.holt_model.forecast(horizon)
        forecast_snarimax = self.snarimax_model.forecast(horizon)
        self.forecast_cst = self.cst_model.forecast(horizon, x)

        # A stock can't go below 0
        self.forecast_holt = np.maximum(forecast_holt, 0)
        self.forecast_snarimax = np.maximum(forecast_snarimax, 0)

    def _scores(self, date):
        rmse_holt = metrics.RMSE()
        rmse_snarimax = metrics.RMSE()
        rmse_cst = metrics.RMSE()

        for yt, yh, ys, yc in zip(
            self.y_true_memory,
            self.forecast_holt,
            self.forecast_snarimax,
            self.forecast_cst,
        ):
            rmse_holt.update(yt, yh)
            rmse_snarimax.update(yt, ys)
            rmse_cst.update(yt, yc)

        self.forecast_dates.append(date - datetime.timedelta(days=30))
        self.rmse_holt_list.append(rmse_holt.get())
        self.rmse_snarimax_list.append(rmse_snarimax.get())
        self.rmse_cst_list.append(rmse_cst.get())

    def train(self, x, date):
        if x > self.max_value:
            self.max_value = x

        # learn_one on models
        self.holt_model = self.holt_model.learn_one(x)
        self.snarimax_model = self.snarimax_model.learn_one(x)

        # add to memory OR calculate scores and reset memory
        if self.y_true_memory is not None and len(self.y_true_memory) < horizon:
            self.y_true_memory.append(x)
        elif self.y_true_memory is not None:
            self._scores(date)
            self._plot_forecasts(date)
            self.y_true_memory = None

        # predict and set memory
        if self.idx >= 365 and self.idx % self.forecast_delta == 0:
            self._forecast(x)
            self.y_true_memory = []
        self.idx += 1

    def _plot_forecasts(self, date):
        plt.figure(figsize=(15, 8))
        plt.plot(self.forecast_holt, "r-", label="Predictions HoltWinters")
        plt.plot(self.forecast_snarimax, "y-", label="Predictions SNARIMAX")
        plt.plot(self.forecast_cst, "b-", label="Predictions Constant")
        plt.plot(self.y_true_memory, "g-", label="True Values")
        plt.legend()
        plt.grid()
        plt.savefig(
            plot_folder + f"{self.topic}_forecast_{horizon}_on_{date:%Y-%m-%d}.png"
        )
        plt.close()


def make_predictions():
    consumer_indexes = KafkaConsumer(
        sp500_topic,
        cac40_topic,
        nikkei225_topic,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode()),
    )

    sp500_models = StreamModels(sp500_topic, horizon, forecast_delta)
    cac40_models = StreamModels(cac40_topic, horizon, forecast_delta)
    nikkei225_models = StreamModels(nikkei225_topic, horizon, forecast_delta)

    topic2model = {
        sp500_topic: sp500_models,
        cac40_topic: cac40_models,
        nikkei225_topic: nikkei225_models,
    }

    launch_date = datetime.datetime.now().date()

    for msg in consumer_indexes:
        date = pd.to_datetime(msg.timestamp, unit="s").date()  # get date
        close = msg.value["Close"]

        if not close:
            continue

        if msg.topic in topic2model.keys():
            topic2model[msg.topic].train(close, date)

        # plot at the end
        if date >= launch_date:
            plot_metrics_models(list(topic2model.values()))
            break


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

    plot_folder = f"stocks_forecast/"
    metrics_plot_file = "metrics_through_time.png"

    make_predictions()
