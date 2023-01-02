# -*- coding: utf-8 -*-
"""
Created on Sat Dec 31 15:13:43 2022

@author: Vincent
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import time
import json
from kafka import KafkaConsumer
from kafka import KafkaConsumer
from river.stream import iter_pandas
from river import time_series
from river import metrics
from river import utils
import math


msft = yf.Ticker('^GSPC')
hist = msft.history(start="2016-01-01", end="2020-01-01")

def make_predictions():
    
    sp500_topic = "sp500"
    
    consumer_sp500 = KafkaConsumer(sp500_topic, bootstrap_servers="localhost:9092", 
                                   value_deserializer=lambda m: json.loads(m.decode()))
    df = pd.DataFrame(columns=['Close','Dividends','High','Low', 'Open', 'Stock Splits', 'Volume'])
    
    model_1 = time_series.HoltWinters(alpha=0.3,beta=0.1,gamma=0.6,seasonality=24)
    period = 12
    model_2 = time_series.SNARIMAX(p=period,d=1,q=period,m=period,sd=1)
    
    time_s = []
    for row in consumer_sp500:
        ts = str(pd.to_datetime(row.timestamp, unit='s'))[:10]
        time_s.append(ts)
        row = pd.DataFrame([row.value])
        df = pd.concat([df, row])
        if len(df)==365:
            break
        
    print(df)
    y_true = list()
    for y in df['Close']:
        if not math.isnan(y):
            model_1 = model_1.learn_one(y)
            model_2 = model_2.learn_one(y)
            y_true.append(y)
            
    horizon = 90
    forecast = model_1.forecast(horizon=horizon)
    forecast_2 = model_2.forecast(horizon=horizon)
    
    y_true.append(forecast);y_true.append(forecast_2)
    idx = int(np.where(hist['Close']==hist['Close'][time_s[-1]])[0])
    
    plt.figure(figsize=(15,8))
    x = [range(len(y_true[:-2])), range(len(y_true[:-2]), len(y_true[:-2])+len(y_true[-1]))]
    plt.plot(x[0],y_true[:-2], 'b-', label="Training")
    plt.plot(x[1],y_true[-2], 'r-', label="Predictions HoltWinters")
    plt.plot(x[1],y_true[-1], 'y-', label="Predictions SNARIMAX")
    plt.plot(x[1], hist['Close'][idx:idx+horizon], 'g-', label="True Values")
    plt.legend()
    plt.grid()
    plt.savefig("90_days_forecast.png")
    plt.show()
    
make_predictions()