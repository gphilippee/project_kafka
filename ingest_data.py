"""
Authors: Romain Bernard, Vincent Tchoumba, Guillaume Philippe
"""

from kafka import KafkaProducer
import yfinance as yf
import time

sp500_ind = "^GSPC"
sp500_topic = "sp500"

cac40_ind = "^FCHI"
cac40_topic = "cac40"

nikkei225_ind = "^N225"
nikkei225_topic = "nikkei225"

if __name__ == "__main__":

    tickers = yf.Tickers([sp500_ind, cac40_ind, nikkei225_ind])
    tickers_history = tickers.history(start="2000-01-01")

    producer = KafkaProducer(value_serializer=lambda s: s.to_json().encode())

    for row in tickers_history.iterrows():
        ts, data = row
        ts = int(ts.timestamp())
        # S&P 500
        producer.send(sp500_topic, data[slice(None), sp500_ind], timestamp_ms=ts)

        # CAC 40
        producer.send(cac40_topic, data[slice(None), cac40_ind], timestamp_ms=ts)

        # Nikkei 225
        producer.send(nikkei225_topic, data[slice(None), nikkei225_ind], timestamp_ms=ts)
