"""
Script to ingest stocks data into Kafka

1. API request to Yahoo! Finance to get stocks data
2. Iter data
3. Send via Kafka Producer

Authors: Romain Bernard, Vincent Tchoumba, Guillaume Philippe
"""

from kafka import KafkaProducer
import yfinance as yf
import time


def ingest_stocks(
    sp500_ind,
    sp500_topic,
    cac40_ind,
    cac40_topic,
    nikkei225_ind,
    nikkei225_topic,
    delay,
):
    """

    :param sp500_ind:
    :param sp500_topic:
    :param cac40_ind:
    :param cac40_topic:
    :param nikkei225_ind:
    :param nikkei225_topic:
    :param delay:
    :return:
    """
    tickers = yf.Tickers([sp500_ind, cac40_ind, nikkei225_ind])
    tickers_history = tickers.history(start="2000-01-01")
    print("Total trading days:", tickers_history.shape[0])

    producer = KafkaProducer(value_serializer=lambda s: s.to_json().encode())

    for row in tickers_history.iterrows():
        ts, data = row
        ts = int(ts.timestamp())
        # S&P 500
        producer.send(sp500_topic, data[slice(None), sp500_ind], timestamp_ms=ts)

        # CAC 40
        producer.send(cac40_topic, data[slice(None), cac40_ind], timestamp_ms=ts)

        # Nikkei 225
        producer.send(
            nikkei225_topic, data[slice(None), nikkei225_ind], timestamp_ms=ts
        )

        time.sleep(delay)


if __name__ == "__main__":
    sp500_ind = "^GSPC"
    sp500_topic = "sp500"

    cac40_ind = "^FCHI"
    cac40_topic = "cac40"

    nikkei225_ind = "^N225"
    nikkei225_topic = "nikkei225"

    # Delay in seconds between 2 rows of the dataframe
    # In a real case, it should be 86400 seconds (1 day)
    delay = 0.1

    ingest_stocks(
        sp500_ind,
        sp500_topic,
        cac40_ind,
        cac40_topic,
        nikkei225_ind,
        nikkei225_topic,
        delay,
    )
