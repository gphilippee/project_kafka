# Project on Kafka

Authors: Romain Bernard, Vincent Tchoumba, Guillaume Philippe

## Delivrables

Put the code Source .py and HTML in Zip file (Moodle) and available on a Github  
Presentation PowerPoint & Results (5” min): Problem, Solution, Results
Demo (Proof that the code compiles and everything run) – 5 min
Send an email to mariam.barry@polytechnique.edu with groupe ID - Student Names in object + Github link, slides and put all the materials (code source & slides)  on the Moodle of Ecole Polytechnique before 15/01/2023 23h59

## Description

Project 7 : (Trading Data) Collect trading data using Yahoo finance API and use online regression (from river) to predict markets stocks of CAC40, S&P500, Google, Facebook & Amazon or any others enterprise.

Option 1: Use global index data streams of each of the 3 regions https://www.bloomberg.com/markets/stocks
For example for US, S&P500, for EU CAC40, 

Option 2 : For each of these 5 countries, use 1 major industry stock data
For ex, in US Google,  in France BNP Paribas, in China Alibaba, in Russia or England, use a major international industry.
This option was initially given in the project.

Option 3 : Stock Market for cryptos such as Bitcoin stock or other Binance stock (https://www.binance.com/en/landing/data) or Currency Data evolution (EUR/USD, USD/RUB)

For each option, each group should use at least 3 different data streams, with online and adaptive regression on RIVER and compare the performances with batch regression model (scikit-learn).

To Do: Compare online Regression vs Batch Regression / Time Series forecasting and discuss the performance.

Bonus: Use recent stock market data in 2022

Online resources: 
You can use the Python library to collect Yahoo Finance data in streaming https://pypi.org/project/yfinance/
You can compute time-series statistics and moving averages (MACD) for features engineering https://www.statsmodels.org/stable/tsa.html

## How to run ?

1. Start the ZooKeeper service
```sh
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```
Open another terminal session and run:
2. Start the Kafka broker service
```sh
kafka-server-start /usr/local/etc/kafka/server.properties
```
Open another terminal session and run:
3. Create virtual env
```sh
python -m venv .venv
```

4. Install requirements
```sh
pip install -r requirements.txt
```
