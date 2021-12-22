# 202112-48-Correlation-Between-Public-Sentiment-on-Tech-Companies-vs-NASDAQ-Index
EECS E6893 Big Data Analytics final project

## Introduction
We explored whether or not general sentiment on publicly traded companies is subtly correlated with the movement of their stock prices. To do so, streamed Twitter for tweets related to the specific companies we have selected (companies that dominate the Nasdaq-100 index), perform sentiment analysis on these tweets, stream the movement of the Nasdaq-100 index, and finally perform correlation analysis between daily changes in public sentiment vs. changes in the index’s price. 


## Prerequisites
### VM Environment
The project is hosted on GCP VM instance with the following configuration:

 - machine type: n1-standard-8
 - Google-cloud-bigquery tweepy==3.1.0
 - cs-connector-version=1.9.16
 - bigquery-connector-version=1.0.0
 - OS: Ubuntu 18.04.4

### Packages
This project is developed using python Python 3.7.12 and the packages below

 - Python 3.7.12
 - tensorflow 2.4.0
 - Airflow
 - Tweepy
 - Yfinance
 - Sklearn
 - Numpy
 - Pandas

## Datasets

1. Twitter streamed data - streamed daily for 20 days. saved in Google BigQuery. project_id='bigdata6893-328419' total 943622 raw tweets
2. NASDAQ - 100 index data streamed using yfinance. saved in Google BigQuery. project_id='bigdata6893-328419' total 20 days raw data
3. NLTK tweets sample - 10000 tweets with sentiment label. downloaded from NLTK https://www.nltk.org/howto/twitter.html

## Model

LSTM model is stored in Google Drive: https://drive.google.com/drive/folders/1gRJkuD8sRVmAQt4h95VcxAKBeBiCRXMG?usp=sharing

## System Airflow files

1. project_flow.py - To run the whole flow first run project_flow.py all file below are dependency
2. twitterHTTPClinet.py - To set up twitter clinet to stream tweets
3. stream_twitter_data.py - To stream raw tweets
4. stream_stock_data.py - To stream NASDAQ-100 index data
5. preprocess_stock.py - To clean raw stock data to desired format
6. preprocess_tweets.py - To clean raw tweets data to desired format
7. sentiment_analysis.py - To run sentiment analysis on our pre-trained LSTM model with GloVe embeddings
8. correlate_sentiment_stock.py -  To run corrlation analysis

## Results

From the twitter streams and the Nasdaq-100 index price movement that we obtained over the 20 day period that we streamed,we determined a correlation coefficient of 0.41 with a two-tailed p-value of 0.07. This is not statistically significant at the 5% level of confidence, but it still suggests that there may be correlation between the two variables (results are statistically significant at the 10% level of confidence). Based on the approaches that we took, it seems like the GloVe embeddings with neural networks are a better pipeline for sentiment analysis over approaches that utilize sparse embeddings like a bag-of-words or TF-IDF approach and traditional machine learning classifiers.   

## Organization

Jupiter Notebook and python file: sentimentModelTraditionalML.ipynb, sentimentModelNN.ipynb, correlate_sentiment_stock.py

Source code: included in corresponding folder

