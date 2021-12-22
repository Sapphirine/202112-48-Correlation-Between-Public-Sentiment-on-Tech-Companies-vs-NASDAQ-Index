# -*- coding: utf-8 -*-
"""correlate_sentiment_stock.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1HeRT5FETeXttXsIfKPTFcVaH9KqGelhx
"""

# Commented out IPython magic to ensure Python compatibility.
import pandas_gbq
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
import seaborn as sns
from scipy import stats

# %matplotlib inline
sns.set_style('darkgrid')

if __name__ == '__main__':

        sentiment_results = pandas_gbq.read_gbq(
            'SELECT * FROM bigdata6893-328419.sentiment_ratio',
            project_id = 'bigdata6893-328419'
        )

        stock_results = pandas_gbq.read_gbq(
            'SELECT delta FROM bigdata6893-328419.stock_data',
            project_id = 'bigdata6893-328419'
        )

        sentiment_scaler = StandardScaler()
        stock_scaler = StandardScaler()

        sentiment_results = sentiment_scaler.fit_tramsform(sentiment_results.to_numpy())
        stock_results = stock_scaler.fit_transform(stock_results.to_numpy())

        plt.figure(figsize = (12, 8))
        sns.regplot(x = sentiment_results, y = stock_results)
        plt.title('Standardized Stock Price Change vs. Standardized Ratio of Positive Sentiment', fontsize = 15)
        plt.ylabel('Standardized Stock Price Change', fontsize = 15)
        plt.xlabel('Standardized ratio of Positive Sentiment', fontsize = 15)
        plt.tick_params(labelsize = 15)

        correlation_coefficient = stats.pearsonr(sentiment_results, stock_results)
        print('The correlation coefficient between normalized stock price and normalized sentiment is: {:.2f}'.format(correlation_coefficient[0]))
        print('The two-tailed p-value is: {:.2f}'.format(correlation_coefficient[1]))
