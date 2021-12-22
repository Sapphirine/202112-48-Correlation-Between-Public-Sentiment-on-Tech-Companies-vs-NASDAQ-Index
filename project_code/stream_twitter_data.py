#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This module is the spark streaming analysis process.


Usage:
    If used with dataproc:
        gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

    Create a dataset in BigQuery first using
        bq mk bigdata_sparkStreaming

    Remember to replace the bucket with your own bucket name


Todo:
    1. hashtagCount: calculate accumulated hashtags count
    2. wordCount: calculate word count every 60 seconds
        the word you should track is listed below.
    3. save the result to google BigQuery

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time
import subprocess
import re
from google.cloud import bigquery
import time
import datetime

# global variables
bucket = "e6893-data"    # TODO : replace with your own bucket name
output_directory_hashtags = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/hashtagsCount'.format(bucket)
output_directory_wordcount = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/wordcount'.format(bucket)
#output_directory_words = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/text'.format(bucket)

# output table and columns name
output_dataset = 'bigdata_sparkStreaming'                     #the name of your dataset in BigQuery
output_table_hashtags = 'hashtags'
columns_name_hashtags = ['hashtags', 'count']
output_table_wordcount = 'wordcount'
columns_name_wordcount = ['word', 'count', 'time']

output_table_words = 'words'
columns_name_words = ['tweet']

# parameter
IP = 'localhost'    # ip port
PORT = 9001       # port

#time = datetime.datetime.now()
STREAMTIME = 50          # time that the streaming process runs
WINDOWLEN = STREAMTIME/10
INTERVAL = STREAMTIME/10
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

WORD = ['data', 'spark', 'ai', 'movie', 'good']     #the words you should filter and do word count

# Helper functions
def saveToStorage(rdd, output_directory, columns_name, mode):
    """
    Save each RDD in this DStream to google storage
    Args:
        rdd: input rdd
        output_directory: output directory in google storage
        columns_name: columns name of dataframe
        mode: mode = "overwirte", overwirte the file
              mode = "append", append data to the end of file
    """
    if not rdd.isEmpty():
        (rdd.toDF(columns_name) \
        .write.save(output_directory, format = "json", mode = mode))

def saveToBigQuery(sc, output_dataset, output_table, directory):
    """
    Put temp streaming json files in google storage to google BigQuery
    and clean the output files in google storage
    """
    files = directory + '/part-*'
    subprocess.check_call(
        'bq load --source_format NEWLINE_DELIMITED_JSON '
        '--replace '
        '--autodetect '
        '{dataset}.{table} {files}'.format(
            dataset=output_dataset, table = output_table, files = files
        ).split())
    output_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
    output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
        output_path, True)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount) #add the new values with the previous running count
    
def hashtagCount(words):
    """
    Calculate the accumulated hashtags count sum from the beginning of the stream
    and sort it by descending order of the count.
    Ignore case sensitivity when counting the hashtags:
        "#Ab" and "#ab" is considered to be a same hashtag
    You have to:
    1. Filter out the word that is hashtags.
       Hashtag usually start with "#" and followed by a serious of alphanumeric
    2. map (hashtag) to (hashtag, 1)
    3. sum the count of current DStream state and previous state
    4. transform unordered DStream to a ordered Dstream
    Hints:
        you may use regular expression to filter the words
        You can take a look at updateStateByKey and transform transformations
    Args:
        dstream(DStream): stream of real time tweets
    Returns:
        DStream Object with inner structure (hashtag, count)
    """

    # TODO: insert your code here
    
    #filter words so that each entry starts with #, then map to lower case and convert to (word, 1) format
    hashtags = words.map(lambda x: x.lower()).filter(lambda x: len(x) >= 2 and x[0] == '#').map(lambda x: (x, 1))

    #get word/frequency 
    h_counts = hashtags.reduceByKey(lambda a, b: a + b)
    
    #join DStreams and order in descending frequency
    t_counts = h_counts.updateStateByKey(updateFunction).transform(lambda df: df.sortBy(lambda x: x[1], ascending = False))
    
    return t_counts
    
def wordCount(words):
    """
    Calculte the count of 5 special words for every 60 seconds (window no overlap)
    You can choose your own words.
    Your should:
    1. filter the words
    2. count the word during a special window size
    3. add a time related mark to the output of each window, ex: a datetime type
    Hints:
        You can take a look at reduceByKeyAndWindow transformation
        Dstream is a serious of rdd, each RDD in a DStream contains data from a certain interval
        You may want to take a look of transform transformation of DStream when trying to add a time
    Args:
        dstream(DStream): stream of real time tweets
    Returns:
        DStream Object with inner structure (word, (count, time))
    """
    
    # TODO: insert your code here
    special_words = words.map(lambda x: x.lower()).filter(lambda x: x in WORD).map(lambda x: (x, 1)) \
    .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOWLEN, INTERVAL) \
    .transform(lambda time, rdd: rdd.map(lambda x: (x[0], x[1], time)))
    
    return special_words

if __name__ == '__main__':
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('TwitterStreamApp')

    # create spark context with the above configuration
    sc = SparkContext(conf = conf)
    sc.setLogLevel('ERROR')

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)
    
    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint('~/checkpoint_TwitterApp')

    # read data from port 9001
    dataStream = ssc.socketTextStream(IP, PORT)
    dataStream.pprint()
    
    words = dataStream.flatMap(lambda line: line.split(' '))
    #dataStream.saveAsTextFiles(output_directory_words, 'txt')
    
    # calculate the accumulated hashtags count sum from the beginning of the stream
    topTags = hashtagCount(words)
    topTags.pprint()
 
    # calculte the word count during each time period 6s
    wordCount = wordCount(words)
    wordCount.pprint()
    
    # save hashtags count and word count to google storage
    # used to save to google BigQuery
    # You should:
    #   1. topTags: only save the lastest rdd in DStream
    #   2. wordCount: save each rdd in DStream
    # Hints:
    #   1. You can take a look at foreachRDD transformation
    #   2. You may want to use helper function saveToStorage
    #   3. You should use save output to output_directory_hashtags, output_directory_wordcount,
    #       and have output columns name columns_name_hashtags and columns_name_wordcount.
    # TODO: insert your code here
    
    topTags.foreachRDD(lambda rdd: saveToStorage(rdd, output_directory_hashtags, columns_name_hashtags, mode = 'overwrite'))
    
    wordCount.foreachRDD(lambda rdd: saveToStorage(rdd, output_directory_wordcount, columns_name_wordcount, mode = 'append'))
    
    #words.foreachRDD(lambda rdd: saveToStorage(rdd, output_directory_words, column_name_words, mode = 'append'))
    
    
    # start streaming process, wait for 600s and then stop.
    ssc.start()
    time.sleep(STREAMTIME)
    ssc.stop(stopSparkContext = False, stopGraceFully = True)
    
    # put the temp result in google storage to google BigQuery
    saveToBigQuery(sc, output_dataset, output_table_hashtags, output_directory_hashtags)
    saveToBigQuery(sc, output_dataset, output_table_wordcount, output_directory_wordcount)
    #saveToBigQuery(sc, output_dataset, output_table_words, output_directory_words)
    