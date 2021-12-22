from datetime import datetime, timedelta
from textwrap import dedent
import time

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'bian',
    'depends_on_past': False,
    'email': ['bm3024@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

count = 0


def correct_sleeping_function():
    """This is a function that will run within the DAG execution"""
    time.sleep(2)


def count_function():
    # this task is t1
    global count
    count += 1
    print('count_increase output: {}'.format(count))
    time.sleep(2)


def print_a():
    print('Hello')
    time.sleep(2)


def wrong_sleeping_function():
    # this task is t2_1, t1 >> t2_1
    global count
    print('wrong sleeping function output: {}'.format(count))
    assert count == 1
    time.sleep(2)


with DAG(
        'stock_dag',
        default_args=default_args,
        description='A simple toy DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 11, 29, 14, 0),
        catchup=False,
        tags=['example'],
) as dag:
    # t* examples of tasks created by instantiating operators

    t1 = BashOperator(
        task_id='twitterHTTPClient',
        bash_command='python /home/bigdatabm3024/airflow/dags/twitterHTTPClient.py',
        retries=3,
    )

    t2 = BashOperator(
        task_id='stream_twitter_data',
        bash_command='python /home/bigdatabm3024/airflow/dags/stream_twitter_data.py',
        retries=3,
    )

    t3 = BashOperator(
        task_id='preprocess_tweets',
        bash_command='python /home/bigdatabm3024/airflow/dags/preprocess_tweets.py',
        retries=3,
    )

    t4 = BashOperator(
        task_id='sentiment_analysis',
        bash_command='python /home/bigdatabm3024/airflow/dags/sentiment_analysis.py',
        retries=3,
    )

    t5 = BashOperator(
        task_id='stream_stock_data',
        bash_command='python /home/bigdatabm3024/airflow/dags/stream_stock_data.py',
        retries=3,
    )

    t6 = BashOperator(
        task_id='preprocess_stock',
        bash_command='python /home/bigdatabm3024/airflow/dags/preprocess_stock.py',
        retries=3,
    )

    t7 = BashOperator(
        task_id='correlate_sentiment_stock',
        bash_command='python /home/bigdatabm3024/airflow/dags/correlate_sentiment_stock.py',
        retries=3,
    )

    t8 = BashOperator(
        task_id='write_to_bq',
        bash_command='python /home/bigdatabm3024/airflow/dags/write_to_bq.py',
        retries=3,
    )

    # task dependencies

    t1 >> t2
    t2 >> t3
    t3 >> t4
    t5 >> t6
    t4 >> t7
    t6 >> t7
    t7 >> t8
    t2 >> t8
    t5 >> t8
