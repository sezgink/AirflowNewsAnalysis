import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Sertan Kutlu',
    'depends_on_past': False,
    'start_date': datetime.datetime(2015, 6, 1),
    'email': ['sezgink@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

crawl_process_dag = DAG(
    dag_id="crawl_process_dag",
    default_args=default_args,
    # start_date=datetime.datetime(2021, 1, 1),
    # schedule="@daily",
    # start_date=datetime.datetime.now()-datetime.timedelta(days=1),
    schedule="*/2 * * * *",
    dagrun_timeout=datetime.timedelta(minutes=2)
)
scrape = BashOperator(
    task_id="scrape",
    # bash_command='echo "Scraping now" ',
    # bash_command='pwd',
    execution_timeout=datetime.timedelta(minutes=2),
    bash_command='python /opt/airflow/TwitterScraper/src/twitterdatacollector.py single elonmusk /opt/airflow/data/fetchedTweets.csv',
    dag=crawl_process_dag
)

process = BashOperator(
    task_id="process",
    # bash_command='echo "Processing now"',
    bash_command='pwd',
    dag=crawl_process_dag
)

scrape >> process