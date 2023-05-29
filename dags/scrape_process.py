import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

crawl_process_dag = DAG(
    dag_id="crawl_process_dag",
    # start_date=datetime.datetime(2021, 1, 1),
    # schedule="@daily",
    start_date=datetime.datetime.now(),
    schedule="*/2 * * * *",
    dagrun_timeout=datetime.timedelta(seconds=5)
)
scrape = BashOperator(
    task_id="scrape",
    bash_command='echo "Scraping now" ',
    dag=crawl_process_dag
)

process = BashOperator(
    task_id="process",
    bash_command='echo "Processing now"',
    dag=crawl_process_dag
)

scrape >> process