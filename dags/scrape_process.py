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
    # bash_command='python /opt/airflow/TwitterScraper/src/twitterdatacollector.py single elonmusk /opt/airflow/data/fetchedTweets.csv',
    bash_command='python /opt/airflow/TwitterScraper/src/twitterdatacollector.py single elonmusk /opt/airflow/data/fetchedTweets{{ts}}.csv',
    dag=crawl_process_dag
)

get_new_scrapes = BashOperator(
    task_id="get_new_scrapes",
    # bash_command='echo "Processing now"',
    # bash_command='pwd',
    # bash_command="""
    # output_dir="/opt/airflow/dataOut"
    # output_file="$output_dir/new_twits.csv"

    # # Create the output directory if it doesn't exist
    # mkdir -p "$output_dir"

    # file1=$(ls -lt /opt/airflow/data/*.csv | grep -v '^d' | awk 'NR==1{print $9}')
    # echo "File 1: $file1"
    # file2=$(ls -lt /opt/airflow/data/*.csv | grep -v '^d' | awk 'NR==2{print $9}')

    # if [[ -z "$file2" ]]; then
    #     # If there is no second file, copy the first file to the output
    #     echo "There is no second file using first one directly"
    #     cp $file1 $output_file
    #     exit 0
    # fi

    # # If there is a second file, find the difference
    # echo "File 2: $file2"
    # tail -n +2 $file1 | sort > $output_dir/first_sorted.csv
    # tail -n +2 $file2 | sort > $output_dir/second_sorted.csv
    # comm -23 $output_dir/first_sorted.csv $output_dir/second_sorted.csv > $output_file
    # (head -n 1 $file1 && cat $output_file) > $output_dir/temp.csv && mv $output_dir/temp.csv $output_file

    # """,
    bash_command="""
    output_dir="/opt/airflow/dataOut"
    output_file="$output_dir/new_twits.csv"

    # Create the output directory if it doesn't exist
    mkdir -p "$output_dir"

    file1=$(ls -lt /opt/airflow/data/*.csv | grep -v '^d' | awk 'NR==1{print $9}')
    echo "File 1: $file1"
    file2=$(ls -lt /opt/airflow/data/*.csv | grep -v '^d' | awk 'NR==2{print $9}')

    if [[ -z "$file2" ]]; then
        # If there is no second file, copy the first file to the output
        echo "There is no second file using first one directly"
        cp $file1 $output_file
        exit 0
    fi

    # If there is a second file, find the difference
    echo "File 2: $file2"
    awk -F ',' 'NR==FNR{a[$2]; next} !($2 in a)' <(cut -d ',' -f 2 $file1| sort -u) <(cut -d ',' -f 2 $file2 | sort -u) > $output_file
    (head -n 1 $file1 && cat $output_file) > $output_dir/temp.csv && mv $output_dir/temp.csv $output_file

    """,
    dag=crawl_process_dag
)
process = BashOperator(
    task_id="process",
    # bash_command='echo "Processing now"',
    bash_command='pwd',
    dag=crawl_process_dag
)

scrape >> get_new_scrapes >> process