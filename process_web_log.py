#imports
from airflow import DAG
from airflow.operators.bash import BashOperator 
import datetime as dt 

# dag arguments/instantiate 
default_args = {
    'owner': 'walker_martin',
    'start_date': dt.datetime(2024, 4, 21),
    'email': "dummy_email@example.com",
    'email_on_failure': 'True',
    'email_on_retry': 'True',
    'retries': 1,
    'retry_delay':dt.timedelta(minutes=5),
}

# create dag definitions
with DAG(
    dag_id = 'process_web_log',
    schedule_interval = dt.timedelta(days=1),
    default_args = default_args,
    description = "ETL web log data pipeline",
) as dag:

    # extract data
    extract_data = BashOperator(
        task_id = 'extract_data',
        bash_command = 'awk \'{print $1}\' /home/project/airflow/dags/accesslog.txt > /home/project/airflow/dags/extracted_data.txt',
        dag=dag 
    )

    # transform data
    transform_data = BashOperator(
        task_id = 'transform_data',
        bash_command = 'grep -v "198.46.149.143" /home/project/airflow/dags/extracted_data.txt > /home/project/airflow/dags/transformed_data.txt',
        dag=dag 
    )

    # load data
    load_data = BashOperator(
        task_id = 'load_data',
        bash_command = 'tar -czvf /home/project/airflow/dags/weblog.tar.gz /home/project/airflow/dags/transformed_data.txt',
        dag=dag 
    )

extract_data >> transform_data >> load_data