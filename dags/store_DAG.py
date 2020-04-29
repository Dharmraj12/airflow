from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'Airflow',
	'depends_on_past': False,
    'start_date': datetime(2020, 2, 17),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_dag12',default_args=default_args,schedule_interval='@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:
    t1=BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
    t2=PythonOperator(task_id='clean_raw_csv', python_callable = data_cleaner)
    t3=MySqlOperator(task_id = 'create_mysql_table', mysql_conn_id = "mysql_conn", sql = "create_table.sql")
    t4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id="mysql_conn", sql="insert_into_table.sql")
    t1>>t2>>t3>>t4