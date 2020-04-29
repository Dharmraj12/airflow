from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from psycopg2.extras import execute_values
from airflow.models import Variable
import requests

default_args = {
    'owner': 'Airflow',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': datetime.today().strftime('%Y-%m-%d'),
    'schedule_interval': '0 0 * * 0'
  
}

def fetch_bankruptcy_data_function(ds, **kwargs):
    last_scheduler_run = None
    last_dag_run =  dag.get_last_dagrun()
    if last_dag_run is None:
        last_scheduler_run = datetime.today().strftime('%Y-%m-%d')
    else:
        last_scheduler_run = last_dag_run.execution_date.strftime("%Y-%m-%d")
    postgresQuery = """select * from notices where date BETWEEN %s and %s;"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    postgres_conn = postgres_hook.get_conn()
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute(postgresQuery, (last_scheduler_run, datetime.today().strftime('%Y-%m-%d'))) 
    kwargs['ti'].xcom_push(key ='postgresRecords', value = postgres_cursor.fetchall())
    postgres_cursor.close()
    postgres_conn.close()
  
   
def fetch_customer_data_function(ds, **kwargs):
    mysqlQuery = "select * from customer"
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='airflow')
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(mysqlQuery)
    kwargs['ti'].xcom_push(key ='mysqlRecords', value = mysql_cursor.fetchall())
    mysql_cursor.close()
    mysql_conn.close()
def filter_bankruptcy_data_function(ds, **kwargs):
    result = []
    mysqlRecords =  kwargs['ti'].xcom_pull(key ='mysqlRecords')
    postgresRecords =  kwargs['ti'].xcom_pull(key ='postgresRecords')
    print("Customer data is ")
    for x in mysqlRecords :
        print(x)
    print("Bankcrupty  data is ")
    for x in  postgresRecords:
        print(x)
    if( postgresRecords):
        for x in  postgresRecords:
         hkId = x[2][ 0 : 4 ]
         name = x[1]
         result.extend([i for i in  mysqlRecords if i[0].startswith(hkId) and i[1]== name] )

    if len(result) == 0:
        for x in postgresRecords:
         hkId = x[2][ 0 : 4 ]
         result.extend([i for i in mysqlRecords if i[0].startswith(hkId)] )

    if len(result) > 0: 
        result = list(set([i for i in result]))
        kwargs['ti'].xcom_push(key ='matched_records', value = result)
        
    else:
        last_scheduler_run = None
        last_dag_run =  dag.get_last_dagrun()
        if last_dag_run is None:
            last_scheduler_run = datetime.today().strftime('%Y-%m-%d')
        else:
            last_scheduler_run = last_dag_run.execution_date.strftime("%Y-%m-%d")
            print("No Data found in between this date " + last_scheduler_run +" to " + datetime.today().strftime('%Y-%m-%d'))

def create_process_for_matched_data(ds, **kwargs):
    matchedRecords = kwargs['ti'].xcom_pull(key ='matched_records')
    if matchedRecords is not None and len(matchedRecords) > 0 :
        obj = {"name": "Testing 1","processDefinitionKey": "testingprocess"}
        r = requests.post('http://111.93.191.82:9595/activiti-app/api/enterprise/process-instances', auth=('admin@app.activiti.com', 'admin'),data= obj)
        # connetion  = SimpleHttpOperator(http_conn_id ="http_conn","/activiti-app/api/enterprise/process-instances","POST", data)
        print(" ststaus is " + str(r.status_code))
        
with  DAG('bankruptcy-airflow-job', default_args=default_args) as dag:
    t1 = PythonOperator(task_id='fetch-bankruptcy-latest-record', python_callable=fetch_bankruptcy_data_function, on_failure_callback=fetch_bankruptcy_data_function,
    provide_context=True, dag=dag)
    t2 = PythonOperator(task_id='fetch-all-customer-records', python_callable=fetch_customer_data_function, on_failure_callback=fetch_customer_data_function,
    provide_context=True, dag=dag)
    t3 = PythonOperator(task_id='filter-bankruptcy-records', python_callable=filter_bankruptcy_data_function, on_failure_callback=filter_bankruptcy_data_function,
    provide_context=True, dag=dag)
    t4 = PythonOperator(task_id='create-process-for-customer', python_callable=create_process_for_matched_data, on_failure_callback=create_process_for_matched_data,
    provide_context=True, dag=dag)
    t1>>t2>>t3>>t4
