from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
   'owner': 'broskii',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}


# Xcom
# - purpose : XComs enable tasks to share data such as file paths, configuration, and small results.
# - identification: Each XCom is identified by a unique key, along with the task_id and dag_id it originated from.
# - size limit: Designed for small data payloads. For larger data, consider external storage like S3 or HDFS.

### Caution: Never use Xcom to share large data for example pandas dataframe, othewise it will crash.

def greet(ti):
   fname = ti.xcom_pull(task_ids='get_name', key='first_name')
   lname = ti.xcom_pull(task_ids='get_name', key='last_name')
   age = ti.xcom_pull(task_ids='get_age', key='age')
   print(f'Hello World! My name is {fname} {lname}, and I am {age} years old.')

def getName(ti):
   ti.xcom_push(key='first_name', value="Jake")
   ti.xcom_push(key='last_name', value="Rory")
   
def getAge(ti):
   ti.xcom_push(key='age', value=20)

with DAG(
   default_args=default_args,
   dag_id='dag_w_python_operator_v06',
   description='Dag using python operator',
   start_date=datetime(2024, 7, 31),
   schedule_interval="@daily",
) as dag:
   task1 = PythonOperator(
      task_id='greet',
      python_callable=greet,
   )
   
   task2 = PythonOperator(
      task_id='get_name',
      python_callable=getName
   )
   
   task3 = PythonOperator(
      task_id='get_age',
      python_callable=getAge
   )
   
   [task2, task3] >> task1