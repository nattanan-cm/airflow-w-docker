from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
   'owner': 'broskii',
   'retires': 5,
   'retry_delay': timedelta(minutes=2)
}

def sklearn_version():
   import sklearn
   
   print(f"scikit-learn with version: {sklearn.__version__}")
   
def pandas_version():
   import pandas
   print(f"pandas with version: {pandas.__version__}")

with DAG(
  dag_id='python_dependencies_v1',
  default_args=default_args,
  description='description',
  start_date=datetime(2024, 7, 31),
  schedule_interval='@daily',
) as dag:
  get_sklearn = PythonOperator(
     task_id='get_sklearn',
     python_callable=sklearn_version
  )
  
  get_pandas = PythonOperator(
     task_id='get_pandas',
     python_callable=pandas_version
  )
  
  get_sklearn >> get_pandas