from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
   'owner': 'broskii',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}

with DAG(
  dag_id='catchup_and_backfill_v2',
  default_args=default_args,
  description='description',
  start_date=datetime(2024, 7, 31),
  schedule_interval='@daily',
  catchup=False,
# catchup=True
) as dag:
  task1 = BashOperator(
      task_id='task_one',
      bash_command='echo task 1'
   )
  
# Backfill Command
# $ docker ps 

# then copy the container id of scheduler then run
# $ docker exec -it <your_scheduler_container_id> bash

# then run 
# $ airflow dags backfill -s <start_date> -e <end_date> <your_dag_id>
# $ airflow dags backfill -s 2024-07-30 -e 2024-08-02 catchup_and_backfill_v2 # for this example
# termial with show 'INFO - Backfill done for DAG <DAG: catchup_and_backfill_v2>. Exiting.

# to exit container run
# $ exit

# Airflow Catchup & Backfill — Demystified, Author: Amit Singh Rathore
# https://medium.com/nerd-for-tech/airflow-catchup-backfill-demystified-355def1b6f92