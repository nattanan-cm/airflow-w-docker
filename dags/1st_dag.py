from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
   'owner': 'broskii',
   'retires': 5,
   'retry_delay': timedelta(minutes=2)
}

with DAG(
   dag_id='1st_dag',
   default_args=default_args,
   description='this is my 1st dag',
   start_date=datetime(2024, 8, 1, 2),
   schedule_interval='@daily', # Run once a day at midnight
   # schedule_interval='None', # Don’t schedule, use for exclusively “externally triggered” DAGs
   # schedule_interval='@once', # Schedule once and only once
   # schedule_interval='@hourly', # Run once an hour at the beginning of the hour
   # schedule_interval='@weekly', # Run once a week at midnight on Sunday morning
   # schedule_interval='@monthly', # Run once a month at midnight of the first day of the month
   # schedule_interval='@yearly', # Run once a year at midnight of January 1
) as dag:
   task1 = BashOperator(
      task_id='task_one',
      bash_command='echo task 1'
   )
   
   task2 = BashOperator(
      task_id='task_two',
      bash_command="echo task 2 will be running after task 1"
   )
   
   task3 = BashOperator(
      task_id='task_three',
      bash_command="echo task 3 will be running after task 1 at the same time of task 2"
   )
   
   # Task dependency method 1
   # task1.set_downstream(task2)
   # task1.set_downstream(task3)
   
   # Task dependency method 2
   # task1 >> task2
   # task1 >> task3
   
   # Task dependency method 2
   task1 >> [task2, task3]