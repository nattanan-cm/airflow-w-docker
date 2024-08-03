from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
   'owner': 'broskii',
   'retires': 5,
   'retry_delay': timedelta(minutes=2)
}

with DAG(
  dag_id='postgres_operator_v3',
  default_args=default_args,
  description='description',
  start_date=datetime(2024, 7, 31),
  schedule_interval='0 0 * * *',
) as dag:
   create_task = PostgresOperator(
     task_id='create_postgres_table',
     postgres_conn_id='postgres_localhost',
     sql="""
         CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id character varying,
            PRIMARY KEY (dt, dag_id)
         )
     """
   )
  
   insert_task = PostgresOperator(
     task_id='insert_into_table',
     postgres_conn_id='postgres_localhost',
     sql="""
         INSERT INTO dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
     """
   )
   
   delete_task = PostgresOperator(
      task_id='delete_from_table',
      postgres_conn_id='postgres_localhost',
      sql="""
         DELETE FROM dag_runs WHERE dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'
      """
   )
  
   create_task >> delete_task >> insert_task