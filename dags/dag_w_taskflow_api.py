from airflow.decorators import dag, task
from datetime import datetime, timedelta


default_args = {
   'owner': 'broskii',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}

@dag(
   dag_id='dag_with_taskflow_api_v2',
   default_args=default_args,
   start_date=datetime(2024, 7, 31),
   schedule_interval="@daily",
)
def theater_ads_etl():

   # @task()
   # def getMoiveName():
   #    return 'Deadpool & Wolverine'
   
   @task(multiple_outputs=True)
   def getMoiveName():
      return {'new':'Deadpool & Wolverine', 'old': 'Deadpool & Friend'}
   
   @task()
   def getReleaseDate():
      return 'July 26, 2024'
      
   @task()
   def movie(newName, oldName, releaseDate):
      print(f'{newName} was originally titled "{oldName}" and will be released in theaters on {releaseDate}.')
         
         
   name_dict = getMoiveName()
   releaseDate = getReleaseDate()
   movie(newName=name_dict['new'], oldName=name_dict['old'], releaseDate=releaseDate)
   
theater_dag = theater_ads_etl()

