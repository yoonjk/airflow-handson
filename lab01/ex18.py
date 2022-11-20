
from airflow import DAG 
from airflow.operators.python import PythonOperator 

# Utils
from airflow.utils.dates import days_ago 
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta  

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '*/5 * * * *',
  'catchup': False,
  'description': '샘플 dag'
}
def hello() -> None:
    print('Hello')
    sleep(3)
    
with DAG(
  dag_id='ex_group',
  default_args=default_args 
) as dag:

  start = PythonOperator(
      task_id='start',
      python_callable=hello,
      dag=dag
  )

  end = PythonOperator(
      task_id='end',
      python_callable=hello,
      dag=dag
  )

  with TaskGroup(group_id='group_1') as sampleGroup:
    task1 = PythonOperator(
      task_id = 'task_1',
      python_callable=hello,
    )

    task2 = PythonOperator(
      task_id = 'task_2',
      python_callable=hello
    )

    task3 = PythonOperator(
      task_id = 'task_3',
      python_callable=hello
    )
    
    task4 = PythonOperator(
      task_id = 'task_4',
      python_callable=hello
    )

    with TaskGroup(group_id='inner_group_1') as innerGroup:
      task5 = PythonOperator(
        task_id = 'task_5',
        python_callable=hello
      )

      task6= PythonOperator(
        task_id = 'task_6',
        python_callable=hello
      )

      task7= PythonOperator(
        task_id = 'task_7',
        python_callable=hello
      )      
      task5 >> [task6, task7]

    task1 >> innerGroup >> [task2, task3] >> task4

  start >> sampleGroup >> end