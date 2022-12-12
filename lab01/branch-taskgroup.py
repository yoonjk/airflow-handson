from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup 

from datetime import timedelta 
from airflow.utils.dates import days_ago 
from random import uniform 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

def _training_model(ti):
  accuracy = uniform(0.1, 10.0)
  print(f'model\'s accuracy:{accuracy}')
  print('accuracy:{accuracy}')
  ti.xcom_push(key='model_accuracy', value=accuracy)


def _choose_best_model(ti):
  accuracies = ti.xcom_pull(key = 'model_accuracy', task_ids=[
    'processing_tasks.training_model_a',
    'processing_tasks.training_model_b',
    'processing_tasks.training_model_c'
  ])
  models = ['a', 'b', 'c']
  print(accuracies)
  accuracies = zip(models, accuracies)

  bestModel = 5; 
  for model, accuracy in accuracies:
    if accuracy > bestModel:
      bestModel = accuracy
      ti.xcom_push(key='best_model', value=model)
  
  if ti.xcom_pull(key='best_model') != None:
    print('bestModel:', ti.xcom_pull(key='best_model'))
    return 'accurate'
  else: 
    return 'inaccurate'
  
def _dump(**context):
  xcom_push_val = context['ti'].xcom_pull(key = 'best_model')
  print('best_model:', xcom_push_val)
    
with DAG(
  dag_id = 'branch-taskgroup',
  default_args=default_args,
  tags = ['training']
) as dag: 
  downloading_data = BashOperator(
    task_id = 'downloading_data',
    do_xcom_push = False,
    bash_command = 'sleep 3'
  )
  
  with TaskGroup(group_id='processing_tasks') as group1:
    training_model_a = PythonOperator(
      task_id = 'training_model_a',
      python_callable = _training_model
    )
    training_model_b = PythonOperator(
      task_id = 'training_model_b',
      python_callable = _training_model
    )
    training_model_c = PythonOperator(
      task_id = 'training_model_c',
      python_callable = _training_model
    )
    
  choose_best_model = python_task = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
  )
    
  accurate =PythonOperator(task_id='accurate', python_callable = _dump)
  inaccurate = PythonOperator(task_id='inaccurate', python_callable =  _dump)

  start = EmptyOperator(task_id = 'start')
  end = EmptyOperator(task_id = 'end', trigger_rule = 'one_success')
  start >> downloading_data >> group1 >> choose_best_model >> [accurate, inaccurate] >> end

