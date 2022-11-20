from airflow import DAG  
from airflow.operators.empty import EmptyOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Date utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

dag = DAG(
  dag_id = 'caller_trigger_dag',
  default_args = default_args,
  tags=['training']
)

trigger = TriggerDagRunOperator(
  task_id='trigger_dagrun',
  trigger_dag_id='callee_dag',
  conf={'message': 'Hello world'},
  dag=dag
)   

start = EmptyOperator(
  task_id = 'start',
  dag=dag 
)

end = EmptyOperator(
  task_id = 'end',
  dag=dag
)

start >> trigger >>  end 