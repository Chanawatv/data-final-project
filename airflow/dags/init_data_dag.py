import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.kafka_producer import load_init_data
from src.kafka_consumer import get_data

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='init_data_dag',
    default_args=args,
    schedule_interval= '@once',
	catchup=False,
)

task1 = PythonOperator(
    task_id='load_init_data',
    python_callable=load_init_data,        
    dag=dag
)

task2 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

task1 >> task2                  