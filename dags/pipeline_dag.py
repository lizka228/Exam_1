from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

import sys
sys.path.append("/home/airflow/")
import etl.load_data as load_data
import etl.preprocessing as preprocessing
import etl.train_model as train_model
import etl.evaluate_model as evaluate_model
import etl.save_results as save_results

import os
from dotenv import load_dotenv

load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")

def check_data_path():
    if not DATA_PATH:
        raise AirflowException("Error: DATA_PATH is not specified! STOP DAG.")

default_args = {
'owner': 'Elizaveta',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
'email': ['ladymazurina@mail.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5)
}

ml_dag = DAG( 'ml_pipeline' ,
              description= 'Pipeline Logistic Regression DAG',
              default_args=default_args,
              schedule_interval="@daily",
              start_date=datetime( 2023 , 1 , 1),
              catchup= False )

check_env_task = PythonOperator(
    task_id="check_data_path",
    python_callable=check_data_path,
    dag=ml_dag,
)

task_load = PythonOperator(task_id= 'load_data' ,
                      python_callable=load_data.load_data,
                      dag=ml_dag)

task_preprocessing = PythonOperator(task_id= 'preprocessing_data' ,
                               python_callable=preprocessing.preprocess,
                               dag=ml_dag)

task_train = PythonOperator(task_id= 'train_model' ,
                                python_callable=train_model.train_model,
                            dag=ml_dag)

task_evaluate = PythonOperator(task_id= 'evaluate_model' ,
                               python_callable=evaluate_model.evaluate_model,
                               dag=ml_dag)

task_save = PythonOperator(task_id= 'save_results' ,
                           python_callable=save_results.save_results,
                           retries=3, 
                           retry_delay=timedelta(minutes=1)
                           dag=ml_dag)


check_env_task >> task_load >> task_preprocessing >> task_train >> task_evaluate >> task_save