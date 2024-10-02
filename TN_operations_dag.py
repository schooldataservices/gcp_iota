import sys
import os
import logging
working_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/TN_operations')
sys.path.append(working_dir)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from modules.sftp_utils import *
from modules.sftp_configs import *
from modules.sftp_ops import *


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='TN_SFTP_operations_dag',
    default_args=default_args,
    description='A DAG to handle SFTP data exchanges dynamically',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

     # Load credentials and push to XCom 
    def load_and_push_credentials(**context):
        creds = load_credentials()
        logging.info(f"Loaded credentials: {creds}")

        if creds:
            context['ti'].xcom_push(key='credentials', value=creds)
        else:
            logging.error("No credentials loaded. Cannot push to XCom.")

    #Making callable task to be called within sftp_file_exchange_task
    load_credentials_task = PythonOperator(
        task_id='load_credentials',
        python_callable=load_and_push_credentials,
    )

    configure_logging_task = PythonOperator(
    task_id='configure_logging',
    python_callable=configure_logging
    )
    
    # --------------------------------------
    for config in sftp_configs:

        task_id = f"sftp_file_exchange_{config['sftp_type']}"
        logging.info(f"Processing config for task {task_id}: {config}")

        sftp_file_exchange_task = PythonOperator(
            task_id=task_id,
            python_callable=dynamic_sftp_file_exchange(**config),
            provide_context=True
        )

        #Task dependencies
        configure_logging_task >> load_credentials_task >> sftp_file_exchange_task



#Can not let bars be green if conn is not established. 
#issue querying savvas. Need to specify the db if it does not hava  default




