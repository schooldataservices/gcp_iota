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

    # Task 1: Configure logging
    configure_logging_task = PythonOperator(
        task_id='configure_logging',
        python_callable=configure_logging
    )

        # Task 2: Load credentials and push to XCom
    def load_and_push_credentials(**context):
        creds = load_credentials()
        logging.info(f"Loaded credentials: {creds}")

        if creds:
            context['ti'].xcom_push(key='credentials', value=creds)
        else:
            logging.error("No credentials loaded. Cannot push to XCom.")
            
    load_credentials_task = PythonOperator(
        task_id='load_credentials',
        python_callable=load_and_push_credentials,
    )
    
    close_connections_task = PythonOperator(
        task_id='close_connections',
        python_callable=close_all_sftp_connections_task,
        provide_context=True
    )

    sftp_exchange_tasks = []

    for config in sftp_configs:
        task_id = f"sftp_file_exchange_{config['sftp_type']}"

        # Extract necessary arguments from the config dictionary
        sftp_type = config.get('sftp_type')
        import_or_export = config.get('import_or_export')
        target_sftp_folder_name = config.get('target_sftp_folder_name')
        local_folder_name = config.get('local_folder_name')
        file_to_download = config.get('file_to_download', None)
        naming_dict = config.get('naming_dict', None)
        export_local_bq_replications = config.get('export_local_bq_replications', False)
        export_sftp_folder_name = config.get('export_sftp_folder_name', None)
        use_pool = config.get('use_pool', False)

        # Call dynamic_sftp_file_exchange and pass the required arguments
        sftp_file_exchange_task = PythonOperator(
            task_id=task_id,
            python_callable=dynamic_sftp_file_exchange(
                sftp_type,
                import_or_export,
                target_sftp_folder_name,
                local_folder_name,
                file_to_download,
                naming_dict,
                export_local_bq_replications,
                export_sftp_folder_name,
                use_pool
            ),  # This will return the inner function
            op_args=[load_credentials_task.output],
        )

        sftp_exchange_tasks.append(sftp_file_exchange_task)
        configure_logging_task >> load_credentials_task >> sftp_file_exchange_task

    # Task dependencies
    for task in sftp_exchange_tasks:
        task >> close_connections_task


#easy IEP import is downloading everything for some reason. 
#Should be fixed, as long as file_to_download is coming across

#Also need to make sure connection was established for clever export
#Connectino was not established for savva. 
#Can not let these be green if conn is not established. 

# TypeError: close_all_sftp_connections() missing 1 required positional argument: 'connections'
#Should be fixed

#By pushing the SFTP_conns via x com and attempting to close all at once it has made it too complicated. 
#This has also created an issue with serialization of the SFTP_conn variable. Do not try and push x coms here
#This is because inner_dynamic_file_exchange takes creds as the arg. 

