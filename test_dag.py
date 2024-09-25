from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from modules.sftp_utils import configure_logging, load_credentials, setup_sftp_connection, close_all_sftp_connections
from modules.sftp_ops import SFTP_conn_file_exchange, SFTP_export_dir_to_SFTP
from modules.sftp_configs import sftp_configs

# Configure the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Create the DAG instance
with DAG(
    dag_id='sftp_data_exchange_dag',
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

    # Task 2: Load credentials
    load_credentials_task = PythonOperator(
        task_id='load_credentials',
        python_callable=load_credentials
        
    )

    # This will ensure that all connections are closed at the end of the DAG
    close_connections_task = PythonOperator(
        task_id='close_connections',
        python_callable=close_all_sftp_connections,
        op_args=[{config['sftp_type']: None for config in sftp_configs}]  # Placeholder for connections
    )


SFTP_conn_file_exchange(sftp_conn_clever_import,
                    import_or_export = 'import',
                    sftp_folder_name='idm-sensitive-exports', 
                    local_folder_name=r'S:\SFTP\powerschool_combined',
                    use_pool=False)

#import_sftp_folder_name is local_folder_name

# Task 3: Dynamic SFTP setup and file exchange
def dynamic_sftp_file_exchange(sftp_type, import_or_export, import_sftp_folder_name, local_folder_name, export_local_bq_replications=False, export_sftp_folder_name=None):

    def inner_dynamic_file_exchange(creds):
        sftp_conn = setup_sftp_connection(creds, type_=sftp_type)
        SFTP_conn_file_exchange(
            sftp_conn,
            import_or_export=import_or_export,
            sftp_folder_name=import_sftp_folder_name,
            local_folder_name=local_folder_name,
            use_pool=False
        )
        if export_local_bq_replications and export_sftp_folder_name is not None:
            SFTP_export_dir_to_SFTP(local_dir=local_folder_name, remote_dir=export_sftp_folder_name, sftp=sftp_conn)
        
        close_all_sftp_connections({sftp_type: sftp_conn}) #close all cons at the end

    return inner_dynamic_file_exchange

# Collect all SFTP exchange tasks
sftp_exchange_tasks = []

# Dynamically create tasks for each SFTP configuration
for config in sftp_configs:
    task_id = f"sftp_file_exchange_{config['sftp_type']}"
    sftp_file_exchange_task = PythonOperator(
        
        task_id=task_id,
        python_callable=dynamic_sftp_file_exchange(
            sftp_type=config['sftp_type'],
            import_or_export=config['import_or_export'],
            sftp_folder_name=config['sftp_folder_name'],
            local_folder_name=config['local_folder_name'],
            remote_dir=config.get('remote_dir')  # Pass remote_dir if it exists
        ),
        
        op_args=[load_credentials_task.output]
    )
    sftp_exchange_tasks.append(sftp_file_exchange_task)
    configure_logging_task >> load_credentials_task >> sftp_file_exchange_task


# Ensure all SFTP tasks are completed before closing connections
sftp_exchange_tasks >> close_connections_task
