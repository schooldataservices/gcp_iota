import logging
from modules.sftp_ops import SFTPConnection, SFTP_conn_file_exchange, SFTP_export_dir_to_SFTP
import json
import os
from airflow.exceptions import AirflowException

def configure_logging():
    logging.basicConfig(
        filename='logs/SFTP_operations.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S'
    )
    logging.info('\n\n-------------New SFTP Operations Logging Instance')


def load_credentials():
    json_file_path = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/TN_operations/powerschool-420113-db919282054b.json')
    current_dir = os.getcwd()
    logging.info(f"Current working directory: {current_dir}")
    logging.info(f"JSON file path: {json_file_path}")
    try:
        with open(json_file_path) as json_file:
            credentials = json.load(json_file)
        logging.info("Credentials loaded successfully.")
        return credentials
    except Exception as e:
        logging.error(f"Error loading credentials: {e}")
        raise


def setup_sftp_connection(creds, type_):


    if creds is None:
        logging.error("No credentials found in XCom. Cannot establish SFTP connection.")
        raise AirflowException("Credentials are required to establish SFTP connection.")

    connections = {
        'clever_import': SFTPConnection(creds['clever_import_host'], creds['clever_import_username'], creds['clever_import_password'], use_pool=False),
        'clever_export': SFTPConnection(creds['clever_export_host'], creds['clever_export_username'], creds['clever_export_password'], use_pool=False),
        'savva_export': SFTPConnection(creds['savva_host'], creds['savva_username'], creds['savva_password'], use_pool=False),
        'easyIEP_import': SFTPConnection(creds['easyIEP_host'], creds['easyIEP_username'], creds['easyIEP_password'], use_pool=False),
    }

    if type_ not in connections:
        raise AirflowException(f"Unknown connection type: {type_}")
    return connections[type_]





# sftp_type, import_or_export, target_sftp_folder_name, local_folder_name, db, file_to_download=None, naming_dict=None, export_local_bq_replications=False, export_sftp_folder_name=None, use_pool=False, project_id='powerschool-420113', **kwargs
def dynamic_sftp_file_exchange(**kwargs):
    def inner_dynamic_file_exchange(**context):
        
        # When Airflow runs this callable (dynamic_sftp_file_exchange), the context is automatically injected from provide_context=True.
        ti = context['ti']
        creds = ti.xcom_pull(task_ids='load_credentials', key='credentials')

        if creds is None:
            raise AirflowException("No credentials found in XCom.")
        
        sftp_type = kwargs.get('sftp_type', 'default_sftp_type')  # Retrieve sftp_type from kwargs
        sftp_conn = setup_sftp_connection(creds, type_=sftp_type)

        try:
            SFTP_conn_file_exchange(
                sftp_conn,
                **kwargs
            )

        except Exception as e:
            logging.error(f'Error during SFTP file exchange for {sftp_type} - {e}')
        
        try:
            sftp_conn.close_all_connections()
            logging.info(f'SFTP connection for {sftp_type} closed')
        except Exception as e:
            logging.error(f'Unable to close connection for {sftp_type} due to {e}')

    return inner_dynamic_file_exchange #allows dag to execute func

def close_all_sftp_connections_task(**context):
    # Retrieve all SFTP connections from XCom
    connections = {}
    for task_id in context['task_instance'].dag.get_task_ids():
        conn = context['ti'].xcom_pull(task_ids=task_id, key=f'{task_id}_conn')
        if conn:
            connections.update(conn)
    
    # Call the close_all_sftp_connections function
    close_all_sftp_connections(connections)

def close_all_sftp_connections(connections):
    for conn in connections.values():
        conn.close_all_connections()
    logging.info('All SFTP connections closed')



# dynamic_sftp_file_exchange to inner_dynamic_file_exchange to SFTP_conn_file_exchange
# to replicate_SFTP_file_to_local replicate_BQ_views_to_local needs to all be OOP
#This will reduce the need for passing variables