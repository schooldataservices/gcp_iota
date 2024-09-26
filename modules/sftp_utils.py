import logging
from modules.sftp_ops import SFTPConnection, SFTP_conn_file_exchange, SFTP_export_dir_to_SFTP
import json
import os

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
        logging.error("Credentials are None.")
        raise ValueError("Credentials are required to establish SFTP connection.")

    connections = {
        'clever_import': SFTPConnection(creds['clever_import_host'], creds['clever_import_username'], creds['clever_import_password'], use_pool=False),
        'clever_export': SFTPConnection(creds['clever_export_host'], creds['clever_export_username'], creds['clever_export_password'], use_pool=False),
        'savva_export': SFTPConnection(creds['savva_host'], creds['savva_username'], creds['savva_password'], use_pool=False),
        'easyIEP_import': SFTPConnection(creds['easyIEP_host'], creds['easyIEP_username'], creds['easyIEP_password'], use_pool=False),
    }

    if type_ not in connections:
        raise ValueError(f"Unknown connection type: {type_}")
    return connections[type_]




def dynamic_sftp_file_exchange(sftp_type, import_or_export, target_sftp_folder_name, local_folder_name, file_to_download=None, naming_dict=None, export_local_bq_replications=False, export_sftp_folder_name=None, use_pool=False):
    def inner_dynamic_file_exchange(creds, **context):

        creds = context['ti'].xcom_pull(task_ids='load_credentials', key='credentials')
        if creds is None:
            logging.error("No credentials found in XCom. Cannot establish SFTP connection.")
            return  # Exit early if no credentials
        
        sftp_conn = setup_sftp_connection(creds, type_=sftp_type)

        # Perform file exchange
        SFTP_conn_file_exchange(
            sftp_conn,
            import_or_export=import_or_export,
            target_sftp_folder_name=target_sftp_folder_name,
            local_folder_name=local_folder_name
        )

        # Export files if needed
        if export_local_bq_replications and export_sftp_folder_name is not None:
            SFTP_export_dir_to_SFTP(local_dir=local_folder_name, remote_dir=export_sftp_folder_name, sftp=sftp_conn)

        # Push SFTP connection to XCom
        context['ti'].xcom_push(key=f'{sftp_type}_conn', value={sftp_type: sftp_conn})

    return inner_dynamic_file_exchange

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
