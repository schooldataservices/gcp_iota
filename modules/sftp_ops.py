import pandas as pd
import pandas_gbq
import pysftp
import logging
import time
import os
from queue import Queue
from threading import Lock
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

def clear_logging_handlers():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


class SFTPConnection:
    default_cnopts = pysftp.CnOpts()
    default_cnopts.hostkeys = None

    def __init__(self, host, username, password, max_connections=5, use_pool=True, cnopts=None):
        self.host = host
        self.username = username
        self.password = password
        self.max_connections = max_connections
        self.use_pool = use_pool
        self.cnopts = cnopts or self.default_cnopts
        if use_pool:
            self.pool = Queue(max_connections)
            self.lock = Lock()
            self._initialize_pool()
        else:
            self.pool = None
            self.lock = None

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            self.pool.put(self._create_new_connection())

    def _create_new_connection(self):
        return pysftp.Connection(
            host=self.host,
            username=self.username,
            password=self.password,
            cnopts=self.cnopts
        )

    def get_connection(self):
        if self.use_pool:
            with self.lock:
                if self.pool.empty():
                    return self._create_new_connection()
                return self.pool.get()
        else:
            return self._create_new_connection()

    def return_connection(self, conn):
        if self.use_pool:
            with self.lock:
                if self.pool.full():
                    conn.close()
                else:
                    self.pool.put(conn)
        else:
            conn.close()

    def close_all_connections(self):
        if self.use_pool:
            while not self.pool.empty():
                conn = self.pool.get()
                conn.close()
                logging.info('SFTP connection closed')





def replicate_SFTP_file_to_local(sftp_conn, sftp_folder_name, local_folder_name, file_to_download=None, naming_dict=None):
    os.makedirs(local_folder_name, exist_ok=True)

    try:
        sftp_conn.chdir(sftp_folder_name)
        dir_contents = sftp_conn.listdir()
        logging.info(f'Dir contents of {sftp_folder_name}: {dir_contents}')

        if file_to_download:
            if file_to_download in dir_contents:
                remote_file_path = os.path.join(sftp_folder_name, file_to_download)

                #reform local_file_path specifically for easyIEP. Singular file rename that references the dictionary passed in
                dict_name = naming_dict.get(file_to_download)

                local_file_path = os.path.join(local_folder_name, dict_name)

                logging.info(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')

                sftp_conn.get(file_to_download, local_file_path)
                logging.info(f'File "{file_to_download}" downloaded to local directory "{local_file_path}"')
            else:
                logging.warning(f'File "{file_to_download}" not found in directory "{sftp_folder_name}".')
        else:
            if not dir_contents:
                logging.info(f'No files to download in folder "{sftp_folder_name}".')
                return

            logging.info('No specification for file_to_download as a standalone, assuming all files need to be downloaded')

            for file_name in dir_contents:
                remote_file_path = os.path.join(sftp_folder_name, file_name)
                local_file_path = os.path.join(local_folder_name, file_name)

                logging.info(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')

                sftp_conn.get(file_name, local_file_path)
                logging.info(f'File "{file_name}" downloaded to local directory "{local_file_path}"')

            logging.info(f'All files in folder "{sftp_folder_name}" downloaded to local directory "{local_folder_name}"')

    except Exception as e:
        logging.error(f'An error occurred during file replication: {e}')





def SFTP_conn_file_exchange(sftp_conn, **kwargs):
    conn = None #For loops

  
    # Access parameters using kwargs.get()
    import_or_export = kwargs.get('import_or_export')
    target_sftp_folder_name = kwargs.get('target_sftp_folder_name')
    local_folder_name = kwargs.get('local_folder_name', None)
    file_to_download = kwargs.get('file_to_download', None)
    use_pool = kwargs.get('use_pool', False)
    naming_dict = kwargs.get('naming_dict', None)
    db = kwargs.get('db', None)
    export_local_bq_replications = kwargs.get('export_local_bq_replications', None)
    export_sftp_folder_name = kwargs.get('export_sftp_folder_name', None)
    project_id = 'powerschool-420113'

    # Logging for debugging
    logging.info(f"import_or_export: {import_or_export}")
    logging.info(f"target_sftp_folder_name: {target_sftp_folder_name}")
    logging.info(f"local_folder_name: {local_folder_name}")
    logging.info(f"file_to_download: {file_to_download}")
    logging.info(f"use_pool: {use_pool}")
    logging.info(f"naming_dict: {naming_dict}")
    logging.info(f"db: {db}")
    logging.info(f"export_local_bq_replications: {export_local_bq_replications}")
    logging.info(f"export_sftp_folder_name: {export_sftp_folder_name}")
    logging.info(f"use_pool: {use_pool}")
    logging.info(f"project_id: {project_id}")

    try:
        if use_pool:
            conn = sftp_conn.get_connection()
            logging.info('\n\n\nSFTP connection established successfully from pool')
        else:
            logging.info(f'\n\n\nAttempting to create new connection')
            conn = sftp_conn._create_new_connection()
            logging.info('SFTP singular connection established successfully within SFTP_conn_file_exchange')

           # import pulls over google passwords from Clever, export sends GCP views over
        if import_or_export == 'import':
            logging.info('Attempting to replicate SFTP file to local')
            replicate_SFTP_file_to_local(conn, target_sftp_folder_name, local_folder_name, file_to_download, naming_dict)

        elif import_or_export == 'export':
            logging.info('Attempting to replicate BQ views to local')
            replicate_BQ_views_to_local(local_folder_name, project_id, db, naming_dict)  
        else:
            logging.error('Wrong variable for import or export')

        # If true, and folder specified. Currently only for Savva and clever exports
        if export_local_bq_replications and export_sftp_folder_name is not None:
            try:
                logging.info(f'Attempting to export local bq replications to {export_sftp_folder_name}')
                SFTP_export_dir_to_SFTP(local_folder_name, export_sftp_folder_name, sftp=sftp_conn)
            except Exception as e:
                logging.error(f'Error during SFTP_export_dir_to_SFTP as {e}')
        else:
            logging.info(f'export_local_bq_replications value of {export_local_bq_replications} and export_sftp_folder_name value of {export_sftp_folder_name} preventing the SFTP_export_dir_to_SFTP function from being triggered')
    
    except Exception as e:
        logging.error(f'Process failed due to error: {e}')   

    finally:
        if conn:
            if use_pool:
                sftp_conn.return_connection(conn)  # Return the connection to the pool
                logging.info('SFTP connection returned to pool')
            else:
                conn.close()
                logging.info('SFTP singular connection closed')





def replicate_BQ_views_to_local(local_folder_name, project_id, db, naming_dict):
    """
    Function to send files to SFTP server from BigQuery tables.
    
    Parameters:
    - sftp: pysftp.Connection object for SFTP connection.
    - dictionary_naming: Dictionary mapping table names to remote filenames.
    - sftp_folder_name: Remote folder name on SFTP server.
    - project_id: Google Cloud project ID for BigQuery.
    """

    # Iterate over dictionary of table names and filenames
    for table_name, remote_filename in naming_dict.items():

        # Query BigQuery table
        query = f"""SELECT * FROM `{project_id}.{db}.{table_name}`"""
        try:
            # Execute the query and store the result in a DataFrame
            df = pandas_gbq.read_gbq(query, project_id=project_id)
            logging.info(f'Reading {query} from BQ')
        except Exception as e:
            logging.error(f'Error querying table "{table_name}": {str(e)}')
            continue
        
        # Create nested folder based on sftp_folder_name
        try:
            # Check if directory exists before trying to create it
            if not os.path.exists(local_folder_name):
                os.makedirs(local_folder_name)
                logging.info(f'Directory {local_folder_name} was created for the first time.')
            else:
                logging.info(f'Directory {local_folder_name} already exists.')
        except Exception as e:
            logging.error(f'Unable to create {local_folder_name} due to {str(e)}')

  
        # Write files to local dir
        local_path = os.path.join(local_folder_name, remote_filename)
        try:
            df.to_csv(local_path, index=False)
            logging.info(f'Big Query table - {table_name} being replicated to local dir {local_path}')
        except Exception as e:
            logging.error(f'Big Query table - {table_name} unable to be replicated to local dir {local_path}: {str(e)}')
            



def SFTP_export_dir_to_SFTP(local_folder_name, export_sftp_folder_name, sftp):
    conn = sftp._create_new_connection()
    logging.info('\n\n\nSFTP singular connection established successfully')

    #test if local_folder_name has files first
    if not os.listdir(local_folder_name):  # Check if the folder is empty
        logging.warning(f'The local folder {local_folder_name} is empty. No files to export to {export_sftp_folder_name}')
        return

    for root, _, files in os.walk(local_folder_name):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_folder_name)
            remote_path = os.path.join(export_sftp_folder_name, relative_path).replace('\\', '/')


            # Log paths for debugging
            logging.info(f"Local path: {local_path}")
            logging.info(f"Remote path: {remote_path}")

            # Check if the file exists
            if not os.path.exists(local_path):
                logging.error(f"File not found: {local_path}")
                continue

            # Upload the file
            try:
                conn.put(local_path, remote_path)
                logging.info(f"Uploaded {local_path} to {remote_path}")
            except Exception as e:
                logging.error(f"Error uploading {local_path} to {remote_path}: {str(e)}")

    # Close the connection after the operation
    conn.close()
    logging.info('SFTP singular connection closed')



#This can benefit from OOP

