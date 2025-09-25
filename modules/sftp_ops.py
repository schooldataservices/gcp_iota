import pandas as pd
import pandas_gbq
import pysftp
import shutil
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

    def __init__(self, host, username, password, port=22, max_connections=5, use_pool=True, cnopts=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = port  # Add port to the instance
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
            port=self.port,  # Use the port here
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




import os
import logging

def replicate_SFTP_file_to_local(sftp, sftp_folder_name, local_folder_name, file_to_download=None, naming_dict=None):
    os.makedirs(local_folder_name, exist_ok=True)

    try:
        sftp.chdir(sftp_folder_name)
        dir_contents = sftp.listdir()
        logging.info(f'Dir contents of {sftp_folder_name}: {dir_contents}')

        if file_to_download:
            if file_to_download in dir_contents:
                remote_file_path = os.path.join(sftp_folder_name, file_to_download)

                #reform local_file_path specifically for easyIEP. Singular file rename that references the dictionary passed in
                dict_name = naming_dict.get(file_to_download)

                local_file_path = os.path.join(local_folder_name, dict_name)

                logging.info(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')
                print(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')

                sftp.get(file_to_download, local_file_path)
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
                print(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')

                sftp.get(file_name, local_file_path)
                logging.info(f'File "{file_name}" downloaded to local directory "{local_file_path}"')

            logging.info(f'All files in folder "{sftp_folder_name}" downloaded to local directory "{local_folder_name}"')

    except Exception as e:
        logging.error(f'An error occurred during file replication: {e}')





def SFTP_conn_file_exchange(sftp_conn, import_or_export, sftp_folder_name, db=None, local_folder_name=None, file_to_download=None, use_pool=False, naming_dict=None, project_id='powerschool-420113'):
    conn = None

    try:
        if use_pool:
            conn = sftp_conn.get_connection()
            logging.info('SFTP connection established successfully from pool')
        else:
            conn = sftp_conn._create_new_connection()
            logging.info('\n\nSFTP singular connection established successfully')

        # import pulls over google passwords from Clever, export sends GCP views over
        if import_or_export == 'import':
            replicate_SFTP_file_to_local(conn, sftp_folder_name, local_folder_name, file_to_download, naming_dict)

        elif import_or_export == 'export':
            replicate_BQ_views_to_local(sftp_folder_name, project_id, db, naming_dict)
        else:
            logging.error('Wrong variable for import or export')

    except Exception as e:
        logging.error(f'Failed to establish SFTP connection due to error: {e}')   

    finally:
        if conn:
            if use_pool:
                sftp_conn.return_connection(conn)  # Return the connection to the pool
                logging.info('SFTP connection returned to pool')
            else:
                conn.close()
                logging.info('SFTP singular connection closed')





def replicate_BQ_views_to_local(sftp_folder_name, project_id, db, naming_dict):
    """
    Function to send files to SFTP server from BigQuery tables.
    
    Parameters:
    - sftp: pysftp.Connection object for SFTP connection.
    - dictionary_naming: Dictionary mapping table names to remote filenames.
    - sftp_folder_name: Remote folder name on SFTP server.
    - project_id: Google Cloud project ID for BigQuery.
    """
    logging.info(f'Replicating BQ views to local dir from the {db} db')

    # Iterate over dictionary of table names and filenames
    for table_name, remote_filename in naming_dict.items():

        # Query BigQuery table
        query = f"""
        SELECT * FROM `{project_id}.{db}.{table_name}`
        """
        try:
            # Execute the query and store the result in a DataFrame
            df = pandas_gbq.read_gbq(query, project_id=project_id)
        except Exception as e:
            logging.error(f'Error querying table "{table_name}": {str(e)}')
            continue

        # Create nested folder based on sftp_folder_name
        os.makedirs(sftp_folder_name, exist_ok=True)
        logging.info('Directory sftp_file_transfer created or already exists')

        # Write files to local dir
        local_path = os.path.join(sftp_folder_name, remote_filename)
        try:
            df.to_csv(local_path, index=False)
            logging.info(f'File {table_name} being written to local dir {local_path}')
        except Exception as e:
            logging.error(f'File {table_name} unable to be written to local dir {local_path}: {str(e)}')
            



def SFTP_export_dir_to_SFTP(local_dir, remote_dir, sftp):
    conn = sftp._create_new_connection()
    logging.info('\n\nSFTP singular connection established successfully')

    logging.info(f'Local dir: {local_dir}')
    logging.info(f'Remote dir: {remote_dir}')

    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            remote_path = os.path.join(remote_dir, relative_path).replace('\\', '/')

            # Print paths for debugging
            print(f"Local path: {local_path}")
            print(f"Remote path: {remote_path}")

            # Log paths for debugging
            logging.info(f"Local path: {local_path}")
            logging.info(f"Remote path: {remote_path}")

            # Check if the file exists
            if not os.path.exists(local_path):
                print(f"File not found: {local_path}")
                logging.error(f"File not found: {local_path}")
                continue

            # Upload the file
            try:
                conn.put(local_path, remote_path)
                print(f"Uploaded {local_path} to {remote_path}")
                logging.info(f"Uploaded {local_path} to {remote_path}")
            except Exception as e:
                print(f"Error uploading {local_path} to {remote_path}: {str(e)}")
                logging.error(f"Error uploading {local_path} to {remote_path}: {str(e)}")

    # Close the connection after the operation
    conn.close()
    logging.info('SFTP singular connection closed')




def copy_newest_savvas_files(source_dir, target_dir, file_prefixes):
    """
    Copies the newest files that start with specific prefixes from the source directory to the target directory.

    :param source_dir: Directory to search for files.
    :param target_dir: Directory to copy the files to.
    :param file_info: A dictionary with keys as file prefixes and values as destination filenames.
    """
    # Check if source and target directories exist
    if not os.path.exists(source_dir):
        logging.error(f"Source directory '{source_dir}' does not exist.")
        raise FileNotFoundError(f"Source directory '{source_dir}' does not exist.")
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        logging.info(f'Creating {target_dir} for the first time')

    # Loop through each prefix and destination filename in file_info
    for prefix, filename in file_prefixes.items():
        # Find all files that start with the specified prefix
        files_with_prefix = [
            os.path.join(source_dir, f) for f in os.listdir(source_dir)
            if os.path.isfile(os.path.join(source_dir, f)) and f.startswith(prefix)
        ]
        
        # Find the most recent file for the current prefix
        if not files_with_prefix:
            logging.error(f"No files found in the source directory with prefix '{prefix}'.")
            raise FileNotFoundError(f"No files found in the source directory with prefix '{prefix}'.")
        
        newest_file = max(files_with_prefix, key=os.path.getmtime)
        new_file_path = os.path.join(target_dir, filename)

        # Copy the newest file to the target directory with the specified filename
        shutil.copy2(newest_file, new_file_path)
        logging.info(f"Copied '{newest_file}' to '{new_file_path}'.")


def add_quotes_to_column_names(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Add quotes around column names
                df.columns = ['"' + col + '"' for col in df.columns]
                
                # Save the file back to the same location
                df.to_csv(file_path, index=False)
                logging.info(f"Processed: {filename}")
            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")