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





def SFTP_conn_file_exchange(sftp_conn, import_or_export, sftp_folder_name, local_folder_name=None, file_to_download=None, use_pool=False, naming_dict=None, project_id='powerschool-420113', db='powerschool_staged'):
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



#This can benefit from OOP


#SSH tunneling example for CustomPlanet
# from sshtunnel import SSHTunnelForwarder
# import pymysql

# # Establish SSH tunnel
# with SSHTunnelForwarder(
#     (ssh_host, ssh_port),
#     ssh_username=ssh_username,
#     ssh_password=ssh_password,
#     remote_bind_address=(mysql_host, mysql_port),
# ) as tunnel:
#     print(f'Tunnel local bind port: {tunnel.local_bind_port}')
#     print(f'Tunnel is active: {tunnel.is_active}')

#     # Connect to MySQL through the tunnel
#     conn = pymysql.connect(
#         host=mysql_host,
#         port=tunnel.local_bind_port,
#         user=mysql_username,
#         password=mysql_password,
#         database='opencartdb',
#         connect_timeout=30,  # Increase the connection timeout
#     )

#     orders = '''
#             SELECT order_id, firstname, lastname, email, telephone, payment_city, payment_zone, payment_country, payment_method, 
#             shipping_address_1, shipping_city, shipping_country, total, date_added, date_modified, design_file,
#             shipping_date FROM oc_order
#             '''
    
#     order_product = '''
#             SELECT order_product_id, order_id, product_id, name, model, quantity, price, total FROM oc_order_product

#                      '''
    
#     just_product = '''
#             SELECT product_id, what, image FROM oc_product

#     '''    
    

#     orders = pd.read_sql_query(orders, conn)
#     order_product = pd.read_sql_query(order_product, conn)
#     just_product = pd.read_sql_query(just_product, conn)

#     try:
#         conn.close()
#         print('conn is closed')
#     except:
#         print('conn still open')