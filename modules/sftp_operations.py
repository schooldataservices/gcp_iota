import pandas as pd
import pysftp
import logging
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




def replicate_SFTP_files_to_local(sftp, sftp_folder_name, local_folder_name):
    os.makedirs(local_folder_name, exist_ok=True)

    try:
        sftp.chdir(sftp_folder_name)
        dir_contents = sftp.listdir()
        logging.info(f'Dir contents of {sftp_folder_name}: {dir_contents}')

        if not dir_contents:
            logging.info(f'No files to download in folder "{sftp_folder_name}".')
            return

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



def SFTP_conn_file_transfer(sftp_folder_name, local_folder_name, sftp_pool, use_pool):
    sftp = None

    try:
        if use_pool:
            sftp = sftp_pool.get_connection()
            logging.info('SFTP connection established successfully from pool')
        else:
            sftp = sftp_pool._create_new_connection()
            logging.info('SFTP singular connection established successfully')

        # Use the appropriate connection for file replication
        replicate_SFTP_files_to_local(sftp, sftp_folder_name, local_folder_name)

    except pysftp.ConnectionException as ce:
        logging.error(f'Failed to establish SFTP connection: {ce}')
    except pysftp.AuthenticationException as ae:
        logging.error(f'Authentication error during SFTP connection: {ae}')
    except Exception as e:
        logging.error(f'An error occurred during SFTP operation: {e}')

    finally:
        if sftp:
            if use_pool:
                sftp_pool.return_connection(sftp)  # Return the connection to the pool
                logging.info('SFTP connection returned to pool')
            else:
                sftp.close()
                logging.info('SFTP singular connection closed')

    
       


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