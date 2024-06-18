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
    def __init__(self, host, username, password, cnopts, max_connections=5):
        self.host = host
        self.username = username
        self.password = password
        self.cnopts = cnopts
        self.max_connections = max_connections
        self.pool = Queue(max_connections)
        self.lock = Lock()
        self._initialize_pool()

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
        with self.lock:
            if self.pool.empty():
                return self._create_new_connection()
            return self.pool.get()

    def return_connection(self, conn):
        with self.lock:
            if self.pool.full():
                conn.close()
            else:
                self.pool.put(conn)

    def close_all_connections(self):
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()
            logging.info('SFTP connection closed')


    def create_singular_connection(self):
        return self._create_new_connection()



def replicate_SFTP_files_to_local(sftp, sftp_folder_name):

    if sftp_folder_name == 'idm-sensitive-exports':
        local_dir = os.path.join('C:\\', 'powerschool')
    else:
        local_dir = os.path.join('C:\\', sftp_folder_name)

    #create sftp local dir if it does not exists already. 
    # local_dir = os.path.join('S:\\SFTP', sftp_folder_name)
  
   
    os.makedirs(local_dir, exist_ok=True)


    try:
        sftp.chdir(sftp_folder_name)
        dir_contents = sftp.listdir()
        logging.info(f'Dir contents of {sftp_folder_name}: {dir_contents}')

        for file_name in dir_contents:
            remote_file_path = os.path.join(sftp_folder_name, file_name)
            local_file_path = os.path.join(local_dir, file_name)

            logging.info(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')
            print(f'Trying to download remote file: {remote_file_path} to local path: {local_file_path}')

            sftp.get(file_name, local_file_path)
            logging.info(f'File "{file_name}" downloaded to local directory "{local_dir}"')

        logging.info(f'All files in folder "{sftp_folder_name}" downloaded to local directory "{local_dir}"')

    except Exception as e:
        logging.error(f'An error occurred during file replication: {e}')




def SFTP_conn(sftp_folder_name, sftp_pool, use_pool):
    sftp = None
 
    try:
        sftp = sftp_pool.get_connection()
        logging.info('SFTP connection established successfully from pool')

        # Use the appropriate connection for file replication
        replicate_SFTP_files_to_local(sftp, sftp_folder_name)

    except pysftp.ConnectionException as ce:
        logging.error(f'Failed to establish SFTP connection: {ce}')
    except pysftp.AuthenticationException as ae:
        logging.error(f'Authentication error during SFTP connection: {ae}')
    except Exception as e:
        logging.error(f'An error occurred during SFTP operation: {e}')

    finally:
        if sftp:
            sftp_pool.return_connection(sftp)  # Return the connection to the pool
            logging.info('SFTP connection returned to pool')
       


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