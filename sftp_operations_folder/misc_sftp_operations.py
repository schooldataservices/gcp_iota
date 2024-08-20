from add_parent_to_sys_path import add_parent_to_sys_path
add_parent_to_sys_path()

from modules.sftp_ops import *
import json 
import logging
clear_logging_handlers()


#Configure loggging
logging.basicConfig(filename='../logs/SFTP_operations_misc.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('\n\n-------------New SFTP Operations MISC Logging Instance')

with open('../powerschool-420113-db919282054b.json') as json_file:
    j = json.load(json_file) 
    clever_import_password = j['clever_import_password']
    clever_import_username = j['clever_import_username']
    clever_import_host = j['clever_import_host']
    easyIEP_username = j['easyIEP_username']
    easyIEP_password = j['easyIEP_password']
    easyIEP_host = j['easyIEP_host']


#All BQ tables that are begin queryed are the keys. 
#How they are saved in the local dir are the values
easyIEP_dictionary = {'TN-Options Report 2014.txt': 'TN_options_report_2014.txt'}


#Instantiate Clever sftp_conn
#This needs to be done within SFTP_conn_file_exchange given the fact that Clevers uses two SFTPs
sftp_conn_clever_import = SFTPConnection(
    host=clever_import_host,
    username=clever_import_username,
    password=clever_import_password,
    use_pool=False 
)

# Import brings in Google passwords info from Clevers SFTP directly to powerschool_combined
SFTP_conn_file_exchange(sftp_conn_clever_import,
                        import_or_export = 'import',
                        sftp_folder_name='idm-sensitive-exports', 
                        local_folder_name=r'S:\SFTP\misc_imports',
                        use_pool=False)

# ----------------------------EasyIEP Singular File piece-----------------------------------

sftp_conn_iep = SFTPConnection(
    host=easyIEP_host,
    username=easyIEP_username,
    password=easyIEP_password,
    use_pool=False
)

#Import easyIEP files directly to PS combined
#This downloaded all files in their for some reason

SFTP_conn_file_exchange(sftp_conn_iep,
                        import_or_export = 'import',
                        sftp_folder_name='Reports', 
                        naming_dict=easyIEP_dictionary,
                        local_folder_name=r'S:\SFTP\misc_imports',
                        file_to_download='TN-Options Report 2014.txt',
                        use_pool=False)


sftp_conn_clever_import.close_all_connections()
sftp_conn_iep.close_all_connections()
logging.info('Process has reached the end')
