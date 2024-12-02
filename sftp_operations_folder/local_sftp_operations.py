from add_parent_to_sys_path import add_parent_to_sys_path
add_parent_to_sys_path()

from modules.sftp_ops import *
import json 
import logging
clear_logging_handlers()

#Configure loggging
logging.basicConfig(filename='../logs/SFTP_operations_local.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('\n\n-------------New SFTP Operations local Logging Instance')


with open('../powerschool-420113-db919282054b.json') as json_file:
    j = json.load(json_file) 
    iota_sftp_host = j['iota_sftp_host']
    iota_sftp_username = j['iota_sftp_username']
    iota_sftp_password = j['iota_sftp_password']


#All BQ tables that are begin queryed are the keys. 
#How they are saved in the local dir are the values
iota_dictionary = {'GoogleForPSImport_ASD':'GoogleForPSImport_ASD.txt',
                    'GoogleForPSImport_TPCSC':'GoogleForPSImport_TPCSC.txt',
                    'IEP_Import_ASD': 'IEP_Import_ASD.txt',
                    'RTI_ASD': 'RTI_ASD.txt',
                    'RTI_TPCSC': 'RTI_TPCSC.txt',
                    'Homeless_Foster_ASD': 'Homeless_Foster_ASD.txt',
                    'Homeless_Foster_BLF': 'Homeless_Foster_BLF.txt'
                    }


sftp_conn_iota_export = SFTPConnection(
    host=iota_sftp_host,
    username=iota_sftp_username,
    password=iota_sftp_password,
    use_pool=False
)

#Export BQ views to local dir. Follow naming convention of dictionary arg. 
SFTP_conn_file_exchange(sftp_conn_iota_export,
                        import_or_export = 'export',
                        sftp_folder_name='file_transfers/local_iota_file_transfer', 
                        db='powerschool_imports',
                        naming_dict = iota_dictionary,
                        use_pool=False
                        )

#Sends local files over to IOTA sftp PS_imports folder
SFTP_export_dir_to_SFTP(local_dir=os.getcwd() + '\\file_transfers\\local_iota_file_transfer',
                        remote_dir='/PS_imports',  #root dir on clevers sftp
                        sftp = sftp_conn_iota_export)