import os

# Define the SFTP types, folder names, file exchange modes (import/export), and optional remote directories
sftp_configs = [
    {
        'sftp_type': 'clever_import',
        'import_or_export': 'import',
        'import_sftp_folder_name': 'idm-sensitive-exports',
        'local_folder_name': r'S:\SFTP\powerschool_combined',

        'export_local_bq_replications': False,
        'export_sftp_folder_name': None  
    },
    {
        'sftp_type': 'clever_export',
        'import_or_export': 'export',
        'import_sftp_folder_name': 'clever_iota_file_transfer',
        'local_folder_name': os.getcwd() + '\\clever_iota_file_transfer',        #This might need to change

        'export_local_bq_replications': True,
        'export_sftp_folder_name': '/home/boundless-calendar-0789'  
    },
    {
        'sftp_type': 'easyIEP_import',
        'import_or_export': 'import',
        'local_sftp_folder_name': 'easyIEP-sensitive-exports',
        'local_folder_name': r'S:\SFTP\easyIEP_combined',

        'export_local_bq_replications': False,
        'export_SFTP_folder_name': None  # No remote dir for imports
    },
    {
        'sftp_type': 'savva_export',
        'import_or_export': 'export',
        'local_sftp_folder_name': 'saava_iota_file_transfer',
        'local_folder_name': os.getcwd() + '\\savva_iota_file_transfer',

        'export_local_bq_replications': False, #This is sent to SFTP already
        'export_sftp_folder_name': '/SIS'
    }
]


# python_callable=dynamic_sftp_file_exchange(
#     sftp_type=config['sftp_type'],
#     import_or_export=config['import_or_export'],
#     sftp_folder_name=config['sftp_folder_name'],
#     local_folder_name=config['local_folder_name'],
#     remote_dir=config.get('remote_dir')  # Pass remote_dir if it exists
# ),


#Issue with sftp_folder_name in SFTP_conn_file_exchange 
# & remote dir in SFTP_export_dir_to_SFTP

#sftp_folder_name goes to the local IOTA sftp and ensures that the local directory exists


#What about naming dict seen in SFTP_conn_file_exchagne for 

# saava export
# easyIEP import
# clever export


# Is the variable local_folder_name truly always local_folder_name?