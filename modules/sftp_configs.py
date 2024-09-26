import os

#All BQ tables that are begin queryed are the keys. 
#How they are saved in the local dir are the values
clever_dictionary = {'Clever_schools':'schools.csv',
                    'Clever_students':'students.csv',
                    'Clever_teachers':'teachers.csv',
                    'Clever_sections':'sections.csv',
                    'Clever_enrollments':'enrollments.csv', 
                    'Clever_staff': 'staff.csv'
                    }

savva_dictionary = {
                    'SAVVAS_CODE_DISTRICT': 'CODE_DISTRICT.txt',
                    'SAVVAS_SCHOOL': 'SCHOOL.txt',
                    'SAVVAS_STUDENT': 'STUDENT.txt',
                    'SAVVAS_STAFF': 'STAFF.txt',
                    'SAVVAS_PIF_SECTION': 'PIF_SECTION.txt',
                    'SAVVAS_PIF_SECTION_STAFF': 'PIF_SECTION_STAFF.txt',
                    'SAVVAS_PIF_SECTION_STUDENT': 'PIF_SECTION_STUDENT.txt',
                    'SAVVAS_ENROLLMENT': 'ENROLLMENT.txt',
                    'SAVVAS_ASSIGNMENT': 'ASSIGNMENT.txt'
                }

easyIEP_dictionary = {'TN-Options Report 2014.txt': 'TN_options_report_2014.txt'}

# Define the SFTP types, folder names, file exchange modes (import/export), and optional remote directories
sftp_configs = [
    {
        'sftp_type': 'clever_import',
        'import_or_export': 'import',
        'target_sftp_folder_name': 'idm-sensitive-exports',
        'local_folder_name': r'S:\SFTP\powerschool_combined',

        'export_local_bq_replications': False,
        'export_sftp_folder_name': None  
    },

    # --------------------------------
    {
        'sftp_type': 'clever_export',
        'import_or_export': 'export',
        'target_sftp_folder_name': 'clever_iota_file_transfer',
        'naming_dict': clever_dictionary,
        'local_folder_name': os.getcwd() + '\\clever_iota_file_transfer',        #This might need to change

        'export_local_bq_replications': True,
        'export_sftp_folder_name': '/home/boundless-calendar-0789'  
    },
    # ----------------------------------

    {
        'sftp_type': 'easyIEP_import',
        'import_or_export': 'import',
        'target_sftp_folder_name': 'Reports',
        'naming_dict': easyIEP_dictionary,
        'local_folder_name': r'S:\SFTP\easyIEP_combined',
        'file_to_download': 'TN-Options Report 2014.txt',

        'export_local_bq_replications': False,
        'export_SFTP_folder_name': None  # No remote dir for imports
    },

    # -------------------------------------

    {
        'sftp_type': 'savva_export',
        'import_or_export': 'export',
        'target_sftp_folder_name': 'saava_iota_file_transfer',
        'naming_dict': savva_dictionary,
        'local_folder_name': os.getcwd() + '\\savva_iota_file_transfer',

        'export_local_bq_replications': False, #This is sent to SFTP already
        'export_sftp_folder_name': '/SIS'
    }
     # --------------------------------
]




# --------------------------------------General notes


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

#Which one
# import_sftp_folder_name 
#'local_sftp_folder_name'


# SFTP_conn_file_exchange(sftp_conn_savva,
#                         import_or_export = 'export',
#                         sftp_folder_name='savva_iota_file_transfer', 
#                         naming_dict = savva_dictionary,
#                         use_pool=False
#                         )

# SFTP_conn_file_exchange(sftp_conn_iep,
#                         import_or_export = 'import',
#                         sftp_folder_name='Reports',                           #Due to being an import, 'Reports' is a directory of easyIEP that a file is
#                         naming_dict=easyIEP_dictionary,                                             #pulled out of. Appropriate name would be sftp_folder_name
#                         local_folder_name=r'S:\SFTP\powerschool_combined',
#                         file_to_download='TN-Options Report 2014.txt',
#                         use_pool=False)


#Is the master function
# dynamic_sftp_file_exchange(sftp_type, import_or_export, import_sftp_folder_name, local_folder_name, export_local_bq_replications=False, export_sftp_folder_name=None):

#Incorporate naming dict