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


sftp_configs = [
    {
        'sftp_type': 'clever_import',
        'import_or_export': 'import',
        'target_sftp_folder_name': 'idm-sensitive-exports',
        'local_folder_name': r'S:\SFTP\powerschool_combined', #Write directly to local SFTP on server without auth. This will need to change once deployed
        #Also could make SFTP send legit with auth, rather than local

        'export_local_bq_replications': False,
        'export_sftp_folder_name': None  
    },

    # --------------------------------

    {
        'sftp_type': 'clever_export',
        'import_or_export': 'export',
        'target_sftp_folder_name': 'clever_iota_file_transfer',
        'naming_dict': clever_dictionary,
        'local_folder_name': os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory', 'TN_operations', 'bq_replications', 'clever_iota_file_transfer'),
        'db':'roster_files',

        'export_local_bq_replications': True,
        'export_sftp_folder_name': '/home/boundless-calendar-0789'  
    },
    # ----------------------------------

    {
        'sftp_type': 'easyIEP_import',
        'import_or_export': 'import',
        'target_sftp_folder_name': 'Reports',
        'naming_dict': easyIEP_dictionary,
        'local_folder_name': r'S:\SFTP\easyIEP_combined',  #Write directly to local SFTP on server without auth. This will need to change once deployed
        'file_to_download': 'TN-Options Report 2014.txt',   

        'export_local_bq_replications': False,
        'export_SFTP_folder_name': None  
    },

    # -------------------------------------

    {
        'sftp_type': 'savva_export',
        'import_or_export': 'export',
        'target_sftp_folder_name': 'saava_iota_file_transfer',
        'naming_dict': savva_dictionary,
        'db': 'roster_files',
        'local_folder_name': os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory', 'TN_operations', 'bq_replications', 'saava_iota_file_transfer'),

        'export_local_bq_replications': True, 
        'export_sftp_folder_name': '/SIS'
    }
     # --------------------------------
]




