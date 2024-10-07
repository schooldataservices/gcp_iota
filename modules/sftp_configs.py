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
        'local_sftp_type': 'iota_sftp',
        'import_or_export': 'import',
        'target_sftp_folder_name': '/idm-sensitive-exports',
        'local_sftp_folder_name': '/powerschool_combined', 
        'export_local_bq_replications': False,
        'project_id':'powerschool-420113'
    },

    # --------------------------------

    {
        'sftp_type': 'clever_export',
        'local_sftp_type': 'iota_sftp',
        'import_or_export': 'export',
        'target_sftp_folder_name': '/home/boundless-calendar-0789', 
        'local_sftp_folder_name': '/bq_replications/clever_iota_file_transfer',
        'naming_dict': clever_dictionary,
        'db':'roster_files',
        'export_local_bq_replications': True,
        'project_id':'powerschool-420113'
    },
    # ----------------------------------

    {
        'sftp_type': 'easyIEP_import',
        'local_sftp_type': 'iota_sftp',
        'import_or_export': 'import',
        'target_sftp_folder_name': '/Reports',
        'local_sftp_folder_name': '/easyIEP_combined', 
        'file_to_download': 'TN-Options Report 2014.txt',  
        'naming_dict': easyIEP_dictionary,  
        'export_local_bq_replications': False,
        'project_id':'powerschool-420113'
    },

    # -------------------------------------

    {
        'sftp_type': 'savva_export',
        'local_sftp_type': 'iota_sftp',
        'import_or_export': 'export',
        'target_sftp_folder_name': '/SIS',
        'local_sftp_folder_name': '/bq_replications/savva_iota_file_transfer',
        'naming_dict': savva_dictionary,
        'db': 'roster_files',
        'export_local_bq_replications': True, 
        'project_id':'powerschool-420113'
    }
     # --------------------------------
]


