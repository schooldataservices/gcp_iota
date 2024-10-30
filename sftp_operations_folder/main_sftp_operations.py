from add_parent_to_sys_path import add_parent_to_sys_path
add_parent_to_sys_path()
from modules.sftp_ops import *
import json 
import logging
clear_logging_handlers()

#Configure loggging
logging.basicConfig(filename='../logs/SFTP_operations.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('\n\n-------------New SFTP Operations Logging Instance')


with open('../powerschool-420113-db919282054b.json') as json_file:
    j = json.load(json_file) 
    savva_password = j['savva_password']
    savva_username = j['savva_username']
    savva_host = j['savva_host']
    clever_export_password = j['clever_export_password']
    clever_export_username = j['clever_export_username']
    clever_export_host = j['clever_export_host']
    ellevation_sftp_host = j['ellevation_sftp_host']
    ellevation_sftp_username = j['ellevation_sftp_username']
    ellevation_sftp_password =  j['ellevation_sftp_password']



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

ellevation_dictionary = {
                         'Ellevation_Staff-Roster' : 'Staff-Roster.csv', 
                         'Ellevation_Student-Course-Schedules':'Student-Course-Schedules.csv',
                         'Ellevation_Student-Demographics':'Student-Demographics.csv'
                        }


# sftp_conn_clever_export = SFTPConnection(
#     host=clever_export_host,
#     username=clever_export_username,
#     password=clever_export_password,
#     use_pool=False
# )

# #Export BQ views to local dir for Clever. Follow naming convention of dictionary arg. 
# SFTP_conn_file_exchange(sftp_conn_clever_export,
#                         import_or_export = 'export',
#                         sftp_folder_name='file_transfers\clever_iota_file_transfer', 
#                         db='roster_files',
#                         naming_dict = clever_dictionary,
#                         use_pool=False,
#                         )

# # Export the local replicated files to Clevers SFTP
# SFTP_export_dir_to_SFTP(local_dir=os.getcwd() + '\\file_transfers\clever_iota_file_transfer',
#                remote_dir='/home/boundless-calendar-0789',  
#                sftp = sftp_conn_clever_export)

# ------------------------------Ellevation Piece ----------------------------------------------------

sftp_conn_ellevation_export = SFTPConnection(
    host=ellevation_sftp_host,
    username=ellevation_sftp_username,
    password=ellevation_sftp_password,
    use_pool=False
)


#Export BQ views to local dir for Ellevation. Follow naming convention of dictionary arg. 
SFTP_conn_file_exchange(sftp_conn_ellevation_export,
                        import_or_export = 'export',
                        sftp_folder_name='file_transfers\ellevation_iota_file_transfer', 
                        db='roster_files',
                        naming_dict = ellevation_dictionary,
                        use_pool=False,
                        )

# Export the local replicated files to Clevers SFTP
SFTP_export_dir_to_SFTP(local_dir=os.getcwd() + '\\file_transfers\ellevation_iota_file_transfer',
               remote_dir='/data', 
               sftp = sftp_conn_ellevation_export)

# ----------------------------SAVVA piece-----------------------------------

# sftp_conn_savva = SFTPConnection(
#     host=savva_host,
#     username=savva_username,
#     password=savva_password,
#     use_pool=False
# )


# #Export replicates BQ views to local dir. Follow naming convention as dictionary arg. 
# SFTP_conn_file_exchange(sftp_conn_savva,
#                         import_or_export = 'export',
#                         sftp_folder_name='file_transfers\savva_iota_file_transfer', 
#                         db='roster_files',
#                         naming_dict = savva_dictionary,
#                         use_pool=False
#                         )

# #Sends local files over the SAVVAS sftp SIS folder
# SFTP_export_dir_to_SFTP(local_dir=os.getcwd() + '\\file_transfers\savva_iota_file_transfer',
#                remote_dir='/SIS',  #root dir on clevers sftp
#                sftp = sftp_conn_savva)



sftp_conn_clever_export.close_all_connections()
sftp_conn_savva.close_all_connections()
sftp_conn_ellevation_export.close_all_connections()
logging.info('Process has reached the end')




#Current run diagnosis. 
#Remove piplock until ran in virtual env
#Implement a catch if the file does not exist in BQ, example is clever_enrollments
#Specify which sftp conn is established in logs
