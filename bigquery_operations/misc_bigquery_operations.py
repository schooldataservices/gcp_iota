from add_parent_to_sys_path import add_parent_to_sys_path
add_parent_to_sys_path()
import pysftp
import os
import pandas_gbq
import pandas as pd
from modules.buckets import *
from modules.reproducibility import *
from modules.stacking_bluff_asd import *
from modules.sftp_ops import *
import logging

clear_logging_handlers()

#Configure loggging
logging.basicConfig(filename='../logs/MiscBigQuery.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', force=True)
logging.info('\n\n-------------New Misc Big Query Logging Instance')

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable in order to interact, import the SFTP password from the same file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '../powerschool-420113-db919282054b.json'
    
# ----------------------------------------------------------
#Need to make this portion to where it assesses all files in the dir recursively. 
def main(SFTP_folder_name):

    SFTP_folder_name  = initial_schema_check(SFTP_folder_name)
    print(SFTP_folder_name)

    instance = Create(
                project_id='powerschool-420113',
                location = 'us-central1',
                bucket=f'{SFTP_folder_name}bucket-iotaschools-1',
                local_dir = fr'S:\SFTP\{SFTP_folder_name}',
                db = SFTP_folder_name,
                append_or_replace='replace',
                )
    
    instance.process()# Pass SFTP files into Bucket & then into Big Query tables

main("misc_imports")
main("EIS")
logging.info('Process has reached the end\n\n')
