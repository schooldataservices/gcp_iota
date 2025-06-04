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

#This must be done to fix the improper dtype mix. Not sure where this file originates from in order to get into the SFTP. 
df = pd.read_csv(r'S:\SFTP\test_scores\iReady_ByYear_PriorYrs.csv')
columns_to_replace = ['met_typical', 'met_stretch'] # Define the columns and the replacement dictionary. Apply the replacement to multiple columns
replacement_dict = {'True': 1, 'False': 0}
df[columns_to_replace] = df[columns_to_replace].replace(replacement_dict).astype('Int64')
df.to_csv(r'S:\SFTP\test_scores\iReady_ByYear_PriorYrs.csv', index=False)

main("misc_imports")
# main("EIS")
main("test_scores")
logging.info('Process has reached the end\n\n')
