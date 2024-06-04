import json 
import pysftp
import os
import pandas_gbq
import pandas as pd
from modules.buckets import *
from modules.reproducibility import *
import logging

#Configure loggging
logging.basicConfig(filename='BigQuery.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('\n\n-------------New Big Query Logging Instance')

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable in order to interact, import the SFTP password from the same file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'powerschool-420113-db919282054b.json'
with open('powerschool-420113-db919282054b.json') as json_file:
    j = json.load(json_file) 
    sftp_pass = j['sftp_password']
    
# ----------------------------------------------------------
#Need to make this portion to where it assesses all files in the dir recursively. 
def main(SFTP_folder_name):

    SFTP_folder_name  = initial_schema_check(SFTP_folder_name)
    print(SFTP_folder_name)

    instance = Create(
                project_id='powerschool-420113',
                location = 'us-south1',
                bucket=f'{SFTP_folder_name}bucket-iotaschools-1',
                local_dir = fr'S:\SFTP\\{SFTP_folder_name}',
                db = SFTP_folder_name,
                append_or_replace='replace',
                )
    
    instance.process()# Pass SFTP files into Bucket & then into Big Query tables



main("powerschool")
main("EIS")