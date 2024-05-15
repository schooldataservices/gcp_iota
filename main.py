# --------------------------SFTP conn to retrieve files---------------------
import json 
import pysftp
import os
import pandas_gbq
import pandas as pd
from modules.buckets import *
import logging

#Configure loggging
logging.basicConfig(filename='BigQuery.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('\n\n-------------New Big Query Logging Instance')
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable in order to interact, import the SFTP password from the same file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'powerschool-420113-db919282054b.json'
with open('powerschool-420113-db919282054b.json') as json_file:
    j = json.load(json_file) 
    sftp_pass = j['sftp_password']


#Get Data from PS Data Export Manager, to SFTP folder, and move to Bucket & then BigQuery
try:

    with pysftp.Connection(
        host="sftp.iotaschools.org",
        username="iota.sftp",
        password=sftp_pass,
        cnopts=cnopts
    ) as sftp:
        
        logging.info('SFTP connection established succesfully')
        
        directory_contents = sftp.listdir()
        current_directory = sftp.pwd
        print("Current directory:", current_directory)
        print('Dir contents: ', directory_contents)

        # Download a remote file to the local machine
        # remote_directory = "/greendottn/custom_report_standards_2024"
        # for item in directory_contents:
            # print(item)

        # local_file = "local_file.txt"   #(This can be dynamic if we want to preserve the file everytime)
        # sftp.get('Test!.txt', local_file) #take file in SFTP and name it to local_file name
        # sftp.close()
except:
    logging.info('Failed to establish SFTP connection')

# ----------------------------------------------------------

ps_students_file_1 = Create(location = 'us-south1',
                            bucket='powerschoolbucket-iotaschools-1',
                            local_file_name = 'Student_Records.csv',
                            end_file_name = 'students.csv',
                            project_id='powerschool-420113',
                            db = 'students',
                            table_name = 'ps_students',
                            append_or_replace='replace',

                            sql_query=f'''

                            SELECT * FROM powerschool-420113.students.ps_students

                                    '''  
                             )


ps_students_file_2 = Create(location = 'us-south1',
                            bucket='powerschoolbucket-iotaschools-1',
                            local_file_name = 'Student_Records.csv',
                            end_file_name = 'students_2.csv',
                            project_id='powerschool-420113',
                            db = 'students',
                            table_name = 'ps_students_2',
                            append_or_replace = 'append',

                            sql_query='''

                             SELECT * FROM powerschool-420113.students.ps_students_2

                             '''  
                    )

query_one = ps_students_file_1.process()
query_two = ps_students_file_2.process()

#-------------Next step put file in clevers SFTP--------------