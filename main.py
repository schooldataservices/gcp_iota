#SFTP 

# # Use
# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None


# #Get Data from PS Data Export Manager SFTP folder, and move to BigQuery
# with pysftp.Connection(
#     host="ps.com",
#     username="greendot",
#     password="*********",
#     cnopts=cnopts
# ) as sftp:
#     # Download a remote file to the local machine
#     remote_file = "/greendottn/custom_report_standards_2024"
#     local_file = "local_file.csv"   (This can be dynamic if we want to preserve the file everytime)
#     sftp.get(remote_file, local_file)

# ----------------------------------------------------------

from modules.buckets import create_bucket, upload_to_bucket, download_from_bucket, upload_to_bq_table
import os
import pandas_gbq
import pandas as pd


# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable in order to interact
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'greendotdataflow-848329b50f47.json'



class Create:

    def __init__(self, bucket, end_file_name, local_file_name, project_id, db, table_name, SQL_query, location=None):
        
        self.location = location
        self.bucket = bucket
        self.end_file_name = end_file_name
        self.local_file_name = local_file_name
        self.project_id = project_id
        self.db = db
        self.table_name = table_name
        self.SQL_query = SQL_query


    def process(self):
    
        #Create the bucket, and upload to that bucket. If already created, bypass
        create_bucket(self.bucket, self.location)

        #Upload file to bucket, demonstrates if overwritten or newfile. 
        #End File Name,  Local File Path, Bucket Name
        upload_to_bucket(self.end_file_name , self.local_file_name, self.bucket)


        upload_to_bq_table(cloud_storage_uri = f'gs://{self.bucket}/{self.end_file_name}',
                        project_id = self.project_id,
                        db = self.db,
                        table_name = self.table_name,
                        location = self.location)

        try:
            print('Running query')
            query = pandas_gbq.read_gbq(self.SQL_query, project_id=self.project_id, location=self.location)
            print('Query completed')
        except:
            print('Unable to run query')


        return(query)





already_created_update = Create(location = 'us-south1',
                                bucket='psholdingbucket7',
                                end_file_name = 'students.csv',
                                local_file_name = 'Student_Records.csv',
                                project_id='greendotdataflow',
                                db = 'Students',
                                table_name = 'Student_Records_7',

                                SQL_query='''

                                SELECT * FROM greendotdataflow.Students.Student_Records_7

                                '''  
                    )

brand_new_creation = Create(location = 'us-south1',
                            bucket='psholdingbucket12',
                            end_file_name = 'students.csv',
                            local_file_name = 'Student_Records.csv',
                            project_id='greendotdataflow',
                            db = 'Students',
                            table_name = 'Student_Records_12',

                            SQL_query='''

                            SELECT * FROM greendotdataflow.Students.Student_Records_12

                            '''  
                    )


query_one = already_created_update.process()

query_two = brand_new_creation.process()


#-------------Next step put file in clevers SFTP--------------