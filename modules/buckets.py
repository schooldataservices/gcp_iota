import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import logging




def create_bucket(bucket_name, location, storage_class = 'STANDARD'):

    storage_client = storage.Client()

    try:
        # Attempt to get the bucket
        bucket = storage_client.get_bucket(bucket_name)
        logging.info(f'Bucket {bucket_name} already exists')
        print(f'\n\nBucket {bucket_name} already exists.')
    
    except NotFound:
        # Bucket not found, create a new one with storage_class arg and location
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = storage_class

        bucket = storage_client.create_bucket(bucket, location)
        print(f'\n\nBucket {bucket_name} created in {location} with {storage} storage class.')
        logging.info(f'\n\nBucket {bucket_name} created in {location} with {storage} storage class.')
    


#blob name is the name of the file once uploaded
#file_path is local file path
#bucket name is GC bucket unique bucket name

def upload_to_bucket(destination_blob_name, local_file, bucket_name):
    # Initialize a Google Cloud Storage client
    storage_client = storage.Client()

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Get the existing blob/filename in the bucket (if it exists)
        existing_blob = bucket.get_blob(destination_blob_name)
        existing_updated_time = existing_blob.updated if existing_blob else None

        # Upload the file
        blob.upload_from_filename(os.getcwd() + f'\\{local_file}')

        # Check if the file was overwritten based on blob names
        if existing_blob and existing_blob.name == destination_blob_name:
            print(f"File {destination_blob_name} in bucket {bucket_name} was overwritten by local file {local_file}.")
            logging.info(f"File {destination_blob_name} in bucket {bucket_name} was overwritten by local file {local_file}.")
        else:
            print(f"Local File {local_file} was uploaded as a new file {destination_blob_name} in the bucket {bucket_name}.")
            logging.info(f"Local File {local_file} was uploaded as a new file {destination_blob_name} in the bucket {bucket_name}.")

    except Exception as e:
        print(e)
        logging.info(f'Error uploading {local_file} to the {bucket_name} due to {e}')





def download_from_bucket(source_blob_name, destination_file_path, bucket_name):

    #Initialize a Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    if os.path.exists(destination_file_path) == True:
        print(f'Downloading {source_blob_name} file from bucket. Local file being overwritten as: {destination_file_path}')
        logging.info(f'Downloading {source_blob_name} file from bucket. Local file being overwritten as: {destination_file_path}')

    try:
        # Download the blob to a local file
        blob.download_to_filename(destination_file_path)
        print(f'File {source_blob_name} downloaded from bucket as {destination_file_path}')
        logging.info(f'File {source_blob_name} downloaded from bucket as {destination_file_path}')

    except Exception as e:
        print(e)



#Upload to BiqQuery using Pandas_GBQ. 
#Schema is created based on pandas dtypes. 
        
def upload_to_bq_table(cloud_storage_uri, project_id, db, table_name, location, append_or_replace):

  
    # project, DB, table name
    table_id = f'{project_id}.{db}.{table_name}'

    # Read the CSV file from Cloud Storage into a Pandas DataFrame
    df = pd.read_csv(cloud_storage_uri)

    client = bigquery.Client()

    try:
        client.get_table(table_id)
        print(f'Table {table_id} already exists, argument is being called to {append_or_replace.upper()} new data with incoming data')
        logging.info(f'Table {table_id} already exists, argument is being called to {append_or_replace.upper()} new data with incoming data')
   
    except NotFound:
        print(f'Table {table_id} has been created, and data has been sent for the first time')
        logging.info(f'Table {table_id} has been created, and data has been sent for the first time')

    pandas_gbq.to_gbq(df, table_id, project_id, if_exists=append_or_replace, location=location)




   


class Create:

    def __init__(self, bucket, end_file_name, local_file_name, project_id, db, table_name, sql_query, append_or_replace, location=None):
        
        self.location = location
        self.bucket = bucket
        self.end_file_name = end_file_name
        self.local_file_name = local_file_name
        self.project_id = project_id
        self.db = db
        self.table_name = table_name
        self.sql_query = sql_query
        self.append_or_replace = append_or_replace


    def process(self):

        logging.info('New file processing started\n')
    
        #Create the bucket, and upload to that bucket. If already created, bypass
        create_bucket(self.bucket, self.location)

        #Upload file to bucket, demonstrates if overwritten or newfile. 
        #End File Name,  Local File Path, Bucket Name
        upload_to_bucket(self.end_file_name , self.local_file_name, self.bucket)


        upload_to_bq_table(cloud_storage_uri = f'gs://{self.bucket}/{self.end_file_name}',
                            project_id = self.project_id,
                            db = self.db,
                            table_name = self.table_name,
                            location = self.location,
                            append_or_replace = self.append_or_replace)
        
        #Could implement sql query here if did not want to be in class instance

        try:
            logging.info(f'Calling SQL query on {self.project_id}.{self.db}.{self.table_name}')
            print(f'Calling SQL query on {self.project_id}.{self.db}.{self.table_name}')

            query = pandas_gbq.read_gbq(self.sql_query, project_id=self.project_id, location=self.location)

            logging.info('SQL Query completed')
            print('SQL Query completed')

        except Exception as e:
            print(f'Unable to run query on {self.project_id}.{self.db}.{self.table_name} due to {e}')
            logging.info(f'Unable to run query on {self.project_id}.{self.db}.{self.table_name} due to {e}')

        return(query)






