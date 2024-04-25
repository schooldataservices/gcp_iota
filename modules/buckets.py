import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import pandas as pd
import pandas_gbq




def create_bucket(bucket_name, location, storage_class = 'STANDARD'):

    storage_client = storage.Client()

    try:
        # Attempt to get the bucket
        bucket = storage_client.get_bucket(bucket_name)
        print(f'\n\nBucket {bucket_name} already exists.')
    
    except NotFound:
        # Bucket not found, create a new one with storage_class arg and location
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = storage_class

        bucket = storage_client.create_bucket(bucket, location)
        print(f'\n\nBucket {bucket_name} created in {location} with {storage} storage class.')
    


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
        else:
            print(f"Local File {local_file} was uploaded as a new file {destination_blob_name} in the bucket {bucket_name}.")

    except Exception as e:
        print(e)





def download_from_bucket(source_blob_name, destination_file_path, bucket_name):

    #Initialize a Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    if os.path.exists(destination_file_path) == True:
        print(f'Downloading {source_blob_name} file from bucket. Local file being overwritten as: {destination_file_path}')

    try:
        # Download the blob to a local file
        blob.download_to_filename(destination_file_path)
        print(f'File {source_blob_name} downloaded from bucket as {destination_file_path}')

    except Exception as e:
        print(e)



#Upload to BiqQuery using Pandas_GBQ. 
#Schema is created based on pandas dtypes. 
        
def upload_to_bq_table(cloud_storage_uri, project_id, db, table_name, location):

  
    # project, DB, table name
    table_id = f'{project_id}.{db}.{table_name}'

    # Read the CSV file from Cloud Storage into a Pandas DataFrame
    df = pd.read_csv(cloud_storage_uri)

    client = bigquery.Client()

    try:
        client.get_table(table_id)
        print(f'Table {table_id} already exists, appending new data')
   
    except NotFound:
        print(f'Table {table_id} has been created, and data has been sent')

    pandas_gbq.to_gbq(df, table_id, project_id='greendotdataflow', if_exists='replace', location=location)




   








