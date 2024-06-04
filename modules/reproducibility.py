import pandas as pd
import logging


def read_file(file_path):

    # Get the file extension
    file_extension = file_path.split('.')[-1].lower()

    # Read the file based on its extension
    if file_extension == 'csv':
        df = pd.read_csv(file_path)
    elif file_extension == 'xlsx' or file_extension == 'xls':
        df = pd.read_excel(file_path)
    elif file_extension == 'json':
        df = pd.read_json(file_path)
    elif file_extension == 'txt':
        # You can specify additional parameters for reading text files
        df = pd.read_csv(file_path, sep='\t')  # Example: tab-separated text file
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    return df

def pre_processing(df):
        
        df = df.fillna('')
        df = df.astype(str)
        df.columns = [col.replace('.', '_') for col in df.columns]

        return(df)

# ----------------------------------------------------
#Bucket names must contain only lowercase letters, numbers, hyphens (-), and cannot start or end with a hyphen

#Alter the SFTP folder name variable within the main func
#Rename the local dir to be the same convention
#Assume the SFTP_folder_name variable

def initial_schema_check(SFTP_folder_name):

    SFTP_folder_name = SFTP_folder_name.lower()
    SFTP_folder_name = SFTP_folder_name.replace('_', '-')

    return(SFTP_folder_name)


# ---------------------------------------------------------

def remove_extension_from_file(file_name):

    parts = file_name.split('.')  # Split the filename by dot
    if len(parts) > 1:  # Check if there is an extension
        return '.'.join(parts[:-1])  # Join all parts except the last one
    else:
        return file_name  # If there's no extension, return the original filename



