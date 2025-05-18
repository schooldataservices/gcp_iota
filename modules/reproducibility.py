import pandas as pd
import logging


def read_file(file_path):

    # Get the file extension
    file_extension = file_path.split('.')[-1].lower()
    

    # Read the file based on its extension
    if file_extension == 'csv':
        try:
            df = pd.read_csv(file_path, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, sep=None, encoding='utf-16') #issue with some excel files coming across with utf-16 and tab delimiter

    elif file_extension == 'xlsx' or file_extension == 'xls':
        df = pd.read_excel(file_path, low_memory=False)
    elif file_extension == 'json':
        df = pd.read_json(file_path, low_memory=False)
    elif file_extension == 'txt':
        # You can specify additional parameters for reading text files
        df = pd.read_csv(file_path, sep='\t', low_memory=False)  # Example: tab-separated text file
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    return df

def pre_processing(df):
        
        df.columns = [col.replace('.', '_') for col in df.columns]

        # Drop rows where all columns have NaN values
        df = df.dropna(how='all')

        # Reset the index
        df = df.reset_index(drop=True)

        return(df)

# ----------------------------------------------------

# Bucket names can only contain lowercase letters, numeric characters, 
# dashes ( - ), underscores ( _ ), and dots ( . ). Spaces are not allowed.

# Table names can contain letters (uppercase and lowercase), numbers, and underscores (_). 
# Table names must start with a letter or underscore.

def initial_schema_check(SFTP_folder_name):

    SFTP_folder_name = SFTP_folder_name.lower()
  
    return(SFTP_folder_name)


# ---------------------------------------------------------

def remove_extension_from_file(file_name):

    parts = file_name.split('.')  # Split the filename by dot
    if len(parts) > 1:  # Check if there is an extension
        return '.'.join(parts[:-1])  # Join all parts except the last one
    else:
        return file_name  # If there's no extension, return the original filename


