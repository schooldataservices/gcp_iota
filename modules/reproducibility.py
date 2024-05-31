import pandas as pd
import pysftp
import logging
import os
import socket
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


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


# --------------------------------------------------


def SFTP_conn(sftp_pass, folder_name):
    sftp = None  # Initialize sftp outside the try block
    
    try:
        sftp = pysftp.Connection(
            host="sftp.iotaschools.org",
            username="iota.sftp",
            password=sftp_pass,
            cnopts=cnopts,
        )
        
        logging.info('SFTP connection established successfully')

        # Set local directory to save SFTP files to computer
        local_directory = f'SFTP_folders/{folder_name}'
        os.makedirs(local_directory, exist_ok=True)  # Create local directory if it doesn't exist
        
        # Change to the remote directory
        sftp.chdir(folder_name)
        
        print('Dir contents: ', sftp.listdir())
        logging.info(f'Dir contents of {folder_name} are the following: {sftp.listdir()} ')
        
        # Download the entire remote folder recursively and place on local directory
        sftp.get_r('.', local_directory)

        logging.info(f'Folder "{folder_name}" replicated to local directory "{local_directory}"')
        
    except pysftp.ConnectionException as ce:
        logging.error(f'Failed to establish SFTP connection: {ce}')
    except pysftp.AuthenticationException as ae:
        logging.error(f'Authentication error during SFTP connection: {ae}')
    except socket.timeout as te:
        logging.error(f'Timeout occurred during SFTP connection: {te}')
    except Exception as e:
        logging.error(f'An error occurred during SFTP operation: {e}')

    finally:
        if sftp:
            sftp.close()  # Close the connection if it was successfully opened
            logging.info('SFTP conn closed')



# ----------------------------------------------------------

def pre_processing(df):
        
        df = df.fillna('')
        df = df.astype(str)
        df.columns = [col.replace('.', '_') for col in df.columns]

        return(df)

# ---------------------------------------------------------

def remove_extension_from_file(file_name):

    parts = file_name.split('.')  # Split the filename by dot
    if len(parts) > 1:  # Check if there is an extension
        return '.'.join(parts[:-1])  # Join all parts except the last one
    else:
        return file_name  # If there's no extension, return the original filename


