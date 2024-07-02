import os
import pandas as pd
import logging


def file_comparison():
    directory_path_blf = r'S:\SFTP\powerschool_tpcsc'
    directory_path_asd = r'S:\SFTP\powerschool'

    file_list_blf = [file for file in os.listdir(directory_path_blf) if os.path.isfile(os.path.join(directory_path_blf, file))]
    file_list_asd = [file for file in os.listdir(directory_path_asd) if os.path.isfile(os.path.join(directory_path_asd, file))]

    blf = pd.DataFrame(file_list_blf, columns = ['BLF_files'])
    asd = pd.DataFrame(file_list_asd, columns = ['ASD_files'])

    file_comparison = pd.merge(blf, asd, left_on='BLF_files', right_on='ASD_files', how='outer')

    return(file_comparison)



def read_file(directory, filename):
   
    file_path = os.path.join(directory, filename)
    return pd.read_csv(file_path, low_memory=False)  # Adjust the file reading method as needed (e.g., pd.read_excel for Excel files)

def concat_files_from_directories(ASD, BLF, output_directory):

    files_dir1 = set(os.listdir(ASD))
    files_dir2 = set(os.listdir(BLF))
    
    # Find common files in both directories
    common_files = files_dir1.intersection(files_dir2)

    logging.info(f'Concatenating {len(common_files)} files from {files_dir1} & {files_dir2}')

    
    for filename in common_files:
        # Read files from both directories
        df1 = read_file(ASD, filename)
        df2 = read_file(BLF, filename)

        df1['district_identification'] = 'ASD'
        df2['district_identification'] = 'BLF'
        
        # Concatenate the dataframes
        combined_df = pd.concat([df1, df2], ignore_index=True)
        
        # Save the concatenated dataframe to the output directory
        output_path = os.path.join(output_directory, filename)
        combined_df.to_csv(output_path, index=False)  # Adjust the file writing method as needed (e.g., to_excel for Excel files)

    logging.info(f'All files sent to {output_directory}')