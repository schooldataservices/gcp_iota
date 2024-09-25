import logging
from modules.sftp_ops import SFTPConnection
import json

# Define task functions
def configure_logging():
    logging.basicConfig(
        filename='logs/SFTP_operations.log', 
        level=logging.INFO,
        format='%(asctime)s - %(message)s', 
        datefmt='%d-%b-%y %H:%M:%S'
    )
    logging.info('\n\n-------------New SFTP Operations Logging Instance')

def load_credentials():
    with open('powerschool-420113-db919282054b.json') as json_file:
        return json.load(json_file)

def setup_sftp_connection(creds, type_):
    connections = {
        'clever_import': SFTPConnection(creds['clever_import_host'], creds['clever_import_username'], creds['clever_import_password'], use_pool=False),
        'clever_export': SFTPConnection(creds['clever_export_host'], creds['clever_export_username'], creds['clever_export_password'], use_pool=False),
        'savvas_import': SFTPConnection(creds['savvas_host'], creds['savvas_username'], creds['savvas_password'], use_pool=False),
        'easyIEP_import': SFTPConnection(creds['easyIEP_host'], creds['easyIEP_username'], creds['easyIEP_password'], use_pool=False),
    }
    if type_ not in connections:
        raise ValueError(f"Unknown connection type: {type_}")
    return connections[type_]

def close_all_sftp_connections(connections):
    for conn in connections.values():
        conn.close_all_connections()
    logging.info('All SFTP connections closed')

