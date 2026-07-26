import logging
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

def upload_to_adls(newest_file, storage_name, file_system, directory_client):
    service_client = DataLakeServiceClient (
        account_url=f"https://{storage_name}.dfs.core.windows.net",
        credential=DefaultAzureCredential()
    )
    # read container
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    
    # read directory didalam container
    directory_client = file_system_client.get_directory_client(directory_client)
    try:
        directory_client.create_directory()
    except Exception as e:
        logger.info(f"Directory creation skipped or failed: {e}")

    file_name = os.path.basename(newest_file)

    # tidak mengupload apapun cuma create handle yang 'point' ke lokasi tersebut
    file_client = directory_client.get_file_client(file_name)

    with open(newest_file, 'rb') as data:
        file_client.upload_data(data, overwrite=True)

    logger.info(f'Uploaded {file_name} to ADLS')

