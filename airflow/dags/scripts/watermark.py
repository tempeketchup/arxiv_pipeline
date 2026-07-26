import json
import logging
from datetime import datetime, timezone

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

# watermark = penanda waktu terakhir yang sudah diproses

logger = logging.getLogger(__name__)

WATERMARK_PATH = '_metadata/watermark.json'
DEFAULT_START = '2024-01-01T00:00:00Z'

def _get_file_client(storage_name, file_system):
    service_client = DataLakeServiceClient(
        account_url=f'https://{storage_name}.dfs.core.windows.net',
        credential=DefaultAzureCredential()
    )
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    return file_system_client.get_file_client(WATERMARK_PATH)

def get_last_watermark(storage_name, file_system, default=DEFAULT_START):
    file_client = _get_file_client(storage_name, file_system)
    try:
        download = file_client.download_file()
        content = json.loads(download.readall())
        watermark = content['last_submitted_date']
        logger.info(f'Loaded watermark: {watermark}')
        return watermark
    except Exception as e:
        logger.info(f'No existing watermark found ({e}); defaulting to {default}')
        return default
    
def update_watermark(storage_name, file_system, new_watermark):
    file_client = _get_file_client(storage_name, file_system)
    payload = json.dumps(
        {
            'last_submitted_date': new_watermark,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
    ).encode('utf-8')
    file_client.upload_data(payload, overwrite=True)
    logger.info(f"Watermark advanced to {new_watermark}")