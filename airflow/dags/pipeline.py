import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException

from scripts.extract_arxiv import fetch_arxiv_data, save_to_json
from scripts.data_quality import (
    run_quality_checks,
    enforce_quality_gate,
    save_rejected,
    DataQualityError,
)
from scripts.watermark import get_last_watermark, update_watermark
from scripts.upload_to_adls import upload_to_adls

logger = logging.getLogger(__name__)

DATA_PATH = 'data/bronze'
STORAGE_NAME = 'arxivetldevsa'
FILE_SYSTEM = 'bronze' # Container
DIRECTORY_CLIENT = 'arxiv'

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15)
}

@dag(
    dag_id='arxiv_to_azure_bronze_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        'query': 'artificial intelligence',
        'max_results': 100,
        'max_rejection_rate': 0.2
    },
    tags=['arxiv', 'bronze', 'ingestion']
)
def arxiv_ingestion_pipeline():
    
    @task
    def get_watermark_window(params: dict, logical_date=None):
        start_str = get_last_watermark(STORAGE_NAME, FILE_SYSTEM)
        start_date = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
        end_date = logical_date
        logger.info(f"Ingestion window: {start_date.isoformat()} -> {end_date.isoformat()}")
        return {
            'query': params['query'],
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        }
    
    @task(retries=4)
    def extract_batch(window: dict, params: dict):
        records = fetch_arxiv_data(
            query=window['query'],
            start_date=datetime.fromisoformat(window['start_date']),
            end_date=datetime.fromisoformat(window['end_date']),
            max_results=params['max_results']
        )
        if not records:
            logger.info('No new records in this window.')
        return records
    
    @task(retries=0)
    def apply_quality_checks(records: list, window: dict, params: dict, run_id: str):
        batch_id = run_id.replace(':', '-').replace('+', '-')
        clean, rejected, report = run_quality_checks(records)

        save_rejected(rejected, DATA_PATH, window['query'], batch_id)

        try:
            enforce_quality_gate(report, max_rejection_rate=params['max_rejection_rate'])
        except DataQualityError as e:
            raise AirflowException(str(e))

        return {'clean': clean, 'batch_id': batch_id}
    
    @task
    def save_batch(payload: dict, window: dict):
        if not payload['clean']:
            return None
        path = save_to_json(
            payload['clean'],
            query=window['query'],
            output_dir=DATA_PATH,
            batch_id=payload['batch_id']
        )
        logger.info(f"Saved {len(payload['clean'])} clean records to {path}")
        return path
    
    @task
    def send_to_azure(filepath: str):
        if filepath is None:
            logger.info("Nothing to upload this run")
            return None
        upload_to_adls(filepath, storage_name=STORAGE_NAME, file_system=FILE_SYSTEM, directory_client=DIRECTORY_CLIENT)
        file_name = os.path.basename(filepath)
        adls_url = f"abfss://{FILE_SYSTEM}@{STORAGE_NAME}.dfs.core.windows.net/{DIRECTORY_CLIENT}/{file_name}"
        return adls_url

    @task
    def advance_watermark(payload: dict):
        """
        Runs only after extraction, the DQ gate, the save, AND the upload have
        all succeeded (enforced below via >>). That ordering is what makes it
        safe to move the watermark forward — advancing it any earlier risks
        marking records as processed when they never actually landed.
        """
        if not payload["clean"]:
            logger.info("No new data; watermark left untouched.")
            return
        latest_submitted = max(r["published"] for r in payload["clean"])
        update_watermark(STORAGE_NAME, FILE_SYSTEM, latest_submitted)
 
    window_task = get_watermark_window()
    records_task = extract_batch(window_task)
    payload_task = apply_quality_checks(records_task, window_task)
    saved_path_task = save_batch(payload_task, window_task)
    adls_uri_task = send_to_azure(saved_path_task)
    
    advance_task = advance_watermark(payload_task)

    # 2. Atur ketergantungan menggunakan FULL ARROW di akhir
    adls_uri_task >> advance_task
 
arxiv_ingestion_pipeline()
