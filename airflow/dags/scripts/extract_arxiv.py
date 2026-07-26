import requests
import feedparser 
import json
import os
import time
import logging
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

ARXIV_BASE_URL = 'https://export.arxiv.org/api/query'
PAGE_SIZE = 100
REQUEST_DELAY_SECONDS = 3

@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=3, min=5, max=60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def _fetch_page(params):
    response = requests.get(ARXIV_BASE_URL, params=params, timeout=45)
    response.raise_for_status()
    return response.content

def _build_search_query(query, start_date, end_date):
    start_fmt = start_date.strftime('%Y%m%d%H%M')
    end_fmt = end_date.strftime('%Y%m%d%H%M')
    return f'all:"{query}" AND submittedDate:[{start_fmt} TO {end_fmt}]'

def _parse_entry(entry):
    return {
        'id': entry.id,
        'title': entry.title,
        'summary': entry.summary,
        'authors': [a.name for a in entry.authors] if hasattr(entry, 'authors') else [],
        'published': entry.published if hasattr(entry, 'published') else None,
        'primary_category': entry.arxiv_primary_category['term'] if hasattr(entry, 'arxiv_primary_category') else None,
        'categories': [t['term'] for t in entry.tags] if hasattr(entry, 'tags') else []
    }

def fetch_arxiv_data(query, start_date, end_date, max_results=None):
    search_query = _build_search_query(query, start_date, end_date)
    all_entries = []
    start_offset = 0

    while True:
        page_size = min(PAGE_SIZE, max_results - len(all_entries)) if max_results else PAGE_SIZE
        params = {
            'search_query': search_query,
            'start': start_offset,
            'max_results': page_size,
            'sortBy': 'submittedDate',
            'sortOrder': 'ascending'
        }

        raw = _fetch_page(params)
        feed = feedparser.parse(raw)

        if not feed.entries:
            break

        all_entries.extend(_parse_entry(e) for e in feed.entries)
        start_offset += len(feed.entries)

        if max_results and len(all_entries) > max_results:
            all_entries = all_entries[:max_results]
        
        is_last_page = len(feed.entries) < page_size
        hit_cap = max_results and len(all_entries) >= max_results
        if is_last_page or hit_cap:
            break

        time.sleep(REQUEST_DELAY_SECONDS)

    logger.info(
        f"Fetched {len(all_entries)} entries for query='{query}' "
        f"between {start_date.isoformat()} and {end_date.isoformat()}"
        )

    return all_entries
    
def save_to_json(data, query, output_dir, batch_id=None):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    safe_query = query.replace(' ', '_')
    suffix = batch_id or timestamp
    filepath = os.path.join(output_dir, f'{safe_query}_{suffix}.json')

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return filepath