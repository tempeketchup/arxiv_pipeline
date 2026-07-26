# data quality check
import json
import os
import logging
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ['id', 'title', 'authors', 'published']


class DataQualityError(Exception):
    # Raised when a batch's rejection rate is bad enough to halt the pipeline
    pass

def _is_null_or_empty(value):
    if value is None:       
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    return False

def check_required_fields(record):
    return [f for f in REQUIRED_FIELDS if _is_null_or_empty(record.get(f))]

def check_valid_date(record):
    published = record.get('published')
    if not published:
        return False
    try:
        datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except (ValueError, TypeError):
        return False
    
def run_quality_checks(records):
    seen_ids = set()
    clean = []
    rejected = []
    reasons = Counter()

    for record in records:
        missing = check_required_fields(record)
        if missing:
            rejected.append({**record, '_rejection_reason': f"missing_fields:{','.join(missing)}"})
            reasons['missing_fields'] += 1
            continue

        if not check_valid_date(record):
            rejected.append({**record, '_rejection_reason': 'invalid_published_date'})
            reasons['invalid_date'] += 1
            continue

        record_id = record.get('id')
        if record_id in seen_ids:
            rejected.append({**record, '_rejection_reason': 'duplicate_in_batch'})
            reasons['duplicate_in_batch'] += 1
            continue

        seen_ids.add(record_id)
        clean.append(record)

    report = {
            'total_input': len(records),
            'clean_count': len(clean),
            'rejected_count': len(rejected),
            'rejection_breakdown': dict(reasons),
    }
    logger.info(f'Data quality report: {report}')
    return clean, rejected, report

def enforce_quality_gate(report, max_rejection_rate=0.2):
    if report['total_input'] == 0:
        return 
    rejection_rate = report['rejected_count'] / report['total_input']
    if rejection_rate > max_rejection_rate:
        raise DataQualityError(
            f"Rejection rate {rejection_rate:.1%} exceeds threshold"
            f"{max_rejection_rate:.1%}"
            f"({report['rejected_count']}/{report['total_input']} records rejected)"
        )
    
def save_rejected(rejected, output_dir, query, batch_id):
    if not rejected:
        return None
    quarantine_dir = os.path.join(output_dir, 'rejected')
    os.makedirs(quarantine_dir, exist_ok=True)
    safe_query = query.replace(' ', '_')
    filepath = os.path.join(quarantine_dir, f'{safe_query}_{batch_id}_rejected.json')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(rejected, f, indent=2, ensure_ascii=False)

    logger.warning(f'Quarantined {len(rejected)} records to {filepath}')
    return filepath
    
