import json
import pytest
from scripts import data_quality

GOOD_RECORD = {
    'id': '1111.1111',
    'title': 'A Great Paper',
    'summary': 'summary text',
    'authors': ['A. Researcher'],
    'published': '2024-05-01T12:00:00Z',
    'primary_category': 'cs.AI',
    'categories': ['cs.AI']
}

def make_record(**overrides):
    record = GOOD_RECORD.copy()
    record.update(overrides)
    return record

def test_check_required_fields_passes_on_good_record():
    assert data_quality.check_required_fields(GOOD_RECORD) == []

@pytest.mark.parametrize(
    'field, value',
    [
        ('title', ''),
        ('title', None),
        ('authors', []),
        ('published', None)
    ],
)
def test_check_required_fields_catches_missing(field, value):
    record = make_record(**{field: value})
    assert field in data_quality.check_required_fields(record)

def test_check_valid_date_accepts_iso_format():
    assert data_quality.check_valid_date(GOOD_RECORD) is True

@pytest.mark.parametrize('bad_date', ['not-a-date', '', None, '2024-05-01'])
def test_check_valid_date_rejects_bad_formats(bad_date):
    record = make_record(published=bad_date)
    assert data_quality.check_valid_date(record) is False

def test_run_quality_checks_separates_clean_and_rejected():
    records = [
        make_record(id='a1'),
        make_record(id='a1'),           # duplicate of the one above
        make_record(id='a1', title=''), # missing required field
        make_record(id='a3', published='garbage'),  # invalid date
        make_record(id='a4'),           # clean
    ]
    clean, rejected, report = data_quality.run_quality_checks(records)

    assert len(clean) == 2      # a1 (first occurence) + a4
    assert len(rejected) == 3
    assert report['total_input'] == 5
    assert report['rejection_breakdown']['duplicate_in_batch'] == 1
    assert report['rejection_breakdown']['missing_fields'] == 1
    assert report['rejection_breakdown']['invalid_date'] == 1

def test_enforce_quality_gate_passes_under_threshold():
    report = {'total_input': 10, 'rejected_count':  1}
    data_quality.enforce_quality_gate(report, max_rejection_rate=0.2)   # should not raise

def test_enforce_quality_gate_raises_over_threshold():
    report = {'total_input': 10, 'rejected_count':  5}
    with pytest.raises(data_quality.DataQualityError):
        data_quality.enforce_quality_gate(report, max_rejection_rate=0.2)

def test_enforce_quality_gate_ignores_empty_batch():
    report = {'total_input': 0, 'rejected_count': 0}
    data_quality.enforce_quality_gate(report, max_rejection_rate=0.2)   # should not raise

def test_save_rejected_returns_none_when_nothing_to_quarantine(tmp_path):
    assert data_quality.save_rejected([], str(tmp_path), 'q', 'batch1') is None

def test_save_rejected_writes_expected_file(tmp_path):
    rejected = [{'id': 'x', '_rejection_reason': 'missing_fields:title'}]
    path = data_quality.save_rejected(rejected, str(tmp_path), 'q', 'batch1')

    assert path is not None
    with open(path) as f:
        saved = json.load(f)
    assert saved == rejected
