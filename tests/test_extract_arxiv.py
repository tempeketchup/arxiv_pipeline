import json
from datetime import datetime, timezone

import pytest
import requests
import tenacity

from scripts import extract_arxiv

@pytest.fixture(autouse=True)
def _fast_retries():
    extract_arxiv._fetch_page.retry.wait = tenacity.wait_none()

def test_build_search_query_format():
    start = datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, 0, 0, tzinfo=timezone.utc)
    query = extract_arxiv._build_search_query('machine learning', start, end)
    assert query == 'all:"machine learning" AND submittedDate:[202405010000 TO 202405020000]'

def test_fetch_arxiv_data_parses_expected_fields(make_feed, monkeypatch):
    feed_bytes = make_feed([{'id': '1111.1111', 'title': 'Paper One', 'category': 'cs.AI'}])
    monkeypatch.setattr(extract_arxiv, '_fetch_page', lambda params: feed_bytes)
    monkeypatch.setattr(extract_arxiv.time, 'sleep', lambda s: None)

    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)
    result = extract_arxiv.fetch_arxiv_data('ai', start, end)

    assert len(result) == 1
    assert result[0]['title'] == 'Paper One'
    assert result[0]['primary_category'] == 'cs.AI'
    assert '1111.1111' in result[0]['id']

def test_fetch_arxiv_data_stops_on_partial_page(make_feed, monkeypatch):
    monkeypatch.setattr(extract_arxiv, 'PAGE_SIZE', 2)
    page1 = make_feed([{'id': '1'}, {'id': '2'}])       # full page -> expect another call
    page2 = make_feed([{'id': '3'}])                    # partial page -> stop here
    pages = [page1, page2]
    calls = {'n': 0}

    def fake_fetch(params):
        result = pages[calls['n']]
        calls['n'] += 1
        return result
    
    monkeypatch.setattr(extract_arxiv, '_fetch_page', fake_fetch)
    monkeypatch.setattr(extract_arxiv.time, 'sleep', lambda s: None)

    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)
    result = extract_arxiv.fetch_arxiv_data('ai', start, end, max_results=3)

    assert len(result) == 3

def test_fetch_page_retries_then_succeeds(monkeypatch):
    attempts = {'n': 0}

    class FakeResponse:
        content = b"<feed></feed>"

        def raise_for_status(self):
            pass

    def flaky_get(*args, **kwargs):
        attempts['n'] += 1
        if attempts['n'] < 3:
            raise requests.exceptions.ConnectionError('simulated network blip')
        return FakeResponse()
    
    monkeypatch.setattr(extract_arxiv.requests, 'get', flaky_get)
    result = extract_arxiv._fetch_page({'search_query': 'x'})

    assert attempts['n'] == 3
    assert result == b'<feed></feed>'

def test_fetch_page_gives_up_after_max_attempts(monkeypatch):
    def always_fails(*args, **kwargs):
        raise requests.exceptions.ConnectionError('still down')
    
    monkeypatch.setattr(extract_arxiv.requests, 'get', always_fails)

    with pytest.raises(requests.exceptions.ConnectionError):
        extract_arxiv._fetch_page({'search_query': 'x'})
    

def test_save_to_json_writes_expected_content(tmp_path):
    records = [{'id': 'abc', 'title': 'Test Paper'}]
    filepath = extract_arxiv.save_to_json(
        records, query='test query', output_dir=str(tmp_path), batch_id='batch123'
    )

    assert 'test_query_batch123.json' in filepath
    with open(filepath) as f:
        saved = json.load(f)
    assert saved == records


