import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'))

import pytest

ATOM_ENTRY_TEMPLATE = """
<entry>
    <id>http://arxiv.org/abs/{id}v1</id>
    <title>{title}</title>
    <summary>{summary}</summary>
    <published>{published}</published>
    <author>
        <name>{author}</name>
    </author>
    <arxiv:primary_category xmlns:arxiv="http://arxiv.org/schemas/atom" term="{category}" scheme="http://arxiv.org/schemas/atom"/>
    <category term="{category}" scheme="http://arxiv.org/schemas/atom"/>
</entry>
"""

ATOM_FEED_TEMPLATE = """<?xml version='1.0'     encoding='UTF-8'?>
<feed xmlns='http://www.w3.org/2005/Atom' xmlns:arxiv='http://arxiv.org/schemas/atom'>
    <title>ArXiv Query</title>
    {entries}
</feed>
"""

def _make_atom_feed(records):
    entries_xml = "".join(
        ATOM_ENTRY_TEMPLATE.format(
            id= r.get('id', '0000.0000'),
            title=r.get('title', 'Untitled'),
            summary=r.get('summary', 'A summary'),
            published=r.get('published', '2024-05-01T00:00:00Z'),
            author=r.get('author', 'A. Researcher'),
            category=r.get('category', 'cs.AI')
        )
        for r in records
    )
    return ATOM_FEED_TEMPLATE.format(entries=entries_xml).encode('utf-8')

@pytest.fixture
def make_feed():
    return _make_atom_feed