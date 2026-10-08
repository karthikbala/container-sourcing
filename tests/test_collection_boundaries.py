"""Offline regressions for catalog-bound evidence and bounded MCP requests."""

import json
from types import SimpleNamespace

import httpx
import pytest

from container_sourcing import collector, sources
from container_sourcing.models import digest


def source_spec():
    return {'id': 'fixture', 'enabled': True, 'url': 'https://example.test/first',
            'allowed_hosts': ['example.test'], 'format': 'csv',
            'fields': {'container_number': 'container_number', 'port': 'port'}}


@pytest.mark.parametrize('change', ['url', 'fields'])
async def test_public_cache_reuses_only_the_same_reviewed_source(monkeypatch, tmp_path, change):
    requests = []
    def respond(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=b'container_number,port,other_port\nABCU1234560,USOAK,USLAX\n')
    original = httpx.AsyncClient
    monkeypatch.setattr(sources.httpx, 'AsyncClient',
                        lambda **kw: original(transport=httpx.MockTransport(respond), **kw))
    spec = source_spec()
    first, receipts = await sources.public_sources({'sources': [spec]}, tmp_path, day='2026-10-08')
    again, reused = await sources.public_sources({'sources': [spec]}, tmp_path, day='2026-10-08')
    assert len(requests) == 1 and again == first and reused == receipts
    changed = spec | ({'url': 'https://example.test/second'} if change == 'url' else
                      {'fields': spec['fields'] | {'port': 'other_port'}})
    updated, outcomes = await sources.public_sources({'sources': [changed]}, tmp_path, day='2026-10-08')
    assert len(requests) == 2 and outcomes[0]['status'] == 'id_rows_observed'
    assert updated[0].evidence['source_url'] == changed['url']
    if change == 'fields':
        assert updated[0].ports[0]['unlocode'] == 'USLAX'


async def test_cached_bytes_cannot_bypass_current_allowlist(monkeypatch, tmp_path):
    spec = source_spec() | {'allowed_hosts': []}
    body = b'container_number,port\nABCU1234560,USOAK\n'
    path = tmp_path / 'source-cache' / '2026-10-08' / (spec['id'] + '-' + digest(spec) + '.bin')
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    path.with_suffix('.json').write_text(json.dumps({
        'retrieved_at': '2026-10-08T10:00:00Z', 'config_hash': digest(spec), 'hash': digest(body.hex()),
    }))
    def unexpected(**kwargs):
        raise AssertionError('allowlist rejection must precede HTTP or cache parsing')
    monkeypatch.setattr(sources.httpx, 'AsyncClient', unexpected)
    candidates, outcomes = await sources.public_sources({'sources': [spec]}, tmp_path, day='2026-10-08')
    assert candidates == []
    assert outcomes == [{'source': 'fixture', 'status': 'parse_failed', 'error_type': 'ValueError', 'ids': 0}]


@pytest.mark.parametrize('max_calls', [0, 1, 2, 3])
async def test_schema_inspection_consumes_the_same_call_budget_as_queries(tmp_path, max_calls):
    calls = []
    columns = {'ds_item': ['id', 'key', 'reference_type', 'source_id'],
               'ds_itemdata': ['id', 'item_id', 'json_data_id', 'created_at'],
               'ds_jsondata': ['id', 'json']}
    class Session:
        async def call_tool(self, name, arguments):
            calls.append(name)
            assert name == 'describe_table'
            return SimpleNamespace(isError=False, structuredContent={
                'columns': [{'Field': field} for field in columns[arguments['table_name']]],
            })
    receipt = await collector.Collector(Session(), tmp_path, max_calls=max_calls).collect()
    assert len(calls) == receipt['mcp_calls'] == max_calls
    assert receipt['collection_status'] == 'partial'
    assert receipt['errors'] == [{'stage': 'collection', 'reason': 'collection_limit_reached'}]


async def test_retry_does_not_issue_another_request_after_time_budget_expires(monkeypatch, tmp_path):
    clock, calls = [0], []
    monkeypatch.setattr(collector.time, 'monotonic', lambda: clock[0])
    class Session:
        async def call_tool(self, name, arguments):
            calls.append(name)
            raise httpx.ReadTimeout('synthetic timeout')
    async def sleep(delay):
        clock[0] += delay
    monkeypatch.setattr(collector.asyncio, 'sleep', sleep)
    collection = collector.Collector(Session(), tmp_path, max_calls=10, timeout=1)
    with pytest.raises(RuntimeError, match='collection_limit_reached'):
        await collection.query('SELECT id FROM ds_item LIMIT 1')
    assert calls == ['execute_read_query'] and collection.calls == 1
