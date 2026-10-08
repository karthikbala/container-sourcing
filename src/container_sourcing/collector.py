"""Bounded, resumable SELECT-only MCP collection. Connection material never enters exports."""
from __future__ import annotations

import asyncio
import json
import os
import time
import tomllib
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from .models import digest, utcnow


def atomic(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, default=str)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def connection(name='prodheadrundb') -> dict:
    configured = os.environ.get('SOURCING_MCP_CONFIG')
    if configured:
        return json.loads(Path(configured).read_text())
    if os.environ.get('SOURCING_MCP_URL'):
        headers = {'Accept': 'application/json, text/event-stream'}
        if os.environ.get('SOURCING_MCP_TOKEN'):
            headers['Authorization'] = 'Bearer ' + os.environ['SOURCING_MCP_TOKEN']
        return {'url': os.environ['SOURCING_MCP_URL'], 'http_headers': headers}
    path = Path(os.environ.get('SOURCING_CODEX_CONFIG', str(Path.home() / '.codex/config.toml')))
    if not path.exists():
        raise RuntimeError('sourcing_mcp_not_configured')
    config = tomllib.loads(path.read_text()).get('mcp_servers', {}).get(name)
    if not config or not config.get('url'):
        raise RuntimeError('sourcing_mcp_connection_missing')
    return config


@asynccontextmanager
async def transport(name='prodheadrundb'):
    config = connection(name)
    try:
        async with httpx.AsyncClient(headers=config.get('http_headers', {}), timeout=45) as http:
            async with streamable_http_client(config['url'], http_client=http) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    yield session
    except Exception as e:
        # MCP exceptions can include URLs and credentials. Preserve no exception text.
        raise RuntimeError('sourcing_mcp_transport_failed:' + type(e).__name__) from None


class Collector:
    def __init__(self, session, output: Path, *, page_size=100, max_rows=10000, max_calls=150, timeout=600):
        if not 1 <= page_size <= 500:
            raise ValueError('page_size must be 1..500')
        self.session, self.output, self.page_size = session, output, page_size
        self.max_rows, self.max_calls, self.timeout = max_rows, max_calls, timeout
        self.started = time.monotonic()
        self.calls = 0
        cp = output / 'checkpoint.json'
        self.state = json.loads(cp.read_text()) if cp.exists() else {'schema_version': 1, 'stages': {}, 'created_at': utcnow().isoformat()}
        signature = digest({'version': 1, 'page_size': page_size, 'max_rows': max_rows})
        if self.state.get('config_hash', signature) != signature:
            raise ValueError('resume_configuration_changed')
        self.state['config_hash'] = signature

    def save(self):
        atomic(self.output / 'checkpoint.json', self.state)

    async def _call_tool(self, name, arguments):
        if self.calls >= self.max_calls or time.monotonic() - self.started >= self.timeout:
            raise RuntimeError('collection_limit_reached')
        self.calls += 1
        return await self.session.call_tool(name, arguments)

    async def query(self, sql, size=None):
        if not sql.lstrip().upper().startswith('SELECT ') or ';' in sql:
            raise ValueError('only fixed single SELECT templates are allowed')
        for retry in range(3):
            try:
                result = await self._call_tool('execute_read_query', {'sql': sql, 'page': 1, 'page_size': size or self.page_size})
            except (httpx.TransportError, TimeoutError):
                if retry == 2 or self.calls >= self.max_calls:
                    raise RuntimeError('mcp_transient_failure') from None
                await asyncio.sleep(2 ** retry)
                continue
            obj = result.structuredContent
            if obj is None:
                try:
                    obj = json.loads(next(c.text for c in result.content if c.type == 'text'))
                except (ValueError, StopIteration):
                    raise RuntimeError('mcp_invalid_envelope') from None
            if result.isError or not isinstance(obj, dict) or obj.get('error'):
                raise RuntimeError('mcp_query_rejected_or_failed')
            if not isinstance(obj.get('rows'), list):
                raise RuntimeError('mcp_missing_rows')
            return obj['rows']
        raise RuntimeError('mcp_collection_failed')

    async def pages(self, stage, table, columns):
        state = self.state['stages'].setdefault(stage, {'last': 0, 'pages': [], 'complete': False})
        if 'high' not in state:
            state['high'] = (await self.query(f'SELECT MAX(id) AS high FROM {table} LIMIT 1', 1))[0]['high'] or 0
            self.save()
        while not state['complete']:
            count = sum(p['rows'] for p in state['pages'])
            if count >= self.max_rows:
                raise RuntimeError('stage_row_cap_reached:' + stage)
            limit = min(self.page_size, self.max_rows - count)
            last = int(state['last'])
            sql = f'SELECT {columns} FROM {table} WHERE id > {last} AND id <= {int(state["high"])} ORDER BY id LIMIT {limit}'
            # Deterministic page path recovers a durable page written before its checkpoint.
            path = self.output / 'pages' / f'{stage}-{last}.json'
            rows = json.loads(path.read_text()) if path.exists() else await self.query(sql, limit)
            atomic(path, rows)
            if rows:
                state['last'] = rows[-1]['id']
            state['pages'].append({'file': str(path.relative_to(self.output)), 'rows': len(rows), 'hash': digest(rows)})
            state['complete'] = len(rows) < limit or state['last'] >= state['high']
            self.save()
        result = []
        for page in state['pages']:
            rows = json.loads((self.output / page['file']).read_text())
            if digest(rows) != page['hash']:
                raise RuntimeError('checkpoint_page_hash_mismatch')
            result.extend(rows)
        return result

    async def collect(self):
        manifest = {'schema_version': 1, 'collection_status': 'running', 'observed_interval_start': self.state['created_at'],
                    'consistency': 'best_effort_observed_interval', 'latest_policy': 'created_at_then_id', 'errors': []}
        try:
            for table, required in {'ds_item': {'id', 'key', 'reference_type', 'source_id'},
                                    'ds_itemdata': {'id', 'item_id', 'json_data_id', 'created_at'},
                                    'ds_jsondata': {'id', 'json'}}.items():
                result = await self._call_tool('describe_table', {'table_name': table})
                obj = result.structuredContent or json.loads(next(c.text for c in result.content if c.type == 'text'))
                if result.isError or not required <= {c['Field'] for c in obj.get('columns', [])}:
                    raise RuntimeError('incompatible_schema:' + table)
            items = await self.pages('items', 'ds_item', 'id, `key` AS reference_number, reference_type, source_id, crawled_at')
            versions = await self.pages('versions', 'ds_itemdata', 'id, item_id, json_data_id, created_at, crawled_at, data_hashkey')
            by_item = {r['id']: r for r in items}
            latest = {}
            for row in versions:
                item_id = row['item_id']
                if item_id in by_item and (str(row.get('created_at') or ''), row['id']) > \
                        (str(latest.get(item_id, {}).get('created_at') or ''), latest.get(item_id, {}).get('id', 0)):
                    latest[item_id] = row
            rows = []
            selected = sorted(latest.values(), key=lambda r: r['json_data_id'])
            for start in range(0, len(selected), self.page_size):
                page = selected[start:start + self.page_size]
                ids = ','.join(str(int(r['json_data_id'])) for r in page)
                path = self.output / 'pages' / f'payloads-{digest(ids)[:20]}.json'
                if path.exists():
                    payloads = json.loads(path.read_text())
                else:
                    payloads = await self.query("SELECT id, JSON_EXTRACT(json, '$.status') AS payload_status, "
                                                "JSON_EXTRACT(json, '$.shipments') AS shipments FROM ds_jsondata "
                                                f'WHERE id IN ({ids}) ORDER BY id LIMIT {len(page)}', len(page))
                    atomic(path, payloads)
                payload_map = {p['id']: p for p in payloads}
                for version in page:
                    payload = payload_map.get(version['json_data_id'])
                    if payload is None:
                        manifest['errors'].append({'stage': 'payload', 'data_id': version['id'], 'reason': 'missing_payload'})
                        continue
                    item = by_item[version['item_id']]
                    rows.append({**payload, 'item_id': item['id'], 'reference_number': item['reference_number'],
                                 'reference_type': item['reference_type'], 'source_id': item['source_id'],
                                 'data_id': version['id'], 'json_id': payload['id'], 'data_crawled_at': version.get('crawled_at')})
            links = []
            for kind, parent, relationship in [('booking', 'ocean_booking', 'ocean_bookingcontainertrip'),
                                                ('bill_of_lading', 'ocean_bol', 'ocean_bolcontainertrip')]:
                last, high = 0, None
                high_rows = await self.query(f'SELECT MAX(containertrip_ptr_id) AS high FROM {relationship} LIMIT 1', 1)
                high = high_rows[0]['high'] or 0
                while last < high:
                    page = await self.query(f'SELECT l.containertrip_ptr_id AS trip_id, p.id AS parent_id, p.`key` AS reference_number, '
                                            f'c.`key` AS container_number FROM {relationship} l JOIN {parent} p ON p.id=l.parent_id '
                                            'JOIN ocean_containertrip c ON c.id=l.containertrip_ptr_id '
                                            f'WHERE l.valid=1 AND l.containertrip_ptr_id > {int(last)} AND l.containertrip_ptr_id <= {int(high)} '
                                            f'ORDER BY l.containertrip_ptr_id LIMIT {self.page_size}')
                    if not page:
                        break
                    links.extend([{**r, 'reference_kind': kind, 'relationship_table': relationship} for r in page])
                    last = page[-1]['trip_id']
                    if len(links) > self.max_rows:
                        raise RuntimeError('relationship_row_cap_reached')
            atomic(self.output / 'links.json', links)
            atomic(self.output / 'rows.json', rows)
            manifest.update(collection_status='partial' if manifest['errors'] else 'complete',
                            items_scanned=len(items), versions_scanned=len(versions), payloads_selected=len(rows),
                            unsuccessful_payloads=sum(r['payload_status'] not in ('SUCCESS', '"SUCCESS"') for r in rows),
                            items_without_payload=len(items) - len(latest), relationship_rows=len(links))
        except Exception as e:
            manifest['collection_status'] = 'partial'
            manifest['errors'].append({'stage': 'collection', 'reason': str(e) if isinstance(e, RuntimeError) else type(e).__name__})
        manifest.update(mcp_calls=self.calls, observed_interval_end=utcnow().isoformat(), elapsed_seconds=round(time.monotonic() - self.started, 2))
        atomic(self.output / 'run_manifest.json', manifest)
        return manifest

    async def recent(self, since: str):
        """Read changed items and newest raw versions, without scanning version history."""
        from datetime import datetime
        datetime.strptime(since, '%Y-%m-%d %H:%M:%S')
        rows, raw, last = [], [], 0
        while len(rows)<1000:
            page=await self.query("SELECT i.id AS item_id,i.`key` AS reference_number,i.reference_type,i.source_id,"
                "d.id AS data_id,d.crawled_at AS data_crawled_at,j.id AS json_id,"
                "JSON_EXTRACT(j.json,'$.status') AS payload_status,JSON_EXTRACT(j.json,'$.shipments') AS shipments "
                "FROM ds_item i JOIN ds_itemdata d ON d.item_id=i.id JOIN ds_jsondata j ON j.id=d.json_data_id "
                f"WHERE i.id>{last} AND (i.crawled_at>='{since}' OR d.crawled_at>='{since}') "
                "AND d.id=(SELECT d2.id FROM ds_itemdata d2 WHERE d2.item_id=i.id ORDER BY d2.created_at DESC,d2.id DESC LIMIT 1) "
                f"ORDER BY i.id LIMIT {self.page_size}")
            rows+=page
            if len(page)<self.page_size:
                break
            last=int(page[-1]['item_id'])
        # Bounded recent raw payloads include known carrier references even if normalization failed.
        raw=await self.query("SELECT c.id AS trip_id,c.`key` AS container_number,c.crawled_at,d.id AS data_id,"
                "JSON_EXTRACT(j.json,'$.detail') AS detail,JSON_EXTRACT(j.json,'$.statuses') AS statuses "
                "FROM ocean_containertrip c JOIN ocean_containertripdata1 d ON d.item_id=c.id "
                "JOIN ocean_containertripjsondata j ON j.id=d.json_data_id "
                f"WHERE c.crawled_at>='{since}' AND JSON_EXTRACT(j.json,'$.detail') IS NOT NULL AND d.id=(SELECT d2.id FROM ocean_containertripdata1 d2 "
                "WHERE d2.item_id=c.id ORDER BY d2.created_at DESC,d2.id DESC LIMIT 1) "
                "ORDER BY c.crawled_at DESC,c.id DESC LIMIT 100",100)
        manifest={'collection_status':'partial' if len(rows)>=1000 else 'complete','items_scanned':len(rows),
                  'raw_trips':len(raw),'mcp_calls':self.calls,'observed_interval_end':utcnow().isoformat(),
                  'latest_policy':'created_at_then_id','incremental_since':since}
        atomic(self.output/'rows.json',rows);atomic(self.output/'raw.json',raw);atomic(self.output/'run_manifest.json',manifest)
        return rows,raw,manifest
