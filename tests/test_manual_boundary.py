import ast
import importlib
from pathlib import Path

import httpx
import pytest

from container_sourcing import cli, discovery, enrollment, iso6346, sources
from container_sourcing.models import Candidate


def test_imports_and_catalog_need_no_application_host():
    package = Path(sources.__file__).parent
    for path in package.glob('*.py'):
        if path.stem != '__main__':
            importlib.import_module('container_sourcing.' + path.stem)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                assert all(not name.name.startswith('porttrack') for name in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module or '').startswith('porttrack')
                assert node.level <= 1
    assert sources.catalogue()['targets']


def test_cli_has_only_manual_commands_and_enrollment_defaults_to_preview():
    args = cli.parser().parse_args(['enroll', '--input', 'candidates.jsonl'])
    assert args.apply is False
    with pytest.raises(SystemExit):
        cli.parser().parse_args(['daily'])


@pytest.mark.asyncio
async def test_unsupported_fallback_has_no_provider_or_browser_execution(tmp_path):
    native = discovery.NativeSources(tmp_path)
    with pytest.raises(discovery.SourceBlocked, match='unsupported_capability:browser_recipe'):
        await native.managed({'browser_fallback': {'steps': []}}, {})
    with pytest.raises(discovery.SourceBlocked, match='unsupported_capability:managed_provider'):
        await native.managed({'managed_fallback': 'scrapedo'}, {})
    with pytest.raises(sources.UnsupportedSourceCapability, match='managed_provider'):
        await sources.managed_content({'url': 'https://example.test'})


@pytest.mark.asyncio
async def test_challenged_public_source_reports_capability_blocked(monkeypatch, tmp_path):
    original = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=b'Verify you are human'))
    monkeypatch.setattr(sources.httpx, 'AsyncClient', lambda **kwargs: original(transport=transport, **kwargs))
    candidates, outcomes = await sources.public_sources({'sources': [{
        'id': 'synthetic', 'enabled': True, 'url': 'https://example.test/list',
        'allowed_hosts': ['example.test'], 'format': 'csv',
    }]}, tmp_path, day='2026-10-08')
    assert candidates == []
    assert outcomes == [{'source': 'synthetic', 'status': 'capability_blocked',
                         'capability': 'managed_provider', 'ids': 0}]


@pytest.mark.asyncio
async def test_discover_reports_blocked_capability_without_enrolling(monkeypatch, tmp_path):
    async def public(config, output, *, day):
        return [], [{'source': 'synthetic', 'status': 'capability_blocked', 'capability': 'managed_provider', 'ids': 0}]

    async def previews(self, config, day, ports):
        return [], [], []

    monkeypatch.setattr(cli, 'public_sources', public)
    monkeypatch.setattr(discovery.NativeSources, 'previews', previews)
    receipt = await cli.discover({'targets': []}, tmp_path)
    assert receipt['status'] == 'partial'
    assert receipt['enrolled'] == receipt['native_requests'] == receipt['unique_ids'] == 0
    assert (tmp_path / 'candidates.jsonl').read_text() == ''


def sample(*, key='ABCU1234560', facility='Synthetic Yard', movement='2026-10-07T10:00:00Z'):
    from datetime import datetime, timezone
    now = datetime(2026, 10, 8, 10, tzinfo=timezone.utc)
    loc = {'unlocode': 'USOAK', 'terminal': facility, 'role': 'portOfDischarge'}
    candidate = Candidate(id='synthetic', container_number=key, source='synthetic-report', trip_id='synthetic-trip',
                          retrieved_at=now.isoformat(), fetched_at=now.isoformat(), ports=[loc],
                          events=[{'eventCode': 'DISC', 'eventQualifier': 'A', 'eventTime': movement,
                                   'scope': 'container', 'role': 'portOfDischarge', 'location': loc}])
    return now, candidate, {'targets': [{'code': 'synthetic-yard', 'ports': ['USOAK'], 'facilities': ['Synthetic Yard']}]}


def test_enrollment_rechecks_evidence_rejecting_port_only_old_invalid_and_ambiguous():
    now, candidate, config = sample()
    assert iso6346.is_valid(candidate.container_number)
    rows, _ = enrollment.eligible_rows([candidate], config, as_of=now)
    assert [(r['terminal'], r['key']) for r in rows] == [('synthetic-yard', 'ABCU1234560')]
    for changes in ({'facility': None}, {'movement': '2025-01-01T10:00:00Z'}, {'key': 'ABCU1234561'}):
        _, rejected, _ = sample(**changes)
        assert enrollment.eligible_rows([rejected], config, as_of=now)[0] == []
    config['targets'].append(config['targets'][0] | {'code': 'second-yard'})
    rows, skips = enrollment.eligible_rows([candidate], config, as_of=now)
    assert rows == []
    assert skips[-1]['reason'] == 'ambiguous_terminal'


@pytest.mark.asyncio
async def test_database_url_has_no_default_or_host_environment_fallback(monkeypatch):
    monkeypatch.delenv('CONTAINER_SOURCING_DATABASE_URL', raising=False)
    monkeypatch.setenv('DATABASE_URL', 'postgresql://ignored.invalid/ignored')
    with pytest.raises(ValueError, match='CONTAINER_SOURCING_DATABASE_URL_required'):
        await enrollment.enroll_database([], {'targets': []})
