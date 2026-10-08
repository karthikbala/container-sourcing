import json

import pytest

from container_sourcing import cli


@pytest.mark.asyncio
@pytest.mark.parametrize('source_status', ['capability_blocked', 'quota_blocked'])
async def test_partial_discovery_remains_partial_through_saved_analysis(tmp_path, source_status):
    discovered = tmp_path / 'discovered'
    discovered.mkdir()
    (discovered / 'candidates.jsonl').write_text('')
    outcome = {'source': 'synthetic-source', 'status': source_status, 'ids': 0}
    (discovered / 'discovery.json').write_text(json.dumps({'status': 'partial', 'outcomes': [outcome]}))
    catalog = tmp_path / 'catalog.yaml'
    catalog.write_text('targets:\n  - code: synthetic-yard\n    name: Synthetic Yard\n    ports: [USOAK]\n    facilities: [Synthetic Yard]\n')
    source = discovered
    for index in range(2):
        output = tmp_path / f'analysis-{index}'
        args = cli.parser().parse_args(['analyze', '--input', str(source), '--catalog', str(catalog),
                                       '--output', str(output), '--as-of', '2026-10-08T10:00:00Z'])
        receipt, code = await cli.run(args)
        coverage = json.loads((output / 'coverage.json').read_text())
        assert code == 2
        assert receipt['collection_status'] == coverage['collection_status'] == 'partial'
        assert receipt['source_outcomes'] == coverage['source_outcomes'] == [outcome]
        assert coverage['terminals'][0]['status'] == 'collection_incomplete'
        assert 'Collection status: partial.' in (output / 'summary.md').read_text()
        source = output


def test_candidate_file_without_source_receipt_has_unknown_completeness(tmp_path):
    path = tmp_path / 'candidates.jsonl'
    path.write_text('')
    candidates, manifests = cli.load_candidates([path])
    assert candidates == []
    assert manifests == [{'collection_status': 'unknown', 'source_outcomes': []}]
