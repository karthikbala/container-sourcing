from __future__ import annotations

import csv
import json
from pathlib import Path

from .analysis import proposals
from .collector import atomic
from .models import digest


def csv_file(path, rows, columns):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def generate(candidates, config, as_of, output: Path, *, manifest=None, top_n=5, fetch_days=7, movement_days=14):
    output.mkdir(parents=True, exist_ok=True)
    manifest = manifest or {'collection_status': 'unknown', 'source_outcomes': []}
    candidates = list({c.id: c for c in candidates}.values())
    ranked = proposals(candidates, config['targets'], as_of, fetch_days=fetch_days, movement_days=movement_days)
    coverage, shortlist, rejected, inputs = [], [], [], []
    for target in config['targets']:
        ps = [p for p in ranked if p.terminal_code == target['code']]
        eligible = [p for p in ps if p.eligible][:top_n]
        status = 'excluded_from_container_sourcing' if target.get('container_sourcing_enabled', True) is False else 'candidates_available' if eligible else ('mapping_unconfigured' if not target.get('ports') else
                 'collection_incomplete' if manifest and manifest.get('collection_status') != 'complete' else
                 'no_candidates_in_successfully_scanned_data')
        coverage.append({'terminal': target['code'], 'name': target['name'], 'status': status,
                         'proposed': len(eligible), 'evidence_trips': len(ps), 'eligible_total': sum(p.eligible for p in ps)})
        inputs.append({'terminal': target['code'], 'keys': list(dict.fromkeys(p.candidate.container_number for p in eligible)),
                       'portal_verified': False, 'api_verified': False})
        for p in ps:
            row = {'terminal': p.terminal_code, 'container': p.candidate.container_number, 'reference': p.candidate.reference,
                   'source': p.candidate.source, 'direction': p.direction, 'match_basis': p.match_basis,
                   'movement_at': p.movement_at, 'disposition': p.disposition, 'priority': p.priority,
                   'reasons': ';'.join(p.reasons), 'candidate_id': p.candidate.id}
            if not p.eligible:
                rejected.append(row)
            elif p in eligible:
                shortlist.append(row)
    columns = ['terminal','container','reference','source','direction','match_basis','movement_at','disposition','priority','reasons','candidate_id']
    csv_file(output / 'terminal_shortlist.csv', shortlist, columns)
    csv_file(output / 'rejected.csv', rejected, columns)
    temp = output / 'candidates.jsonl.tmp'
    with temp.open('w') as f:
        for c in candidates:
            f.write(json.dumps(c.model_dump() | {'decisions': [p.model_dump(exclude={'candidate'}) for p in ranked if p.candidate.id == c.id]}, default=str) + '\n')
    temp.replace(output / 'candidates.jsonl')
    atomic(output / 'coverage.json', {'as_of': as_of.isoformat(), 'config_hash': digest(config), 'terminals': coverage,
                                     'collection_status': manifest.get('collection_status', 'unknown'),
                                     'source_outcomes': manifest.get('source_outcomes', []),
                                     'unique_candidates': len(candidates), 'unique_ids': len({c.container_number for c in candidates})})
    atomic(output / 'test_inputs.json', inputs)
    lines = ['# Container sourcing', '', f'As of {as_of.isoformat()}. Proposed IDs require terminal/API validation.', '',
             f'Collection status: {manifest.get("collection_status", "unknown")}.', '',
             '| Terminal | Suitable trips | Status |', '|---|---:|---|']
    lines += [f'| {r["name"]} | {r["proposed"]} | {r["status"]} |' for r in coverage]
    lines += ['', '## Shortlist', '', '| Terminal | Container | Reference | Match | Movement |', '|---|---|---|---|---|']
    lines += [f'| {r["terminal"]} | {r["container"]} | {r["reference"] or ""} | {r["match_basis"]} | {r["movement_at"] or "unknown"} |' for r in shortlist]
    temp = output / 'summary.md.tmp'
    temp.write_text('\n'.join(lines) + '\n')
    temp.replace(output / 'summary.md')
    return ranked, coverage
