"""Explicit manual collection, discovery, analysis and insert-only enrollment."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .analysis import date, normalized_rows
from .collector import Collector, atomic, transport
from .models import Candidate, SourceReference, utcnow
from .report import generate
from .sources import catalogue, ingest_file, public_sources


def load_candidates(names):
    candidates, manifests = [], []
    for name in names:
        path = Path(name)
        if path.is_dir() and (path / 'rows.json').exists():
            manifest = json.loads((path / 'run_manifest.json').read_text())
            manifests.append(manifest)
            candidates += normalized_rows(json.loads((path / 'rows.json').read_text()), manifest['observed_interval_end'])
        else:
            path = path / 'candidates.jsonl' if path.is_dir() else path
            candidates += [Candidate.model_validate_json(line) for line in path.read_text().splitlines() if line.strip()]
            receipts = [path.parent / filename for filename in ('discovery.json', 'coverage.json')
                        if (path.parent / filename).exists()]
            for receipt_path in receipts:
                receipt = json.loads(receipt_path.read_text())
                manifests.append({
                    'collection_status': receipt.get('collection_status', receipt.get('status', 'unknown')),
                    'source_outcomes': receipt.get('source_outcomes', receipt.get('outcomes', [])),
                })
            if not receipts:
                manifests.append({'collection_status': 'unknown', 'source_outcomes': []})
    return candidates, manifests


async def discover(config, output, *, max_requests=100, max_references=20, references=()):
    from .discovery import NativeSources, SourceBlocked, candidate_references
    if not 1 <= max_requests <= 100 or not 0 <= max_references <= 40:
        raise ValueError('discovery_limit_out_of_range')
    day = str(utcnow().date())
    native = NativeSources(output, limit=max_requests)
    ports = {port for target in config['targets'] for port in target.get('ports', [])}
    # Public reports have their own existing bound of 20 direct requests; native
    # preview/reference requests share the explicit max_requests cap below.
    candidates, outcomes = await public_sources(config, output, day=day)
    refs, observed, preview_outcomes = await native.previews(config, day, ports)
    candidates += observed
    outcomes += preview_outcomes
    refs += list(references) + candidate_references(candidates)
    refs = list({(r.carrier, r.reference_kind, r.reference): r for r in refs}.values())
    checked = 0
    counts = {}
    for reference in refs:
        workflow = next((spec for spec in config.get('carrier_workflows', [])
                         if spec.get('enabled') and reference.carrier in spec['match_carriers']
                         and reference.reference_kind in spec.get('reference_kinds', ['booking', 'bill_of_lading'])), None)
        if workflow is None:
            outcomes.append({'reference_id': reference.id, 'status': 'no_supported_workflow'})
            continue
        if checked >= max_references or counts.get(workflow['id'], 0) >= min(40, workflow.get('max_references', 40)):
            outcomes.append({'reference_id': reference.id, 'status': 'reference_limit'})
            continue
        gate = native.workflow_gate(workflow)
        if gate:
            outcomes.append({'source': workflow['id'], 'reference_id': reference.id, 'status': 'cooldown', 'reason': gate['reason']})
            continue
        checked += 1
        counts[workflow['id']] = counts.get(workflow['id'], 0) + 1
        try:
            found, status = await native.expand(reference, workflow)
            candidates += found
            outcomes.append({'source': workflow['id'], 'reference_id': reference.id, 'status': status, 'ids': len(found)})
        except Exception as exc:
            reason = str(exc) if isinstance(exc, SourceBlocked) else type(exc).__name__
            status = 'capability_blocked' if reason.startswith('unsupported_capability:') else 'access_failed'
            outcomes.append({'source': workflow['id'], 'reference_id': reference.id, 'status': status, 'reason': reason})
            if reason not in ('native_request_limit', 'carrier_reference_mismatch'):
                native.workflow_gate(workflow, reason)
    output.mkdir(parents=True, exist_ok=True)
    candidates = list({candidate.id: candidate for candidate in candidates}.values())
    (output / 'candidates.jsonl').write_text(''.join(c.model_dump_json() + '\n' for c in candidates))
    (output / 'references.jsonl').write_text(''.join(r.model_dump_json() + '\n' for r in refs))
    incomplete = {'capability_blocked', 'access_failed', 'access_blocked', 'quota_blocked',
                  'parse_failed', 'reference_limit', 'cooldown', 'partial'}
    receipt = {'mode': 'manual_discovery', 'status': 'partial' if any(row['status'] in incomplete for row in outcomes) else 'complete',
               'candidates': len(candidates),
               'unique_ids': len({c.container_number for c in candidates}), 'references_checked': checked,
               'native_requests': native.calls, 'outcomes': outcomes, 'enrolled': 0}
    atomic(output / 'discovery.json', receipt)
    return receipt


def parser():
    result = argparse.ArgumentParser(description='Manual container discovery; enrollment is a separate insert-only command.')
    commands = result.add_subparsers(dest='command', required=True)
    collect = commands.add_parser('collect', help='Read bounded source records through configured MCP')
    collect.add_argument('--connection', default='prodheadrundb')
    collect.add_argument('--output', required=True)
    collect.add_argument('--resume', help='Existing checkpoint.json path')
    collect.add_argument('--page-size', type=int, default=100)
    collect.add_argument('--max-rows', type=int, default=10000)
    collect.add_argument('--max-calls', type=int, default=150)
    analyze = commands.add_parser('analyze', help='Rank saved candidates without database or network access')
    analyze.add_argument('--input', action='append', required=True)
    analyze.add_argument('--catalog')
    analyze.add_argument('--terminal', action='append')
    analyze.add_argument('--as-of')
    analyze.add_argument('--top-n', type=int, default=5)
    analyze.add_argument('--fetch-days', type=int, default=7)
    analyze.add_argument('--movement-days', type=int, default=14)
    analyze.add_argument('--output', required=True)
    ingest = commands.add_parser('ingest', help='Parse saved evidence without network access')
    ingest.add_argument('--input', required=True)
    ingest.add_argument('--mapping', required=True)
    ingest.add_argument('--retrieved-at', required=True, help='Original source retrieval time; never refresh old evidence')
    ingest.add_argument('--output', required=True)
    discovery = commands.add_parser('discover', help='Manually run bounded direct HTTP discovery; never enroll')
    discovery.add_argument('--catalog')
    discovery.add_argument('--references', help='Saved SourceReference JSONL to expand')
    discovery.add_argument('--max-requests', type=int, default=100)
    discovery.add_argument('--max-references', type=int, default=20)
    discovery.add_argument('--output', required=True)
    enrollment = commands.add_parser('enroll', help='Preview new IDs; --apply inserts into the existing database')
    enrollment.add_argument('--input', action='append', required=True)
    enrollment.add_argument('--catalog')
    enrollment.add_argument('--apply', action='store_true')
    enrollment.add_argument('--output', help='Optional JSON enrollment receipt')
    return result


async def run(args):
    if args.command == 'collect':
        output = Path(args.resume).parent if args.resume else Path(args.output)
        async with transport(args.connection) as session:
            receipt = await Collector(session, output, page_size=args.page_size, max_rows=args.max_rows, max_calls=args.max_calls).collect()
        return receipt, 0 if receipt['collection_status'] == 'complete' else 2
    if args.command == 'ingest':
        values = ingest_file(Path(args.input), catalogue(args.mapping), args.retrieved_at)
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        (output / 'candidates.jsonl').write_text(''.join(c.model_dump_json() + '\n' for c in values))
        return {'candidates': len(values), 'output': str(output)}, 0
    if args.command == 'discover':
        references = [SourceReference.model_validate_json(line) for line in Path(args.references).read_text().splitlines() if line.strip()] if args.references else []
        receipt = await discover(catalogue(args.catalog), Path(args.output), max_requests=args.max_requests,
                                 max_references=args.max_references, references=references)
        return receipt, 0 if receipt['status'] == 'complete' else 2
    candidates, manifests = load_candidates(args.input)
    config = catalogue(args.catalog)
    if args.command == 'enroll':
        from .enrollment import enroll_database
        receipt = await enroll_database(candidates, config, apply=args.apply)
        if args.output:
            atomic(Path(args.output), receipt)
        return receipt, 0
    as_of = date(args.as_of) if args.as_of else utcnow()
    if not as_of or as_of.tzinfo is None:
        raise ValueError('as_of_requires_UTC_offset')
    if args.terminal:
        config['targets'] = [t for t in config['targets'] if t['code'] in args.terminal]
    statuses = {m.get('collection_status', 'unknown') for m in manifests}
    collection_status = 'complete' if statuses == {'complete'} else 'unknown' if statuses <= {'unknown'} else 'partial'
    outcomes = {json.dumps(outcome, sort_keys=True): outcome
                for manifest in manifests for outcome in manifest.get('source_outcomes', [])}
    manifest = {'collection_status': collection_status, 'source_outcomes': list(outcomes.values())}
    _, coverage = generate(candidates, config, as_of, Path(args.output), manifest=manifest, top_n=args.top_n,
                           fetch_days=args.fetch_days, movement_days=args.movement_days)
    return {'candidates': len(candidates), 'terminals': len(coverage), 'output': args.output,
            **manifest}, 0 if collection_status == 'complete' else 2


def main():
    args = parser().parse_args()
    try:
        receipt, code = asyncio.run(run(args))
        print(json.dumps(receipt, default=str))
    except Exception as exc:
        # Database, HTTP and MCP errors may contain connection secrets.
        print(json.dumps({'status': 'failed', 'error_type': type(exc).__name__}))
        code = 2
    raise SystemExit(code)
