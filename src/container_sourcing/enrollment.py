"""Manual insert-only enrollment into an existing PortTrack database.

No schema setup, tracking updates, source-proof writes or job scheduling belongs
here. The normal PortTrack worker decides when and how to poll the new row.
"""
from __future__ import annotations

import os
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from . import iso6346
from .analysis import proposals
from .models import utcnow


INSERT = """INSERT INTO tracked_items (terminal_id,kind,key,aux_info,created_by)
    VALUES (%s,'container',%s,%s,'porttrack-sourcing')
    ON CONFLICT (terminal_id,kind,key) DO NOTHING RETURNING id"""


async def _insert_new(connection, terminal_id, row):
    # All standalone writers use the same per-ID lock. The transaction is explicit
    # so supplied autocommit connections preserve the lock through the insert.
    async with connection.transaction():
        await connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 147701))', (row['key'],))
        known = await (await connection.execute(
            "SELECT id,terminal_id FROM tracked_items WHERE kind='container' AND key=%s", (row['key'],))).fetchall()
        if known:
            same_terminal = next((item for item in known if item['terminal_id'] == terminal_id), None)
            return {'status': 'already_exists' if same_terminal else 'known_at_other_terminal',
                    'id': (same_terminal or known[0])['id']}
        inserted = await (await connection.execute(
            INSERT, (terminal_id, row['key'], Jsonb(row['aux_info'])))).fetchone()
        if inserted:
            return {'status': 'inserted', 'id': inserted['id']}
        original = await (await connection.execute(
            "SELECT id FROM tracked_items WHERE terminal_id=%s AND kind='container' AND key=%s",
            (terminal_id, row['key']))).fetchone()
        return {'status': 'already_exists', 'id': original['id'] if original else None}


def eligible_rows(candidates, config, *, as_of=None):
    """Recompute decisions from evidence; saved report decisions are not authority."""
    now = as_of or utcnow()
    candidates = list({candidate.id: candidate for candidate in candidates}.values())
    ranked = proposals(candidates, config['targets'], now)
    matched = defaultdict(list)
    skips = []
    ranked_ids = {p.candidate.id for p in ranked}
    for candidate in candidates:
        if candidate.id not in ranked_ids:
            skips.append({'candidate_id': candidate.id, 'reason': 'no_terminal_match'})
    for proposal in ranked:
        candidate = proposal.candidate
        key = iso6346.normalize(candidate.container_number)
        explicit = (proposal.match_basis == 'facility' or
                    candidate.evidence.get('terminal_code') == proposal.terminal_code)
        if not iso6346.is_valid(key):
            reason = 'invalid_container'
        elif not proposal.eligible or proposal.direction != 'import':
            reason = proposal.disposition
        elif not explicit:
            reason = 'terminal_match_not_explicit'
        elif proposal.priority > 2 or not proposal.movement_at:
            reason = 'no_observed_container_movement'
        else:
            matched[key].append(proposal)
            continue
        skips.append({'candidate_id': candidate.id, 'terminal': proposal.terminal_code, 'reason': reason})
    rows = []
    for key, group in sorted(matched.items()):
        codes = {proposal.terminal_code for proposal in group}
        if len(codes) != 1:
            skips.append({'key': key, 'reason': 'ambiguous_terminal', 'terminals': sorted(codes)})
            continue
        rows.append({'key': key, 'terminal': next(iter(codes)), 'aux_info': {'sourcing': {
            'method': 'manual_insert',
            'candidate_ids': sorted({p.candidate.id for p in group}),
            'sources': sorted({p.candidate.source for p in group}),
            'trip_ids': sorted({p.candidate.trip_id for p in group}),
            'retrieved_at': sorted({p.candidate.retrieved_at for p in group}),
            'proposed_at': now.isoformat(),
            'portal_verified': False,
            'api_verified': False,
        }}})
    return rows, skips


async def enroll(connection, candidates, config, *, apply=False, as_of=None):
    """Preview or apply new terminal enrollments; conflicts always preserve originals."""
    candidates = list(candidates)
    rows, skips = eligible_rows(candidates, config, as_of=as_of)
    terminals = {row['code']: row['id'] for row in await (
        await connection.execute('SELECT id,code FROM terminals')).fetchall()}
    keys = sorted({iso6346.normalize(candidate.container_number) for candidate in candidates
                   if iso6346.is_valid(candidate.container_number)})
    existing = await (await connection.execute(
        "SELECT terminal_id,key FROM tracked_items WHERE kind='container' AND key=ANY(%s)",
        (keys,))).fetchall()
    existing_pairs = {(row['terminal_id'], row['key']) for row in existing}
    existing_keys = {row['key'] for row in existing}
    outcomes = []
    for row in rows:
        terminal_id = terminals.get(row['terminal'])
        outcome = {'terminal': row['terminal'], 'key': row['key']}
        if terminal_id is None:
            outcome['status'] = 'unknown_terminal'
        elif (terminal_id, row['key']) in existing_pairs:
            outcome['status'] = 'already_exists'
        elif row['key'] in existing_keys:
            outcome['status'] = 'known_at_other_terminal'
        elif not apply:
            outcome['status'] = 'would_insert'
        else:
            outcome.update(await _insert_new(connection, terminal_id, row))
        outcomes.append(outcome)
    return {
        'mode': 'apply' if apply else 'dry_run',
        'candidate_records': len(candidates),
        'unique_discovered_ids': len(keys),
        'new_discovered_ids': len(set(keys) - existing_keys),
        'eligible_terminal_enrollments': len(rows),
        'inserted_terminal_enrollments': sum(row['status'] == 'inserted' for row in outcomes),
        'would_insert_terminal_enrollments': sum(row['status'] == 'would_insert' for row in outcomes),
        'already_existing_terminal_enrollments': sum(row['status'] == 'already_exists' for row in outcomes),
        'known_at_other_terminal': sum(row['status'] == 'known_at_other_terminal' for row in outcomes),
        'outcomes': outcomes,
        'skipped': skips,
    }


async def enroll_database(candidates, config, *, apply=False):
    url = os.environ.get('CONTAINER_SOURCING_DATABASE_URL')
    if not url:
        raise ValueError('CONTAINER_SOURCING_DATABASE_URL_required')
    async with await psycopg.AsyncConnection.connect(url, row_factory=dict_row) as connection:
        return await enroll(connection, candidates, config, apply=apply)
