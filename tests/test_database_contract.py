"""Optional contract lane against the actual, already migrated PortTrack schema.

Set CONTAINER_SOURCING_TEST_DATABASE_URL to a disposable localhost database named
container_sourcing_test_<suffix>, migrate it with PortTrack, then run this file.
Default standalone CI skips this lane. These tests create no tables, import no
PortTrack code, and remove only their uniquely named fixture terminals/items.
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.types.json import Jsonb

from container_sourcing import enrollment, iso6346
from container_sourcing.models import Candidate


@pytest.fixture
def database_url():
    value = os.environ.get('CONTAINER_SOURCING_TEST_DATABASE_URL')
    if not value:
        pytest.skip('optional real-schema lane: CONTAINER_SOURCING_TEST_DATABASE_URL is unset')
    assert not any(os.environ.get(name) for name in ('PGHOSTADDR', 'PGSERVICE', 'PGSERVICEFILE')), (
        'Unset ambient PostgreSQL routing before using the disposable database')
    parsed = urlsplit(value)
    name = parsed.path.removeprefix('/')
    assert (parsed.scheme in {'postgresql', 'postgres'}
            and parsed.hostname in {'localhost', '127.0.0.1', '::1'}
            and name.startswith('container_sourcing_test_')
            and name != 'container_sourcing_test_'
            and all(c.isascii() and (c.isalnum() or c == '_') for c in name)
            and not parsed.query and not parsed.fragment and parsed.port != 0), (
        'Tests require an explicit localhost container_sourcing_test_<suffix> URL without options')
    return value


@pytest_asyncio.fixture
async def database(database_url):
    async with await psycopg.AsyncConnection.connect(database_url, row_factory=dict_row) as connection:
        schema = await (await connection.execute("""SELECT
            to_regclass('terminals') AS terminals, to_regclass('tracked_items') AS tracked_items,
            to_regclass('container_state') AS container_state, to_regclass('runs') AS runs""")).fetchone()
        assert all(schema.values()), 'Migrate the actual PortTrack schema before running this lane'
        suffix = uuid4().hex
        targets, terminal_ids = [], []
        for index, port in enumerate(('USOAK', 'USLAX')):
            code, facility = f'contract-{suffix}-{index}', f'Contract Fixture Yard {suffix} {index}'
            row = await (await connection.execute(
                'INSERT INTO terminals(code,name) VALUES(%s,%s) RETURNING id',
                (code, facility))).fetchone()
            terminal_ids.append(row['id'])
            targets.append({'code': code, 'ports': [port], 'facilities': [facility]})
        await connection.commit()
        try:
            yield connection, terminal_ids, {'targets': targets}
        finally:
            await connection.rollback()
            await connection.execute('DELETE FROM terminals WHERE id=ANY(%s)', (terminal_ids,))
            await connection.commit()


def candidate(target, now, *, key=None):
    if key is None:
        first10 = f'TSTU{uuid4().int % 1_000_000:06d}'
        key = first10 + str(iso6346.check_digit(first10))
    location = {'unlocode': target['ports'][0], 'terminal': target['facilities'][0],
                'role': 'portOfDischarge'}
    identity = uuid4().hex
    return Candidate(
        id=identity, container_number=key, source='synthetic-contract-fixture', trip_id=identity,
        retrieved_at=now.isoformat(), fetched_at=now.isoformat(), ports=[location],
        events=[{'eventCode': 'DISC', 'eventQualifier': 'A', 'scope': 'container',
                 'role': 'portOfDischarge', 'eventTime': (now - timedelta(hours=1)).isoformat(),
                 'location': location}],
    )


async def snapshot(connection, terminal_ids):
    items = await (await connection.execute(
        'SELECT * FROM tracked_items WHERE terminal_id=ANY(%s) ORDER BY id', (terminal_ids,))).fetchall()
    states = await (await connection.execute(
        'SELECT * FROM container_state WHERE tracked_item_id=ANY(%s) ORDER BY tracked_item_id',
        ([item['id'] for item in items],))).fetchall()
    terminals = await (await connection.execute(
        'SELECT * FROM terminals WHERE id=ANY(%s) ORDER BY id', (terminal_ids,))).fetchall()
    jobs = await (await connection.execute('SELECT count(*) AS count FROM runs')).fetchone()
    return {'items': items, 'states': states, 'terminals': terminals, 'jobs': jobs}


@pytest.mark.asyncio
async def test_dry_run_and_apply_preserve_existing_tracking(database, database_url):
    connection, terminal_ids, config = database
    now = datetime.now(timezone.utc)
    fresh, stopped, source_owned, elsewhere = [candidate(config['targets'][0], now) for _ in range(4)]
    expected_key = fresh.container_number
    fresh.container_number = f'{expected_key[:4].lower()} {expected_key[4:10]}-{expected_key[10]}'
    for evidence, terminal_id, status, owner in (
        (stopped, terminal_ids[0], 'stopped', 'existing-customer'),
        (source_owned, terminal_ids[0], 'completed', 'porttrack-sourcing'),
        (elsewhere, terminal_ids[1], 'active', 'existing-customer'),
    ):
        item = await (await connection.execute("""INSERT INTO tracked_items
            (terminal_id,key,status,status_reason,created_by,aux_info,not_found_count,error_count,
             crawl_priority,next_poll_at,last_polled_at,last_changed_at)
            VALUES(%s,%s,%s,'retain existing visit',%s,%s,3,2,7,%s,%s,%s) RETURNING id""",
            (terminal_id, evidence.container_number, status, owner,
             Jsonb({'customer': 'keep', 'sourcing': {'trip_ids': ['old-visit']}}),
             now + timedelta(days=3), now - timedelta(days=1), now - timedelta(days=2)))).fetchone()
        await connection.execute("""INSERT INTO container_state
            (tracked_item_id,raw,raw_hash,fk,data_hash,events,revision,collected_at,changed_at,checked_at)
            VALUES(%s,%s,'raw-before',%s,'mapped-before',%s,9,%s,%s,%s)""",
            (item['id'], Jsonb({'Container': evidence.container_number, 'Location': 'old-yard'}),
             Jsonb({'readyForDelivery': False}), Jsonb([{'eventCode': 'GTOT'}]), now, now, now))
    await connection.commit()
    before = await snapshot(connection, terminal_ids)
    candidates = [fresh, stopped, source_owned, elsewhere]

    # PostgreSQL itself rejects writes here, including writes whose row count is zero.
    async with await psycopg.AsyncConnection.connect(database_url, row_factory=dict_row) as preview:
        await preview.execute('SET TRANSACTION READ ONLY')
        result = await enrollment.enroll(preview, candidates, config, as_of=now)
    assert result['mode'] == 'dry_run'
    assert result['would_insert_terminal_enrollments'] == 1
    assert result['already_existing_terminal_enrollments'] == 2
    assert result['known_at_other_terminal'] == 1
    assert await snapshot(connection, terminal_ids) == before

    result = await enrollment.enroll(connection, candidates, config, apply=True, as_of=now)
    await connection.commit()
    assert result['inserted_terminal_enrollments'] == 1
    assert result['already_existing_terminal_enrollments'] == 2
    assert result['known_at_other_terminal'] == 1
    after = await snapshot(connection, terminal_ids)
    original_ids = {item['id'] for item in before['items']}
    assert [item for item in after['items'] if item['id'] in original_ids] == before['items']
    assert after['states'] == before['states']
    assert after['terminals'] == before['terminals']
    assert after['jobs'] == before['jobs']
    [inserted] = [item for item in after['items'] if item['id'] not in original_ids]
    assert inserted['key'] == expected_key and inserted['terminal_id'] == terminal_ids[0]
    assert inserted['kind'] == 'container' and inserted['created_by'] == 'porttrack-sourcing'
    assert inserted['status'] == 'active' and inserted['status_reason'] is None
    assert inserted['crawl_priority'] == inserted['not_found_count'] == inserted['error_count'] == 0
    assert inserted['last_polled_at'] is None and inserted['last_changed_at'] is None
    assert now <= inserted['next_poll_at'] <= datetime.now(timezone.utc)
    assert inserted['created_at'] == inserted['updated_at'] == inserted['next_poll_at']
    assert inserted['aux_info']['sourcing']['candidate_ids'] == [fresh.id]
    assert inserted['aux_info']['sourcing']['trip_ids'] == [fresh.trip_id]
    assert inserted['aux_info']['sourcing']['portal_verified'] is False
    assert inserted['aux_info']['sourcing']['api_verified'] is False

    repeated = await enrollment.enroll(connection, candidates, config, apply=True, as_of=now)
    await connection.commit()
    assert repeated['inserted_terminal_enrollments'] == 0
    assert repeated['already_existing_terminal_enrollments'] == 3
    assert await snapshot(connection, terminal_ids) == after


@pytest.mark.asyncio
async def test_invalid_and_ambiguous_evidence_cannot_insert(database):
    connection, terminal_ids, config = database
    now = datetime.now(timezone.utc)
    invalid = candidate(config['targets'][0], now, key='ABCU1234561')
    ambiguous = candidate(config['targets'][0], now)
    # Both real terminals claim the same exact facility: no safe unique destination.
    config['targets'][1]['facilities'].append(config['targets'][0]['facilities'][0])
    before = await snapshot(connection, terminal_ids)
    result = await enrollment.enroll(connection, [invalid, ambiguous], config, apply=True, as_of=now)
    assert result['inserted_terminal_enrollments'] == 0
    assert {'invalid_container', 'ambiguous_terminal'} <= {row['reason'] for row in result['skipped']}
    assert await snapshot(connection, terminal_ids) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('second_terminal', [False, True], ids=['same-terminal', 'different-terminal'])
async def test_concurrent_duplicate_has_one_winner(database, database_url, second_terminal):
    connection, terminal_ids, config = database
    now = datetime.now(timezone.utc)
    first = candidate(config['targets'][0], now)
    second = candidate(config['targets'][int(second_terminal)], now, key=first.container_number)
    before = await snapshot(connection, terminal_ids)
    start = asyncio.Barrier(2)

    async def apply(evidence):
        async with await psycopg.AsyncConnection.connect(database_url, row_factory=dict_row) as writer:
            await writer.execute("SET LOCAL lock_timeout = '5s'")
            await start.wait()
            return await enrollment.enroll(writer, [evidence], config, apply=True, as_of=now)

    tasks = [asyncio.create_task(apply(evidence)) for evidence in (first, second)]
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15)
    assert sum(result['inserted_terminal_enrollments'] for result in results) == 1
    statuses = sorted(row['status'] for result in results for row in result['outcomes'])
    assert statuses == sorted(['inserted', 'known_at_other_terminal' if second_terminal else 'already_exists'])
    after = await snapshot(connection, terminal_ids)
    assert len(after['items']) == 1 and after['items'][0]['key'] == first.container_number
    assert after['states'] == before['states'] == []
    assert after['jobs'] == before['jobs']
    assert after['terminals'] == before['terminals']


@pytest.mark.asyncio
async def test_insert_only_database_role_can_enroll_but_cannot_mutate_or_read_secrets(database):
    connection, terminal_ids, config = database
    role = sql.Identifier('container_sourcing_contract_' + uuid4().hex)
    now = datetime.now(timezone.utc)
    evidence = candidate(config['targets'][0], now)
    # Match ops/porttrack-role.sql without creating a login or handling a password.
    await connection.execute(sql.SQL('CREATE ROLE {} NOSUPERUSER NOCREATEDB NOCREATEROLE '
                                     'NOINHERIT NOREPLICATION NOLOGIN').format(role))
    try:
        database_name = (await (await connection.execute('SELECT current_database() AS name')).fetchone())['name']
        await connection.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(
            sql.Identifier(database_name), role))
        for permission in (
            'USAGE ON SCHEMA public',
            'SELECT (id,code) ON public.terminals',
            'SELECT (id,terminal_id,kind,key) ON public.tracked_items',
            'INSERT (terminal_id,kind,key,aux_info,created_by) ON public.tracked_items',
            'USAGE ON SEQUENCE public.tracked_items_id_seq',
        ):
            await connection.execute(sql.SQL('GRANT ' + permission + ' TO {}').format(role))
        await connection.commit()
        async with connection.transaction():
            await connection.execute(sql.SQL('SET LOCAL ROLE {}').format(role))
            preview = await enrollment.enroll(connection, [evidence], config, as_of=now)
            assert preview['would_insert_terminal_enrollments'] == 1
            applied = await enrollment.enroll(connection, [evidence], config, apply=True, as_of=now)
            assert applied['inserted_terminal_enrollments'] == 1
        for forbidden in (
            'UPDATE tracked_items SET status=\'stopped\' WHERE key=%s',
            'DELETE FROM tracked_items WHERE key=%s',
            'SELECT aux_info FROM tracked_items WHERE key=%s',
            'SELECT password_enc FROM credentials WHERE name=%s',
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with connection.transaction():
                    await connection.execute(sql.SQL('SET LOCAL ROLE {}').format(role))
                    await connection.execute(forbidden, (evidence.container_number,))
        state = await snapshot(connection, terminal_ids)
        assert len(state['items']) == 1 and state['items'][0]['status'] == 'active'
    finally:
        await connection.rollback()
        await connection.execute(sql.SQL('DROP OWNED BY {}').format(role))
        await connection.execute(sql.SQL('DROP ROLE {}').format(role))
        await connection.commit()
