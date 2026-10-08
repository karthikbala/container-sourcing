"""Pure evidence normalization and explainable ranking; no network or database calls."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse

from . import iso6346
from .models import Candidate, Proposal, digest


def decoded(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def date(value):
    try:
        return parse(str(value)) if value else None
    except (ValueError, TypeError, OverflowError):
        return None


def text(value) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip().casefold()


def temporal(value, as_of: datetime) -> tuple[float | None, list[str]]:
    dt = date(value)
    if dt is None:
        return None, ['missing_or_invalid_time']
    flags = []
    if dt.tzinfo is None:
        flags.append('timezone_unknown')
        # Compare as an interval covering legal UTC offsets. Never invent a Z suffix.
        elapsed = (as_of.astimezone(timezone.utc).replace(tzinfo=None) - dt).total_seconds() / 86400
        if elapsed < -14 / 24:
            return elapsed, flags + ['future_time']
        if abs(elapsed) <= 14 / 24:
            flags.append('time_boundary_uncertain')
        return elapsed, flags
    elapsed = (as_of - dt).total_seconds() / 86400
    return elapsed, flags + (['future_time'] if elapsed < 0 else [])


def location(loc) -> dict:
    loc = loc if isinstance(loc, dict) else {}
    return {k: loc.get(k) for k in ('name', 'city', 'country', 'unlocode', 'terminal') if loc.get(k)}


def corroborate_calls(candidates, calls, as_of):
    """Reviewed schedule context ranks a hypothesis; it does not create cargo IDs."""
    out=[]
    for original in candidates:
        c=original.model_copy(deep=True)
        matches=[]
        for call in calls:
            age,_=temporal(call.get('retrieved_at'),as_of)
            if age is None or not 0<=age<=7 or not call.get('terminal') or not call.get('source_url'):
                continue
            if not text(c.evidence.get('vessel')) or text(c.evidence.get('vessel'))!=text(call.get('vessel')):
                continue
            if not text(c.evidence.get('voyage')) or text(c.evidence.get('voyage'))!=text(call.get('voyage')):
                continue
            for e in c.events:
                if e.get('role')!='portOfDischarge' or (e.get('location') or {}).get('unlocode')!=call.get('port'):
                    continue
                a,b=date(e.get('eventTime')),date(call.get('call_at'))
                if a and b and abs((a.replace(tzinfo=None)-b.replace(tzinfo=None)).total_seconds())<=3*86400:
                    matches.append(call)
        if matches:
            c.evidence['vessel_calls']=matches
        out.append(c)
    return out


def normalized_rows(rows: list[dict], retrieved_at: str) -> list[Candidate]:
    out = []
    for row in rows:
        if decoded(row.get('payload_status')) != 'SUCCESS':
            continue
        shipments = decoded(row.get('shipments'))
        if not isinstance(shipments, list):
            continue
        for index, shipment in enumerate(shipments):
            if not isinstance(shipment, dict):
                continue
            key = iso6346.normalize(shipment.get('containerNumber'))
            flags = [] if iso6346.is_valid(key) else ['invalid_container']
            stops = [s for s in shipment.get('stops', []) or [] if isinstance(s, dict)]
            ports = [{**location(s.get('location')), 'role': s.get('stopType'), 'stop_index': i}
                     for i, s in enumerate(stops)]
            events = []
            for e in shipment.get('events', []) or []:
                if not isinstance(e, dict):
                    continue
                si = e.get('stopIndex')
                stop = stops[si] if isinstance(si, int) and 0 <= si < len(stops) else {}
                events.append({k: e.get(k) for k in ('status', 'eventCode', 'eventTime', 'eventQualifier', 'mode',
                                                   'vesselInfo', 'voyageReference')} |
                              {'location': location(e.get('location')) or location(stop.get('location')),
                               'role': stop.get('stopType'), 'scope': 'shipment_container', 'stop_index': si})
            ref = row.get('reference_number')
            trip = digest([key, ref, row.get('source_id'), shipment.get('startedAt'),
                           shipment.get('currentVessel'), [(p.get('unlocode'), p.get('role')) for p in ports]])[:24]
            evidence = {k: row.get(k) for k in ('item_id', 'data_id', 'json_id', 'reference_type', 'source_id')}
            evidence.update(shipment_index=index, payload_hash=digest(shipment), completed_at=shipment.get('completedAt'))
            out.append(Candidate(id=digest(['trackone', trip])[:32], container_number=key, reference=ref,
                                 reference_kind=row.get('reference_type'), source='trackone', trip_id=trip,
                                 retrieved_at=retrieved_at, fetched_at=row.get('data_crawled_at'),
                                 ports=ports, events=events, evidence=evidence, quality_flags=flags))
    return out


def external_row(row: dict, source: dict, retrieved_at: str, pointer: dict) -> list[Candidate]:
    containers = row.get('container_number') or row.get('containers') or []
    if isinstance(containers, str):
        containers = re.findall(r'(?<![A-Z0-9])[A-Z]{4}[ -]?\d{6}[ -]?\d(?!\d)', containers.upper())
    if not isinstance(containers, list):
        return []
    out = []
    for value in containers:
        key = iso6346.normalize(value.get('id') if isinstance(value, dict) else value)
        ref = row.get('reference') or row.get('bol_number')
        movement = row.get('movement_at') or row.get('arrival_date')
        terminal = row.get('terminal') or source.get('facility')
        port = row.get('port') or source.get('port')
        role = row.get('role') or source.get('role', 'portOfDischarge')
        loc = {'unlocode': port, 'terminal': terminal}
        trip = digest([key, ref, movement, row.get('vessel'), row.get('voyage'), port])[:24]
        category = source.get('category', 'manifest')
        event = {'eventTime': movement, 'eventQualifier': row.get('qualifier', 'E'),
                 'eventCode': row.get('event_code', 'MANIFEST'), 'location': loc, 'role': role,
                 'scope': 'container' if category in ('public_exam_list', 'public_discharge_report', 'carrier_tracking_event') else 'manifest'}
        events = [event]
        # Saved carrier timelines must be bound to one observed container row.
        if row.get('events') and len(containers) == 1 and category == 'carrier_tracking_event':
            events = [dict(e, scope='container') for e in row['events'] if isinstance(e, dict)]
        out.append(Candidate(id=digest([source['id'], trip])[:32], container_number=key, reference=ref,
                             reference_kind='bill_of_lading', source=source['id'], trip_id=trip,
                             retrieved_at=retrieved_at, fetched_at=row.get('fetched_at') or retrieved_at,
                             ports=[loc | {'role': role}], events=events,
                             evidence=pointer | ({'bill_type': row['bill_type']} if row.get('bill_type') else {}) | {'category': category, 'source_url': source.get('url'),
                                                 'reported_at': movement, 'row': row.get('row_index'),
                                                 'vessel': row.get('vessel'), 'voyage': row.get('voyage')},
                             quality_flags=[] if iso6346.is_valid(key) else ['invalid_container']))
    return out


def proposals(candidates: list[Candidate], targets: list[dict], as_of: datetime, *, fetch_days=7, movement_days=14) -> list[Proposal]:
    out = []
    carrier_destinations = {}
    for c in candidates:
        age, flags = temporal(c.fetched_at, as_of)
        if not c.reference or not c.evidence.get('carrier_lookup_verified') or age is None or not 0 <= age <= fetch_days or 'future_time' in flags:
            continue
        destinations = [p for p in c.ports if p.get('role') == 'portOfDischarge' and p.get('unlocode')]
        if destinations:
            carrier_destinations.setdefault((c.container_number, text(c.reference)), []).extend(destinations)
    for c in candidates:
        for t in targets:
            if t.get('container_sourcing_enabled', True) is False:
                continue
            if c.evidence.get('terminal_code') and c.evidence['terminal_code'] != t['code']:
                continue
            ports = {p.upper() for p in t.get('ports', [])}
            facilities = {text(p) for p in t.get('facilities', [])}
            associations = [p for p in c.ports if str(p.get('unlocode', '')).upper() in ports
                            or (text(p.get('terminal')) in facilities and p.get('terminal'))]
            if not associations:
                continue
            known=[p for p in c.ports if p.get('role')=='portOfDischarge' and p.get('terminal')]
            if c.evidence.get('carrier_lookup_verified') and known and not any(text(p['terminal']) in facilities for p in known):
                continue
            exact = any(text(p.get('terminal')) in facilities and p.get('terminal') for p in associations)
            reasons = list(c.quality_flags)
            disp, priority, movement = 'needs_terminal_resolution', 3, None
            direction = 'import' if any(p.get('role') == 'portOfDischarge' for p in associations) else 'unclear'
            if any(p.get('role') == 'portOfLoading' for p in associations) and direction != 'import':
                direction = 'export'
            if any(p.get('role') == 'transshipmentPort' for p in associations) and direction == 'unclear':
                direction = 'transshipment'
            fetch_age, flags = temporal(c.fetched_at, as_of)
            reasons += flags
            actual, departed, future_actual = [], [], False
            for e in c.events:
                loc = e.get('location') or {}
                belongs = str(loc.get('unlocode', '')).upper() in ports or text(loc.get('terminal')) in facilities
                code = str(e.get('eventCode') or '').upper()
                category = t.get('event_codes', {}).get(code, code)
                # Delivery elsewhere can end this container visit. Vessel departure elsewhere cannot.
                if not belongs and category != 'DELIVERY':
                    continue
                age, flags = temporal(e.get('eventTime'), as_of)
                reasons += flags
                qual = e.get('eventQualifier')
                if qual not in ('A', 'ACT'):
                    continue
                if age is not None and 'future_time' in flags:
                    future_actual = True
                    continue
                # Vessel arrival does not constitute a container discharge.
                if category in ('DISC', 'GTIN', 'UNLOAD') and e.get('scope') != 'manifest':
                    if age is not None:
                        actual.append((age, e.get('eventTime')))
                elif category in ('GTOT', 'DEPA', 'LOAD', 'DELIVERY'):
                    if age is not None:
                        departed.append((age, e.get('eventTime')))
                elif category not in ('ARRI', 'MANIFEST'):
                    reasons.append('unknown_actual_event:' + code)
            if actual:
                age, movement = min(actual)
                if age >= 0 and age <= movement_days and 'time_boundary_uncertain' not in reasons:
                    priority = 1 if exact else 2
                    disp = 'candidate_for_portal_test' if exact else 'needs_terminal_resolution'
                else:
                    disp = 'historical_or_departed' if age > movement_days else 'conflicting_evidence'
            else:
                arrivals = [e for e in c.events if e.get('role') == 'portOfDischarge' and
                            (str((e.get('location') or {}).get('unlocode', '')).upper() in ports or
                             text((e.get('location') or {}).get('terminal')) in facilities)]
                if arrivals:
                    times = [temporal(e.get('eventTime'), as_of)[0] for e in arrivals]
                    ages = [a for a in times if a is not None]
                    movement = arrivals[-1].get('eventTime')
                    if not ages:
                        disp = 'insufficient_evidence'
                    elif min(ages) < 0:
                        disp = 'upcoming'
                    elif min(ages) > movement_days:
                        disp = 'historical_or_departed'
                    reasons.append('no_observed_container_discharge')
                else:
                    disp = 'insufficient_evidence'
            if departed and (not actual or min(departed)[0] <= min(actual)[0]):
                disp = 'historical_or_departed'
                reasons.append('departure_after_arrival')
            if c.evidence.get('completed_at'):
                disp = 'historical_or_departed'
                reasons.append('shipment_completed')
            if direction in ('export', 'transshipment'):
                disp = 'wrong_direction'
            if direction == 'unclear':
                disp = 'insufficient_evidence'
            if fetch_age is None or fetch_age > fetch_days or 'future_time' in temporal(c.fetched_at, as_of)[1]:
                disp = 'insufficient_evidence'
                reasons.append('source_freshness_unusable')
            if future_actual:
                disp = 'conflicting_evidence'
                reasons.append('future_event_marked_actual')
            if 'invalid_container' in reasons:
                disp = 'invalid_container'
            if t.get('direction', 'import') != 'import':
                disp = 'wrong_direction'
            manifest = c.evidence.get('category') in ('manifest', 'public_manifest_preview') or any(e.get('scope') == 'manifest' for e in c.events)
            if manifest and re.search(r'\bFROB\b', str(c.evidence.get('bill_type') or ''), re.I) and any(p.startswith('US') for p in ports):
                direction, disp = 'transshipment', 'wrong_direction'
                reasons.append('foreign_cargo_remaining_on_board')
            destinations = carrier_destinations.get((c.container_number, text(c.reference)), [])
            if manifest and destinations:
                carrier_ports = {str(p['unlocode']).upper() for p in destinations}
                carrier_facilities = {text(p['terminal']) for p in destinations
                                      if str(p['unlocode']).upper() in ports and p.get('terminal')}
                if ports.isdisjoint(carrier_ports):
                    disp = 'conflicting_evidence'
                    reasons.append('carrier_discharge_port_conflict_same_reference')
                elif carrier_facilities and facilities.isdisjoint(carrier_facilities):
                    disp = 'conflicting_evidence'
                    reasons.append('carrier_discharge_facility_conflict_same_reference')
            call_match=any(text(x.get('terminal')) in facilities and x.get('port') in ports for x in c.evidence.get('vessel_calls',[]))
            if call_match:
                reasons.append('reviewed_vessel_call_hypothesis')
                priority=min(priority,2)
            out.append(Proposal(candidate=c, terminal_code=t['code'], match_basis='facility' if exact else 'vessel_call' if call_match else 'port',
                                direction=direction, disposition=disp, priority=priority,
                                reasons=sorted(set(reasons)), movement_at=movement))
    # Corroborate only the same observed reference and container visit. Container
    # identity alone cannot join a historical trip to a newly reused container.
    for p in out:
        if not p.eligible or not p.candidate.reference or not p.movement_at:
            continue
        for other in out:
            if other is p or (other.terminal_code != p.terminal_code and not other.candidate.evidence.get('carrier_lookup_verified')):
                continue
            if (other.candidate.container_number != p.candidate.container_number or
                text(other.candidate.reference) != text(p.candidate.reference)):
                continue
            if 'departure_after_arrival' not in other.reasons or not other.movement_at:
                continue
            a, b = date(p.movement_at), date(other.movement_at)
            if a and b and abs((a.replace(tzinfo=None)-b.replace(tzinfo=None)).days) <= 21:
                p.disposition = 'historical_or_departed'
                p.reasons += ['corroborated_departure_same_reference', 'evidence_candidate:' + other.candidate.id]
                break
    return sorted(out, key=lambda p: (p.terminal_code, not p.eligible, p.priority,
                                     -(date(p.movement_at).replace(tzinfo=timezone.utc).timestamp() if date(p.movement_at) else 0),
                                     p.candidate.container_number, p.candidate.id))
