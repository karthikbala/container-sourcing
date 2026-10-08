"""Generic reference discovery and configured JSON carrier workflows; no recurring models."""
from __future__ import annotations

import json
import re
import asyncio
import base64
import logging
from datetime import timedelta, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urljoin

import httpx
from jsonpath_ng.ext import parse as jsonpath
from lxml import html

from . import iso6346
from .templates import render
from .analysis import date, decoded, location
from .collector import atomic
from .models import Candidate, SourceReference, digest, utcnow
from .sources import challenge, extract_rows, mapped_row


class SourceBlocked(RuntimeError):
    pass


class _SafeCarrierLog(logging.Filter):
    def filter(self, record):
        message=record.getMessage()
        if re.search(r'[?&](subscription-key|api[_-]?key|token)=',message,re.I):
            record.msg=re.sub(r'([?&](?:subscription-key|api[_-]?key|token)=)[^&\s"\']+',r'\1[redacted]',message,flags=re.I)
            record.args=()
        return True


logging.getLogger('httpx').addFilter(_SafeCarrierLog())


@lru_cache(maxsize=256)
def expression(path):
    return jsonpath(path)


def pick(obj, path, default=None):
    values = [m.value for m in expression(path).find(obj)]
    return values[0] if values else default


def response_body(content, spec):
    if spec.get('format') == 'html':
        return html.fromstring(content)
    body=json.loads(content)
    codec=spec.get('response_decode')
    if codec and isinstance(body,str):
        if codec['type']!='base64_xor':
            raise ValueError('unsupported_response_codec')
        mask=codec['mask'].encode()
        if not mask:
            raise ValueError('empty_response_mask')
        encoded=base64.b64decode(body,validate=True)
        body=json.loads(bytes(value^mask[i%len(mask)] for i,value in enumerate(encoded)))
    return body


def response_values(body, path, spec):
    if spec.get('format') == 'html':
        value = body.xpath(path)
        return value if isinstance(value, list) else [value]
    return [m.value for m in expression(path).find(body)]


def response_fields(row, spec, scope=None):
    fields = {}
    for key, path in spec.get('fields', {}).items():
        if isinstance(path,dict) and 'template' in path:
            fields[key]=render(path['template'],dict(scope or {},row=row))
            continue
        values = response_values(row, path, spec)
        value = values[0] if values else None
        if hasattr(value, 'text_content'):
            value = ' '.join(value.text_content().split())
        fields[key] = value.strip() if isinstance(value, str) else value
    return mapped_row(fields, spec)


def valid_envelope(body, spec):
    if spec.get('format') == 'html':
        return bool(body.xpath(spec['success_xpath']))
    return pick(body, spec.get('success_path', '$.code')) == spec.get('success_value', 1)


def not_found_envelope(body, spec):
    check=spec.get('not_found')
    return bool(check and response_values(body,check['path'],spec)==[check['value']])


def canonical_reference(value, prefixes=()):
    value=str(value).strip()
    return next((value[len(p):] for p in prefixes if value.startswith(p)),value)


def verify_reference(body, spec, reference, prefixes=()):
    if spec.get('reference_path'):
        echoed=response_values(body,spec['reference_path'],spec)
        if canonical_reference(reference,prefixes) not in [canonical_reference(v,prefixes) for v in echoed]:
            raise SourceBlocked('carrier_reference_mismatch')


def reference_rows(content, spec):
    if spec.get('format') not in ('html_table', 'html_pairs'):
        return extract_rows(content, spec)
    doc = html.fromstring(content)
    rows = []
    for table in doc.xpath(spec.get('tables', '//table')):
        if spec['format'] == 'html_pairs':
            pairs = {}
            for tr in table.xpath('.//tr'):
                th, td = tr.xpath('./th'), tr.xpath('./td')
                if len(th) == len(td) == 1:
                    pairs[th[0].text_content().strip()] = td[0].text_content().strip()
            rows.append(pairs)
        else:
            headers = [x.text_content().strip() for x in table.xpath('.//tr[th][1]/th')]
            if not set(spec.get('required_headers', [])) <= set(headers):
                continue
            for tr in table.xpath('.//tr[td]'):
                cells = tr.xpath('./td')
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, [x.text_content().strip() for x in cells])))
    if spec['format'] == 'html_pairs':
        rows = [{k:v for row in rows for k,v in row.items()}]
    return [mapped_row({k:r.get(v) for k,v in spec['fields'].items()}, spec) for r in rows]


def references_from_rows(rows, spec, retrieved):
    refs, candidates = [], []
    from .analysis import external_row
    for i, row in enumerate(rows):
        if row.get('bill_type') in spec.get('exclude_bill_types', []):
            continue
        ref = str(row.get('reference') or '').strip()
        if not re.fullmatch(r'[A-Z0-9-]{6,40}', ref):
            continue
        # A house-bill filer need not be the ocean carrier on the master bill.
        carrier = ref[:4] if ref[:4] in spec.get('master_carrier_prefixes', []) else row.get('carrier') or ref[:4]
        evidence = {'source_url':spec['url'], 'row_index':i, 'vessel':row.get('vessel'),
                    'voyage':row.get('voyage'), 'bill_type':row.get('bill_type'),
                    'house_reference':row.get('house_reference'), 'filing_carrier':row.get('carrier'),
                    'category':'public_manifest_preview'}
        row = row | {'carrier':carrier}
        refs.append(SourceReference(id=digest([spec['id'],ref,row.get('port')])[:32],reference=ref,
                    carrier=carrier,source=spec['id'],retrieved_at=retrieved,
                    reported_at=row.get('movement_at'),port=row.get('port'),evidence=evidence))
        candidates += external_row(row,spec,retrieved,evidence)
    return refs,candidates


class NativeSources:
    def __init__(self, root: Path, *, limit=100, run_id=None,development=False):
        self.root, self.limit, self.calls, self.run_id = root, limit, 0, run_id
        self.providers = {}
        self.development=development

    def workflow_gate(self, spec, reason=None):
        path=self.root/'carrier-state'/(digest(spec)+'.json')
        if reason is not None:
            from .cooldown import next_due
            atomic(path,{'source':spec['id'],'reason':reason,
                         'retry_at':next_due(utcnow()).isoformat()})
        if path.exists():
            saved=json.loads(path.read_text())
            if date(saved['retry_at'])>utcnow():
                return saved
        return None

    async def managed(self, spec, request, scope=None):
        capability = 'browser_recipe' if spec.get('browser_fallback') else 'managed_provider'
        raise SourceBlocked('unsupported_capability:' + capability)

    async def fetch(self, client, spec, scope=None):
        request = render(spec.get('request') or {'url':spec['url']},scope or {})
        url = request['url']
        host = urlsplit(url).hostname
        if urlsplit(url).scheme != 'https' or host not in spec['allowed_hosts']:
            raise ValueError('source_url_not_allowlisted')
        if self.limit is not None and self.calls >= self.limit:
            raise SourceBlocked('native_request_limit')
        group=spec.get('quota_group','public')
        gate = self.root / 'source-cache' / str(utcnow().date()) / (host + '-' + group + '-quota.json')
        if gate.exists():
            raise SourceBlocked('source_daily_quota')
        self.calls += 1
        async with client.stream(request.get('method','GET'), url, params=request.get('query'),
                                 json=request.get('json'),data=request.get('form'),headers=request.get('headers')) as response:
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body)>4_000_000:
                    raise ValueError('source_size_limit')
            if re.search(rb'daily free search limit exceeded|quota exceeded|rate limit exceeded',body,re.I) or response.status_code==429:
                atomic(gate,{'status':'quota_blocked','observed_at':utcnow().isoformat()})
                raise SourceBlocked('source_daily_quota')
            if response.status_code == 403 and spec.get('verified_bot_403'):
                return await self.managed(spec, request, scope)
            if response.status_code in (401,402,403):
                raise SourceBlocked('source_access_' + str(response.status_code))
            response.raise_for_status()
            if challenge(body):
                return await self.managed(spec, request, scope)
            return bytes(body)

    async def previews(self, config, day, ports, *, priorities=None):
        refs, candidates, outcomes = [], [], []
        async with httpx.AsyncClient(timeout=25,follow_redirects=False,headers={'User-Agent':'PortTrack sourcing/2.0'}) as client:
            for template in config.get('reference_sources',[]):
                if not template.get('enabled'):
                    continue
                choices = [None]
                if template.get('discover'):
                    discovery=template['discover']
                    try:
                        content=await self.fetch(client,template|{'request':{'url':discovery['url']}})
                        doc=html.fromstring(content)
                        links=list(dict.fromkeys(urljoin(discovery['url'],link) for link in doc.xpath(discovery['links'])))
                        links=[link for link in links if urlsplit(link).hostname in template['allowed_hosts'] and
                               re.fullmatch(discovery['path_pattern'],urlsplit(link).path) and not urlsplit(link).query]
                        offset=date(day).toordinal()%max(1,len(links))
                        choices=[{'url':url} for url in (links[offset:]+links[:offset])[:discovery.get('max_links',10)]]
                    except Exception as exc:
                        outcomes.append({'source':template['id'],'status':'access_failed','reason':type(exc).__name__})
                        continue
                if template.get('port_queries'):
                    choices = [p for p in template['port_queries'] if p['port'] in ports]
                    # Rotate scarce public searches, preserving priority for uncovered ports.
                    if choices:
                        offset = date(day).toordinal() % len(choices)
                        choices = choices[offset:]+choices[:offset]
                        choices.sort(key=lambda p:(priorities or {}).get(p['port'],2))
                        choices = choices[:template.get('max_queries',3)]
                for port in choices:
                    spec = dict(template)
                    if port:
                        spec['port']=port.get('port')
                        if port.get('url'):
                            spec['url']=port['url'];spec['request']={'url':port['url']}
                    path=self.root/'source-cache'/day/(template['id']+'-'+digest(port or spec['url'])[:16]+'.json')
                    try:
                        scope={'day':day,'start':str(date(day).date()-timedelta(days=7)), 'port':port or {}}
                        spec['url']=render(spec.get('request') or {'url':spec['url']},scope)['url']
                        if path.exists():
                            cached=json.loads(path.read_text())
                        else:
                            content=await self.fetch(client,spec,scope)
                            cached={'rows':reference_rows(content,spec),'retrieved_at':utcnow().isoformat(),'hash':digest(content.hex())}
                            atomic(path,cached)
                        rows=[r | {'port':r.get('port') or spec.get('port')} for r in cached['rows']]
                        rr,cc=references_from_rows(rows,spec,cached['retrieved_at'])
                        refs+=rr;candidates+=cc
                        outcomes.append({'source':spec['id'],'url':spec['url'],'port':spec.get('port'),'status':'references_observed' if rr else 'no_visible_references',
                                         'references':len(rr),'ids':len(cc),'provider':'native'})
                    except Exception as exc:
                        outcomes.append({'source':spec['id'],'status':'capability_blocked' if isinstance(exc,SourceBlocked) and str(exc).startswith('unsupported_capability:') else 'quota_blocked' if isinstance(exc,SourceBlocked) and 'quota' in str(exc) else 'access_failed',
                                         'reason':str(exc) if isinstance(exc,SourceBlocked) else type(exc).__name__})
                        if isinstance(exc,SourceBlocked) and 'quota' in str(exc):
                            break
        return refs,candidates,outcomes

    async def expand(self, reference: SourceReference, spec):
        self.providers.pop(spec['id'],None)
        ref=reference.reference
        for prefix in spec.get('strip_prefixes',[]):
            if ref.startswith(prefix):
                ref=ref[len(prefix):]
                break
        scope={'reference':ref, 'reference_kind':reference.reference_kind,
               'lookup_kind':spec.get('lookup_kinds',{}).get(reference.reference_kind,reference.reference_kind)}
        day=str(utcnow().astimezone(ZoneInfo('Asia/Kolkata')).date())
        cache=self.root/'reference-cache'/(digest([spec,reference.reference_kind,ref,day])+'.json')
        if cache.exists():
            saved=json.loads(cache.read_text())
            age=(utcnow()-date(saved['retrieved_at'])).total_seconds()
            if age < spec.get('cache_seconds',86400):
                self.providers[spec['id']]='cache'
                return [normalize_visit_roles(Candidate.model_validate(c)) for c in saved['candidates']],saved['status']
        out=[];partial=False;observed=0
        async with httpx.AsyncClient(timeout=25,follow_redirects=False) as client:
            for prepare in spec.get('prepare', []):
                prepared = await self.fetch(client, dict(prepare, id=spec['id'], allowed_hosts=spec['allowed_hosts']), scope)
                if prepare.get('captures') or prepare.get('not_found'):
                    prepared_body = response_body(prepared, prepare)
                    if prepare.get('success_path') or prepare.get('success_xpath'):
                        if not valid_envelope(prepared_body,prepare):
                            raise SourceBlocked('carrier_session_envelope')
                    if not_found_envelope(prepared_body,prepare):
                        atomic(cache,{'retrieved_at':utcnow().isoformat(),'status':'not_found','candidates':[]})
                        return [],'not_found'
                    verify_reference(prepared_body,prepare,ref,spec.get('reference_echo_prefixes',[]))
                    for name, path in prepare['captures'].items():
                        descriptor=path if isinstance(path,dict) else {'path':path}
                        values = response_values(prepared_body, descriptor['path'], prepare)
                        if not values:
                            raise SourceBlocked('carrier_session_value_missing')
                        value=values[0]
                        if descriptor.get('json'):
                            value=pick(json.loads(value),descriptor['json'])
                        if value is None:
                            raise SourceBlocked('carrier_session_value_missing')
                        scope[name] = value
            for page in range(1,spec.get('max_pages',3)+1):
                content = await self.fetch(client,spec,scope | {'page':page})
                if spec.get('not_found_pattern') and re.search(spec['not_found_pattern'],content.decode('utf-8'),re.I):
                    break
                body=response_body(content,spec)
                if not valid_envelope(body,spec):
                    raise SourceBlocked('carrier_invalid_envelope')
                if not_found_envelope(body,spec):
                    break
                verify_reference(body,spec,ref,spec.get('reference_echo_prefixes',[]))
                rows=[]
                groups=response_values(body,spec['groups'],spec) if spec.get('groups') else [body]
                for group in groups:
                    group_fields=response_fields(group,{'fields':spec.get('group_fields',{}),'format':spec.get('format')},scope)
                    verify_reference(group,spec.get('group_check',{}),ref,spec.get('reference_echo_prefixes',[]))
                    rows.extend((row,group_fields) for row in response_values(group,spec['rows'],spec))
                observed += len(rows)
                for row,group_fields in rows:
                    row_scope=scope | {'group':group_fields}
                    fields=response_fields(row,spec,row_scope)
                    key=iso6346.normalize(fields.get('container_number'))
                    if not iso6346.is_valid(key):
                        continue
                    event_spec=spec.get('events')
                    events=[]
                    if event_spec:
                        event_body=row if event_spec.get('inline') else response_body(await self.fetch(client,dict(event_spec,id=spec['id'],allowed_hosts=spec['allowed_hosts']),row_scope | {'row':row,'fields':fields}),event_spec)
                        if not event_spec.get('inline') and not valid_envelope(event_body,event_spec):
                            raise SourceBlocked('carrier_event_envelope')
                        for raw in response_values(event_body,event_spec['rows'],event_spec):
                            e=response_fields(raw,event_spec,row_scope | {'fields':fields})
                            e['eventCode']=event_spec.get('event_codes',{}).get(e.get('eventCode'),e.get('eventCode'))
                            if event_spec.get('include_codes') and e['eventCode'] not in event_spec['include_codes']:
                                continue
                            e['eventQualifier']=event_spec.get('qualifiers',{}).get(e.get('eventQualifier'),e.get('eventQualifier'))
                            if event_spec.get('default_qualifier') and e['eventQualifier'] not in ('A','E','ACT'):
                                e['eventQualifier']=event_spec['default_qualifier']
                            if event_spec.get('date_format') and e.get('eventTime'):
                                from datetime import datetime
                                # Date-only sources remain timezone-unknown.
                                e['eventTime']=datetime.strptime(e['eventTime'],event_spec['date_format']).isoformat()
                            port=e.pop('port',None)
                            port=event_spec.get('port_names',{}).get(port,port)
                            loc={'unlocode':port,'terminal':e.pop('terminal',None)}
                            e.update(location=loc,role='portOfDischarge' if loc['unlocode']==fields.get('pod') else 'portOfLoading' if loc['unlocode']==fields.get('pol') else 'other',scope='container')
                            if e['eventCode']=='EMPTY_RETURN':
                                e['role']='emptyReturn'
                            events.append(e)
                    ports=[{'unlocode':fields.get(k),'role':role} for k,role in [('pod','portOfDischarge'),('pol','portOfLoading')] if fields.get(k)]
                    ports += [e['location'] | {'role':e['role']} for e in events if e['location'].get('terminal')]
                    trip=digest([spec['id'],fields.get('booking') or ref,key])[:24]
                    c=Candidate(id=digest([spec['id'],trip])[:32],container_number=key,reference=reference.reference,
                          reference_kind=reference.reference_kind,source=spec['id'],trip_id=trip,retrieved_at=utcnow().isoformat(),fetched_at=utcnow().isoformat(),
                          ports=ports,events=events,evidence={'reference_id':reference.id,'carrier_lookup_verified':True,'category':'carrier_tracking_event',
                          'carrier':reference.carrier,'booking':fields.get('booking'),'vessel':fields.get('vessel'),'voyage':fields.get('voyage'),'payload_hash':digest(html.tostring(row).decode() if hasattr(row,'xpath') else row),'events_hash':digest(events)})
                    out.append(normalize_visit_roles(c))
                total=observed if spec.get('format')=='html' else int(pick(body,spec.get('total_path','$.total'),observed))
                if observed>=total or not rows:
                    break
                if page==spec.get('max_pages',3):
                    partial=True
        keys=[c.container_number for c in out]
        if len(keys)!=len(set(keys)):
            raise SourceBlocked('carrier_duplicate_container_rows')
        status='partial' if partial else 'expanded' if out else 'not_found'
        atomic(cache,{'retrieved_at':utcnow().isoformat(),'status':status,'candidates':[c.model_dump() for c in out]})
        return out,status


def normalize_visit_roles(c):
    """Keep the marine discharge distinct from inland delivery and empty return."""
    if not c.evidence.get('carrier_lookup_verified'):
        return c
    events=[dict(e,role='emptyReturn') if e.get('eventCode')=='EMPTY_RETURN' else e for e in c.events]
    ports=[dict(p) for p in c.ports if not p.get('terminal')]
    discharges=[e for e in events if e.get('eventCode')=='DISC' and e.get('scope')=='container'
                and e.get('eventQualifier') in ('A','ACT','E') and date(e.get('eventTime'))
                and (e.get('location') or {}).get('unlocode') and (e.get('location') or {}).get('terminal')]
    if discharges:
        def event_time(e):
            value=date(e['eventTime'])
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        last=max(discharges,key=event_time)
        discharge_port=last['location']['unlocode']
        # Some carrier envelopes label the final rail destination as POD. The
        # last vessel discharge supplies the marine visit; future dates retain
        # their original qualifier and still rank as upcoming.
        delivery_ports={p['unlocode'] for p in ports if p.get('role')=='portOfDischarge'
                        and p.get('unlocode') and p['unlocode']!=discharge_port}
        ports=[dict(p,role='placeOfDelivery') if p.get('role')=='portOfDischarge'
               and p.get('unlocode') in delivery_ports else p for p in ports]
        if not any(p.get('role')=='portOfDischarge' and p.get('unlocode')==discharge_port for p in ports):
            ports.append({'unlocode':discharge_port,'role':'portOfDischarge'})
        events=[dict(e,role='portOfDischarge') if (e.get('location') or {}).get('unlocode')==discharge_port
                and e.get('eventCode')!='EMPTY_RETURN' else
                dict(e,role='placeOfDelivery') if e.get('role')=='portOfDischarge'
                and (e.get('location') or {}).get('unlocode') in delivery_ports else e for e in events]
    if events==c.events and ports==[p for p in c.ports if not p.get('terminal')]:
        return c
    ports += [e['location'] | {'role':e.get('role')} for e in events if (e.get('location') or {}).get('terminal')]
    return c.model_copy(update={'events':events,'ports':ports,'evidence':c.evidence | {'events_hash':digest(events)}})


def candidate_references(candidates):
    refs=[]
    for c in candidates:
        if not c.reference:
            continue
        carrier=c.evidence.get('carrier') or c.evidence.get('carrier_scac') or c.reference[:4]
        refs.append(SourceReference(id=digest([carrier,c.reference])[:32],reference=c.reference,carrier=carrier,
                    reference_kind=c.reference_kind or 'bill_of_lading',source=c.source,retrieved_at=c.retrieved_at,
                    port=next((p.get('unlocode') for p in c.ports if p.get('role')=='portOfDischarge'),None),
                    container_hint=c.container_number,evidence={'candidate_id':c.id}))
    return refs


def raw_ocean_rows(rows,retrieved):
    out=[]
    for row in rows:
        detail=decoded(row.get('detail')) or {}
        statuses=decoded(row.get('statuses')) or []
        if not isinstance(detail,dict) or not detail:
            continue
        key=iso6346.normalize(row.get('container_number'))
        if not iso6346.is_valid(key):
            continue
        events=[]
        def walk(nodes,parent=None):
            for e in nodes if isinstance(nodes,list) else []:
                if not isinstance(e,dict):
                    continue
                scope=e.get('eventScopeId') or (parent or {}).get('eventScopeId')
                if scope and iso6346.normalize(str(scope))!=key:
                    continue
                if e.get('cancelled'):
                    continue
                if not scope:
                    walk(e.get('children'),e)
                    continue
                loc=e.get('location') or (parent or {}).get('location') or {}
                code=str(e.get('masterEventCode') or e.get('eventCode') or '')
                code={'GOU':'GTOT','GIN':'GTIN','ARR':'ARRI','DEP':'DEPA','DIS':'DISC','UNL':'DISC','DLV':'DELIVERY'}.get(code.upper(),code)
                estimated_child=any(isinstance(child,dict) and str(child.get('eventCode','')).upper() in ('ETA','ETD') for child in e.get('children',[]) or [])
                qualifier='E' if estimated_child or code.upper() in ('ETA','ETD') or 'estimated' in str(e.get('description','')).lower() else e.get('eventQualifier','A')
                events.append({'eventCode':code,'eventQualifier':qualifier,'eventTime':e.get('eventDate'),
                               'location':{'unlocode':loc.get('code'),'terminal':loc.get('terminal'),'name':loc.get('place')},'scope':'container'})
                walk(e.get('children'),e)
        walk(statuses)
        def port_code(value):
            value=str(value or '').strip().upper()
            match=re.search(r'\(([A-Z]{2}[A-Z0-9]{3})\)\s*$',value)
            return match.group(1) if match else value if re.fullmatch(r'[A-Z]{2}[A-Z0-9]{3}',value) else None
        ports=[{'unlocode':port_code(detail.get(k)),'role':role} for k,role in [('mainCarriageOrigin','portOfLoading'),('mainCarriageDestination','portOfDischarge')]]
        for e in events:
            e['role']=next((p['role'] for p in ports if p['unlocode'] and p['unlocode']==e['location'].get('unlocode')),'other')
        ref=detail.get('mainCarriageBookingRef') or detail.get('houseBillNumber')
        trip=digest([key,row['trip_id'],ref])[:24]
        out.append(Candidate(id=digest(['trackone-raw',trip])[:32],container_number=key,reference=ref,reference_kind='booking',source='trackone-raw',trip_id=trip,
                 retrieved_at=retrieved,fetched_at=row.get('crawled_at'),ports=ports,events=events,
                 evidence={'carrier_scac':detail.get('mainCarriageScac'),'trip_id':row['trip_id'],'data_id':row.get('data_id'),'payload_hash':digest([detail,statuses])}))
    return out
