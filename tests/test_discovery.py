import json
from datetime import datetime, timezone

import httpx
import pytest

from container_sourcing.analysis import proposals
from container_sourcing.discovery import NativeSources, SourceBlocked, reference_rows, references_from_rows, raw_ocean_rows
from container_sourcing.models import SourceReference, Candidate
from container_sourcing.sources import catalogue

NOW=datetime(2026,10,1,12,tzinfo=timezone.utc)


def test_house_filer_does_not_override_master_carrier_and_uppercase_port():
    spec=catalogue()['reference_sources'][0]
    body=b'<table><tr><th>Master BOL #</th><td>OOLU0000000001</td></tr><tr><th>Carrier Code</th><td>QACR</td></tr><tr><th>Port of Unlading</th><td>LONG BEACH, CALIFORNIA (2709)</td></tr><tr><th>Container Number</th><td>ABCU0000017</td></tr><tr><th>Actual Arrival Date</th><td>2026-09-26</td></tr></table>'
    refs,cs=references_from_rows(reference_rows(body,spec),spec,NOW.isoformat())
    assert refs[0].carrier=='OOLU' and refs[0].evidence['filing_carrier']=='QACR'
    assert refs[0].port=='USLGB' and cs[0].container_number=='ABCU0000017'
    assert references_from_rows([{'reference':'ONEY123456','bill_type':'FROB'}],{'id':'x','url':'https://example.com','exclude_bill_types':['FROB']},NOW.isoformat())==([],[])


@pytest.mark.asyncio
async def test_http_200_quota_is_not_empty_result_and_blocks_repeat(tmp_path):
    source=NativeSources(tmp_path)
    spec={'id':'preview','url':'https://example.com/search','allowed_hosts':['example.com']}
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,text='Daily free search limit exceeded'))) as client:
        with pytest.raises(SourceBlocked,match='quota'):
            await source.fetch(client,spec)
        with pytest.raises(SourceBlocked,match='quota'):
            await source.fetch(client,spec)
    assert source.calls==1


@pytest.mark.asyncio
async def test_auth_does_not_use_paid_fallback(tmp_path,monkeypatch):
    source=NativeSources(tmp_path,run_id=1)
    async def unexpected(*args):raise AssertionError('paid call on authentication gate')
    monkeypatch.setattr(source,'managed',unexpected)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(401))) as client:
        with pytest.raises(SourceBlocked,match='access_401'):
            await source.fetch(client,{'id':'x','url':'https://example.com','allowed_hosts':['example.com'],'managed_fallback':'scrapedo'})


@pytest.mark.asyncio
async def test_reference_expands_multiple_containers_and_preserves_event_qualifiers(tmp_path,monkeypatch):
    from datetime import timedelta
    from container_sourcing import discovery
    clock=[NOW]
    monkeypatch.setattr(discovery,'utcnow',lambda:clock[0])
    spec=catalogue()['carrier_workflows'][0]
    source=NativeSources(tmp_path)
    calls=[]
    async def fetch(client,request,scope):
        calls.append(scope)
        if 'row' not in scope:
            return json.dumps({'code':1,'total':2,'data':[{'containerNo':k,'bookingNo':'B1','pod':{'code':'USTIW'}} for k in ['ABCU0000038','ABCU0000022']]}).encode()
        return json.dumps({'code':1,'data':[{'matrixId':'E090','triggerType':'ESTIMATED','eventDate':'2026-10-19T12:00:00Z','location':{'code':'USTIW'},'yard':{'yardName':'WUT (WASHINGTON UNITED TERMINALS)'}}]}).encode()
    monkeypatch.setattr(source,'fetch',fetch)
    ref=SourceReference(id='r',reference='ONEYABC123456',carrier='ONEY',source='preview',retrieved_at=NOW.isoformat())
    cs,state=await source.expand(ref,spec)
    assert state=='expanded' and len(cs)==2 and len(calls)==3
    assert calls[0]['reference']=='ABC123456'
    assert all(c.events[0]['eventQualifier']=='E' for c in cs)
    ranked=proposals(cs,[t for t in catalogue()['targets'] if t['code'] in ['huskyterminal','washingtonunitedterminals']],NOW)
    assert len(ranked)==2 and all(p.terminal_code=='washingtonunitedterminals' and p.disposition=='upcoming' for p in ranked)
    await source.expand(ref,spec)
    assert len(calls)==3
    # Tomorrow's daily job gets fresh evidence even before a 24-hour TTL expires.
    clock[0]=NOW+timedelta(hours=7)
    await source.expand(ref,spec)
    assert len(calls)==6


def test_empty_return_depot_is_not_the_import_terminal():
    from container_sourcing.discovery import normalize_visit_roles
    wut={'unlocode':'USTIW','terminal':'WUT (WASHINGTON UNITED TERMINALS)'}
    husky={'unlocode':'USTIW','terminal':'HUSKY TERMINAL'}
    events=[{'eventCode':'DISC','eventQualifier':'A','eventTime':'2026-10-01T06:00:00Z','scope':'container','role':'portOfDischarge','location':wut},
            {'eventCode':'EMPTY_RETURN','eventQualifier':'E','eventTime':'2026-10-05T06:00:00Z','scope':'container','role':'portOfDischarge','location':husky}]
    c=Candidate(id='x',container_number='ABCU0000038',source='carrier',trip_id='v',reference='B',retrieved_at=NOW.isoformat(),fetched_at=NOW.isoformat(),events=events,ports=[wut|{'role':'portOfDischarge'},husky|{'role':'portOfDischarge'}],evidence={'carrier_lookup_verified':True})
    fixed=normalize_visit_roles(c)
    ranked=proposals([fixed],[t for t in catalogue()['targets'] if t['code'] in ('washingtonunitedterminals','huskyterminal')],NOW)
    assert len(ranked)==1 and ranked[0].terminal_code=='washingtonunitedterminals'
    assert ranked[0].eligible and fixed.fetched_at==c.fetched_at


@pytest.mark.parametrize('qualifier,discharged,eligible', [
    ('A','2026-09-30T16:38:00Z',True),
    ('E','2026-10-05T16:38:00Z',False),
])
def test_inland_delivery_does_not_hide_last_marine_discharge(qualifier,discharged,eligible):
    from container_sourcing.discovery import normalize_visit_roles
    marine={'unlocode':'USLAX','terminal':'Marine Yard'}
    inland={'unlocode':'USDAL','terminal':'Rail Yard'}
    transfer={'unlocode':'VNCMP','terminal':'Transfer Yard'}
    events=[{'eventCode':'DISC','eventQualifier':'A','eventTime':'2026-09-05T06:00:00Z','scope':'container','role':'other','location':transfer},
            {'eventCode':'DISC','eventQualifier':qualifier,'eventTime':discharged,'scope':'container','role':'other','location':marine},
            {'eventCode':'E117','eventQualifier':'E','eventTime':'2026-10-07T06:00:00Z','scope':'container','role':'portOfDischarge','location':inland},
            {'eventCode':'EMPTY_RETURN','eventQualifier':'E','eventTime':'2026-10-09T06:00:00Z','scope':'container','role':'portOfDischarge','location':inland}]
    c=Candidate(id='rail',container_number='ABCU0000043',source='carrier',trip_id='visit',retrieved_at=NOW.isoformat(),fetched_at=NOW.isoformat(),
                events=events,ports=[{'unlocode':'USDAL','role':'portOfDischarge'}]+[e['location']|{'role':e['role']} for e in events],
                evidence={'carrier_lookup_verified':True})
    fixed=normalize_visit_roles(c)
    assert fixed.events[0]['role']=='other'
    assert fixed.events[1]['role']=='portOfDischarge'
    assert fixed.events[2]['role']=='placeOfDelivery'
    assert fixed.events[3]['role']=='emptyReturn'
    assert normalize_visit_roles(fixed)==fixed and fixed.fetched_at==c.fetched_at
    ranked=proposals([fixed],[{'code':'marine','ports':['USLAX'],'facilities':['Marine Yard']}],NOW)
    assert len(ranked)==1 and ranked[0].match_basis=='facility' and ranked[0].eligible is eligible


def test_exact_carrier_departure_suppresses_manifest_at_other_port_terminal():
    from container_sourcing.analysis import external_row
    manifest=external_row({'container_number':'ABCU0000038','reference':'B1','port':'USLAX','movement_at':'2026-09-22'}, {'id':'preview'},NOW.isoformat(),{})
    carrier=external_row({'container_number':'ABCU0000038','reference':'B1','port':'USLAX','terminal':'TraPac Los Angeles','movement_at':'2026-09-22','events':[
        {'eventCode':'DISC','eventQualifier':'A','eventTime':'2026-09-22','location':{'unlocode':'USLAX','terminal':'TraPac Los Angeles'},'role':'portOfDischarge'},
        {'eventCode':'GTOT','eventQualifier':'A','eventTime':'2026-09-28','location':{'unlocode':'USLAX','terminal':'TraPac Los Angeles'},'role':'portOfDischarge'}]}, {'id':'carrier','category':'carrier_tracking_event'},NOW.isoformat(),{})
    carrier[0].evidence['carrier_lookup_verified']=True
    targets=[{'code':'apm','ports':['USLAX'],'facilities':['APM Pier 400']},{'code':'trapac','ports':['USLAX'],'facilities':['TraPac Los Angeles']}]
    ranked=proposals(manifest+carrier,targets,NOW)
    assert all(not p.eligible for p in ranked)






def test_raw_eta_children_are_not_promoted_to_actual_and_scope_is_bound():
    row={'trip_id':1,'container_number':'ABCU0000038','crawled_at':NOW.isoformat(),'detail':{'mainCarriageDestination':'USTIW','mainCarriageScac':'MEDU','mainCarriageBookingRef':'ABC123456'},'statuses':[{'eventScopeId':'ABCU0000038','eventCode':'DISC','eventDate':'2026-10-19','location':{'code':'USTIW'},'children':[{'eventCode':'ETA','eventDate':'2026-10-19'}]},{'eventScopeId':'ABCU0000022','eventCode':'DISC','eventDate':'2026-09-29'}]}
    cs=raw_ocean_rows([row],NOW.isoformat())
    assert len(cs[0].events)==2 and all(e['eventQualifier']=='E' for e in cs[0].events)
    assert proposals(cs,[{'code':'wut','ports':['USTIW'],'facilities':[]}],NOW)[0].disposition=='upcoming'






def test_reviewed_vessel_schedule_is_only_a_ranked_hypothesis():
    from container_sourcing.analysis import corroborate_calls
    c=Candidate(id='v',container_number='ABCU0000038',source='manifest',trip_id='v',retrieved_at=NOW.isoformat(),fetched_at=NOW.isoformat(),ports=[{'unlocode':'USOAK','role':'portOfDischarge'}],events=[{'eventTime':'2026-09-30','eventQualifier':'E','eventCode':'MANIFEST','location':{'unlocode':'USOAK'},'role':'portOfDischarge','scope':'manifest'}],evidence={'vessel':'Vessel A','voyage':'001E'})
    call={'vessel':'Vessel A','voyage':'001E','port':'USOAK','terminal':'Oak Yard','call_at':'2026-09-30','retrieved_at':NOW.isoformat(),'source_url':'https://terminal.example/schedule'}
    enriched=corroborate_calls([c],[call],NOW)
    p=proposals(enriched,[{'code':'oak','ports':['USOAK'],'facilities':['Oak Yard']}],NOW)[0]
    assert p.match_basis=='vessel_call' and p.disposition=='needs_terminal_resolution' and not p.candidate.portal_verified
    assert not corroborate_calls([c],[call|{'port':'USLAX'}],NOW)[0].evidence.get('vessel_calls')
