import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from container_sourcing.analysis import external_row, normalized_rows, proposals
from container_sourcing.collector import Collector
from container_sourcing.sources import extract_rows

NOW = datetime(2026,10,1,12,tzinfo=timezone.utc)
TARGETS = [{'code':'oak','ports':['USOAK'],'facilities':['Oak Yard']},
           {'code':'la','ports':['USLAX'],'facilities':['LA Yard']}]


def sample(port='USOAK',time='2026-09-29T12:00:00+00:00',qual='A',code='DISC',terminal=None,extra=None):
    location={'unlocode':port,'terminal':terminal}
    shipment={'containerNumber':'ABCU0000038','stops':[{'stopType':'portOfDischarge','location':location}],
              'events':[{'eventCode':code,'eventQualifier':qual,'eventTime':time,'stopIndex':0}],
              'startedAt':'2026-09-01'}
    if extra:
        shipment.update(extra)
    return {'item_id':1,'data_id':2,'json_id':3,'source_id':1,'reference_number':'BOL-1','reference_type':'bill_of_lading',
            'data_crawled_at':'2026-10-01T10:00:00+00:00','payload_status':'SUCCESS','shipments':[shipment]}


def rank(row):
    return proposals(normalized_rows([row],NOW.isoformat()),TARGETS,NOW)


def test_exact_facility_stronger_than_port_and_wrong_port_never_assigned():
    assert rank(sample(terminal='Oak Yard'))[0].priority==1
    assert rank(sample())[0].match_basis=='port'
    assert [p.terminal_code for p in rank(sample(port='USLAX'))]==['la']


@pytest.mark.parametrize('row,disposition',[
    (sample(time='2026-11-04',qual='A'),'conflicting_evidence'),
    (sample(time='2026-08-16'),'historical_or_departed'),
    (sample(time='2026-11-04',qual='E'),'upcoming'),
    (sample(extra={'completedAt':'2026-09-30'}),'historical_or_departed'),
    (sample(code='UNKNOWN'),'needs_terminal_resolution'),
    (sample(extra={'containerNumber':'ABCU0000039'}),'invalid_container'),
])
def test_dispositions(row,disposition):
    assert rank(row)[0].disposition==disposition


def test_unknown_actual_events_cannot_be_current_samples():
    p=rank(sample(code='UNKNOWN'))[0]
    assert 'unknown_actual_event:UNKNOWN' in p.reasons
    # An actual unknown event must not be silently turned into an arrival.
    assert p.priority==3


def test_container_scoping_and_reused_trips():
    row=sample()
    second=sample(port='USLAX',extra={'containerNumber':'ABCU0000022','startedAt':'2026-09-10'})['shipments'][0]
    row['shipments'].append(second)
    row['shipments']=json.dumps(row['shipments'])
    cs=normalized_rows([row],NOW.isoformat())
    assert [c.ports[0]['unlocode'] for c in cs]==['USOAK','USLAX']
    assert cs[0].trip_id!=cs[1].trip_id
    earlier=sample(extra={'startedAt':'2025-01-01'})
    assert normalized_rows([earlier],NOW.isoformat())[0].trip_id!=cs[0].trip_id


def test_departure_and_reentry_have_visit_order():
    row=sample()
    row['shipments'][0]['events'].append({'eventCode':'GTOT','eventQualifier':'A','eventTime':'2026-09-30T12:00:00Z','stopIndex':0})
    assert rank(row)[0].disposition=='historical_or_departed'
    row['shipments'][0]['events'].append({'eventCode':'GTIN','eventQualifier':'A','eventTime':'2026-10-01T10:00:00Z','stopIndex':0})
    assert rank(row)[0].eligible


def test_transshipment_arrival_is_not_import_discharge():
    row=sample(code='ARRI')
    row['shipments'][0]['stops'][0]['stopType']='transshipmentPort'
    assert rank(row)[0].disposition=='wrong_direction'


def test_manifest_preserves_multicontainer_lists_and_does_not_claim_discharge():
    cs=external_row({'containers':[{'id':'ABCU0000038'},{'id':'ABCU0000022'}], 'reference':'BOL',
                     'port':'USOAK','arrival_date':'2026-09-29'}, {'id':'manifest'},NOW.isoformat(),{'row':1})
    assert len(cs)==2 and all(c.events[0]['scope']=='manifest' for c in cs)
    assert all(not c.portal_verified for c in cs)
    assert all(p.priority==3 for p in proposals(cs,TARGETS,NOW))


def test_html_row_binding_not_neighbouring_snippets():
    content=b'<ul><li><div>2026-09-29</div><p>ABCU0000038</p><span>USOAK</span></li><li><div>2026-09-30</div><p>ABCU0000022</p><span>USLAX</span></li></ul>'
    spec={'format':'html','rows':'//li','fields':{'container_number':'.//p/text()','movement_at':'.//div/text()','port':'.//span/text()'}}
    rows=extract_rows(content,spec)
    assert rows[0]['port']=='USOAK' and rows[1]['container_number']=='ABCU0000022'


def test_failure_payload_does_not_become_empty_success():
    row=sample(); row['payload_status']='FAILURE'
    assert normalized_rows([row],NOW.isoformat())==[]




@pytest.mark.asyncio
async def test_mcp_error_even_with_rows_empty_is_rejection(tmp_path):
    class Session:
        async def call_tool(self,*args):
            return SimpleNamespace(structuredContent={'error':'denied','rows':[]},isError=False)
    collector=Collector(Session(),tmp_path)
    with pytest.raises(RuntimeError,match='rejected'):
        await collector.query('SELECT id FROM ds_item LIMIT 1')


@pytest.mark.asyncio
async def test_page_resume_recovers_durable_orphan_and_detects_changed_config(tmp_path):
    calls=[]
    class Session:
        async def call_tool(self,name,args):
            calls.append(args['sql'])
            rows=[{'high':2}] if 'MAX' in args['sql'] else [{'id':1},{'id':2}]
            return SimpleNamespace(structuredContent={'rows':rows},isError=False)
    session=Session(); collector=Collector(session,tmp_path,page_size=2)
    rows=await collector.pages('x','ds_item','id')
    resumed=Collector(session,tmp_path,page_size=2)
    assert await resumed.pages('x','ds_item','id')==rows
    assert len(calls)==2
    with pytest.raises(ValueError,match='configuration_changed'):
        Collector(session,tmp_path,page_size=3)


def test_saved_carrier_gateout_suppresses_same_bol_manifest_but_not_reused_trip():
    manifest=external_row({'container_number':'ABCU0000038','reference':'BOL-1','port':'USOAK',
                           'movement_at':'2026-09-28'}, {'id':'preview'},NOW.isoformat(),{})
    carrier=external_row({'container_number':'ABCU0000038','reference':'BOL-1','port':'USOAK',
                          'movement_at':'2026-09-29','events':[
                              {'eventCode':'DISC','eventQualifier':'A','eventTime':'2026-09-29', 'location':{'unlocode':'USOAK'},'role':'portOfDischarge'},
                              {'eventCode':'GTOT','eventQualifier':'A','eventTime':'2026-09-30', 'location':{'unlocode':'USOAK'},'role':'portOfDischarge'}]},
                         {'id':'carrier','category':'carrier_tracking_event'},NOW.isoformat(),{})
    ranked=proposals(manifest+carrier,TARGETS,NOW)
    assert all(not p.eligible for p in ranked)
    assert 'corroborated_departure_same_reference' in ranked[0].reasons or 'corroborated_departure_same_reference' in ranked[1].reasons
    manifest[0].reference='NEW-BOL'
    assert next(p for p in proposals(manifest+carrier,TARGETS,NOW) if p.candidate.source=='preview').eligible






def test_exact_portal_observation_is_not_reassigned_to_neighbouring_terminal():
    cs=external_row({'container_number':'ABCU0000038','port':'USOAK','terminal':'Oak Yard',
                     'movement_at':'2026-09-29','event_code':'GTIN','qualifier':'A'},
                    {'id':'portal','category':'public_discharge_report'},NOW.isoformat(),{'terminal_code':'oak'})
    targets=TARGETS+[{'code':'neighbour','ports':['USOAK'],'facilities':['Other Yard']}]
    assert [p.terminal_code for p in proposals(cs,targets,NOW)]==['oak']
