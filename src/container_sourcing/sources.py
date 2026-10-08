"""Configured public reports, saved evidence and bounded managed access fallback."""
from __future__ import annotations

import csv
import io
import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import yaml
from jsonpath_ng.ext import parse as jsonpath
from lxml import html

from .analysis import external_row
from .collector import atomic
from .models import digest, utcnow


class UnsupportedSourceCapability(RuntimeError):
    pass


async def managed_content(spec, run_id=None):
    raise UnsupportedSourceCapability('managed_provider')


DEFAULT_CATALOG = Path(__file__).with_name('catalog.yaml')


def challenge(body):
    return bool(body and re.search(rb'cf-chl-|captcha-delivery|verify you are human|are you a robot|px-captcha', body, re.I))


def catalogue(path=None):
    return yaml.safe_load(Path(path or DEFAULT_CATALOG).read_text())


def mapped_row(raw, spec):
    result = dict(raw)
    for field, transform in spec.get('transforms', {}).items():
        value = str(result.get(field) or '')
        if transform.get('uppercase'):
            value=value.upper()
        if transform.get('regex'):
            match = re.search(transform['regex'], value)
            value = match.group(1) if match else ''
        if transform.get('map'):
            value = transform['map'].get(value.strip(), value)
        result[field] = value
    return result


def extract_rows(content: bytes, spec: dict):
    fmt = spec.get('format', 'csv')
    if fmt == 'json':
        value = json.loads(content)
        nodes = [m.value for m in jsonpath(spec.get('rows', '$[*]')).find(value)]
        out = []
        for node in nodes:
            row = {}
            for field, path in spec.get('fields', {}).items():
                values = [m.value for m in jsonpath(path).find(node)]
                row[field] = values[0] if len(values) == 1 else values
            out.append(mapped_row(row, spec) if spec.get('fields') else node)
        return out
    if fmt == 'csv':
        rows = list(csv.DictReader(io.StringIO(content.decode('utf-8-sig'))))
        return [mapped_row({k: r.get(v) for k, v in spec.get('fields', {}).items()} if spec.get('fields') else r, spec) for r in rows]
    if fmt == 'html':
        doc = html.fromstring(content)
        out = []
        for element in doc.xpath(spec['rows']):
            row = {}
            for field, path in spec['fields'].items():
                parts = element.xpath(path)
                row[field] = ' '.join(p.text_content() if hasattr(p, 'text_content') else str(p) for p in parts).strip()
            out.append(mapped_row(row, spec))
        return out
    if fmt == 'pdf':
        from pypdf import PdfReader
        pages = [(i + 1, p.extract_text() or '') for i, p in enumerate(PdfReader(io.BytesIO(content)).pages)]
        if not any(t.strip() for _, t in pages):
            raise ValueError('pdf_has_no_extractable_text')
    elif fmt == 'text':
        pages = [(1, content.decode('utf-8'))]
    else:
        raise ValueError('unsupported_source_format')
    if not spec.get('row_pattern'):
        raise ValueError('report_row_pattern_required')
    return [mapped_row(m.groupdict() | {'page': page}, spec) for page, body in pages
            for m in re.finditer(spec['row_pattern'], body, re.M)]


def ingest_file(path: Path, spec: dict, retrieved_at=None):
    content = path.read_bytes()
    retrieved = retrieved_at or utcnow().isoformat()
    candidates = []
    rows = extract_rows(content, spec)
    for i, raw in enumerate(rows):
        if not isinstance(raw, dict):
            continue
        candidates.extend(external_row(raw | {'row_index': i}, spec, retrieved,
                                       {'file_hash': digest(content.hex()), 'row_index': i, 'page': raw.get('page')}))
    return candidates


async def public_sources(config: dict, output: Path, *, day: str, run_id=None):
    candidates, outcomes = [], []
    for spec in config.get('sources', [])[:20]:
        if not spec.get('enabled') or spec.get('context_only'):
            outcomes.append({'source': spec['id'], 'status': 'context_only' if spec.get('context_only') else 'disabled',
                             'reason': spec.get('reason'), 'ids': 0})
            continue
        config_hash = digest(spec)
        path = output / 'source-cache' / day / (spec['id'] + '-' + config_hash + '.bin')
        meta_path = path.with_suffix('.json')
        try:
            host = urlsplit(spec['url']).hostname
            if urlsplit(spec['url']).scheme != 'https' or host not in spec.get('allowed_hosts', []):
                raise ValueError('source_url_not_allowlisted')
            if not path.exists():
                body, provider = None, 'native'
                native_status = None
                try:
                    async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
                        async with client.stream('GET', spec['url'], headers={'User-Agent': 'PortTrack sourcing/1.0', 'Accept': '*/*'}) as response:
                            native_status = response.status_code
                            if native_status == 200:
                                content = bytearray()
                                async for part in response.aiter_bytes():
                                    content.extend(part)
                                    if len(content) > 4_000_000:
                                        raise ValueError('source_size_limit')
                                body = bytes(content)
                except httpx.TransportError:
                    pass
                # Do not use providers to evade quotas, explicit auth or subscription gates.
                if (body is None or challenge(body)) and native_status not in (401,402,429) and spec.get('fallback_managed', True):
                    body, provider = await managed_content(spec,run_id)
                if body is None:
                    outcomes.append({'source':spec['id'],'status':'access_failed','http_status':native_status,'ids':0})
                    continue
                if challenge(body):
                    outcomes.append({'source': spec['id'], 'status': 'access_blocked', 'ids': 0})
                    continue
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix('.tmp')
                tmp.write_bytes(body)
                tmp.replace(path)
                atomic(meta_path, {'retrieved_at': utcnow().isoformat(), 'hash': digest(body.hex()),
                                   'config_hash': config_hash, 'provider':provider})
            metadata = json.loads(meta_path.read_text())
            if metadata.get('config_hash') != config_hash:
                raise ValueError('source_cache_configuration_mismatch')
            if metadata.get('hash') != digest(path.read_bytes().hex()):
                raise ValueError('source_cache_content_mismatch')
            found = ingest_file(path, spec, metadata['retrieved_at'])
            candidates.extend(found)
            outcomes.append({'source': spec['id'], 'status': 'id_rows_observed' if found else 'page_opened_no_rows',
                             'ids': len(found), 'retrieved_at': metadata['retrieved_at'], 'hash': metadata['hash'], 'provider':metadata.get('provider','native')})
        except UnsupportedSourceCapability as e:
            outcomes.append({'source': spec['id'], 'status': 'capability_blocked', 'capability': str(e), 'ids': 0})
        except Exception as e:
            outcomes.append({'source': spec['id'], 'status': 'parse_failed' if isinstance(e, (ValueError, KeyError)) else 'access_failed',
                             'error_type': type(e).__name__, 'ids': 0})
    return candidates, outcomes
