# Container sourcing

An independently installed, manually run tool for collecting container evidence,
ranking terminal matches, and inserting newly discovered IDs into an existing
PortTrack database. PortTrack does not import or install this package. This tool
does not import PortTrack, run a service, schedule work, generate recipes, or prove
terminal/API delivery.

## Install and run

Python 3.12 or newer is required. From this repository:

```sh
uv sync --group dev
uv run container-sourcing --help
uv run pytest -q
```

Run discovery and enrollment separately. Commands do only the named step:

```sh
# Bounded SELECT-only source collection using an explicitly configured MCP connection.
uv run container-sourcing collect --output artifacts/collected

# Native HTTP discovery and expansion of optional saved SourceReference JSONL.
uv run container-sourcing discover --output artifacts/discovered --max-requests 40 --max-references 10

# Import saved source evidence, retaining its actual original retrieval timestamp.
uv run container-sourcing ingest --input source.csv --mapping mapping.yaml \
  --retrieved-at 2026-10-08T06:00:00Z --output artifacts/imported

# Analyze saved evidence without database or source requests.
uv run container-sourcing analyze --input artifacts/discovered --output artifacts/report

# Preview against the existing database. Only --apply permits inserts.
uv run container-sourcing enroll --input artifacts/report --output artifacts/preview.json
uv run container-sourcing enroll --input artifacts/report --apply --output artifacts/enrolled.json
```

`--catalog` on discover/analyze/enroll supplies an alternate reviewed YAML catalog.
The bundled catalog describes public source workflows and terminal associations;
disabled sources remain disabled. Analysis accepts repeated `--input` arguments,
saved collector directories, candidate directories, or candidate JSONL files.
Analysis reports are proposals, never source or API verification.

## Database contract

Set `CONTAINER_SOURCING_DATABASE_URL` securely for enrollment. There is no default
URL and no fallback to `DATABASE_URL`. See [PortTrack access](docs/PORTTRACK-ACCESS.md)
for the dedicated role and tunnel setup. The application does not create a role,
database, schema, table, or migration, and does not automatically read `.env` files.

Enrollment recomputes eligibility from candidate evidence at the current time; it
does not trust stored report decisions. A candidate must have a valid ISO 6346
number, fresh evidence of an actual import movement, and an explicit facility or
source-terminal match. Ambiguous terminal assignments, port-only hypotheses,
historical/departed evidence and invalid IDs are skipped. An exact catalog terminal
code must already exist in PortTrack.

The tool reads existing container IDs across all terminals. An ID already present
anywhere is skipped, including stopped/customer-owned rows. There is no renewal or
override mode. Schema uniqueness remains `(terminal_id, kind, key)`; the insert
uses `ON CONFLICT DO NOTHING` to preserve an existing row during a concurrent
same-terminal insertion. Applying enrollment takes a per-ID transaction advisory
lock and rechecks global existence before inserting, so concurrent runs of this
tool cannot insert the same ID at different terminals. Other host writers do not
participate in that advisory-lock protocol; the schema itself has no global ID
uniqueness constraint.

The only write is `INSERT INTO tracked_items` with `terminal_id`, `kind='container'`,
normalized `key`, a `sourcing` provenance object in `aux_info`, and
`created_by='porttrack-sourcing'`. Existing defaults control status, priority, and
next poll. No existing row is updated or deleted; no run, job, state, event,
validation proof, source-ledger or schedule is written. PortTrack's existing worker
and tracking rules handle any inserted rows normally.

Receipts distinguish candidate records, unique discovered IDs, globally new IDs,
eligible terminal enrollments, inserts, existing rows and skipped evidence. A new
candidate is not necessarily a new ID or an eligible terminal enrollment.

## Source access and retired integrations

MCP collection accepts `SOURCING_MCP_CONFIG` (a local JSON connection file), or
`SOURCING_MCP_URL` and optional `SOURCING_MCP_TOKEN`. The inherited optional local
Codex configuration lookup uses `SOURCING_CODEX_CONFIG` and the named connection.
Connection material is never written into collection exports. Keep credentials,
local configuration, exports and receipts out of Git.

Native HTTP retains URL allowlists, response-size bounds, request/reference caps,
authentication/subscription gates, daily quota detection and local source caches.
`discover --max-requests` bounds preview/carrier requests; configured public report
fetches additionally retain their separate bound of at most 20 requests. MCP has
its own `--max-calls`/`--max-rows` limits. No retry is allowed to bypass access gates.

This first standalone release disables paid managed providers and browser-recipe
fallbacks. A source requiring them returns `capability_blocked` with an explicit
unsupported capability. It makes no paid-provider or model call and imports no
crawler engine. Existing fallback descriptions in the catalog are reference
metadata, not enabled adapters. Former automatic daily orchestration, validation
queues, source spend ledgers, sample banks and tracking callbacks were intentionally
retired rather than recreated in this repository.

## Tests and ownership

The package owns discovery, parsing, ranking, reporting, manual enrollment and its
catalog. Tests use synthetic evidence and mocked transport. Optional database
contract tests require a disposable database containing the real PortTrack schema;
they never create or migrate a production database. No live source or provider
request is part of the ordinary test suite.
