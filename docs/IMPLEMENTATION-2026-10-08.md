# Implementation and validation — 8 October 2026

Extracted from private PortTrack revision `bb26168`; the new repository is a clean
source snapshot with no operational exports, credentials or imported Git history.
PortTrack does not install this package and this package does not import PortTrack.

Retained manual MCP/file/direct-HTTP collection, normalization, terminal ranking and
provenance. Added insert-only, dry-run-first enrollment of globally new IDs through
PostgreSQL. Per-key transaction locks serialize concurrent sourcing importers.
Retired automatic cycles, source validation jobs, bootstrap banks, worker callbacks
and PortTrack-engine/paid fallbacks. Unsupported access remains an explicit outcome.

Review fixes preserve partial discovery receipts through analysis, bind public-source
caches to reviewed configuration/content, check allowlists even on cache hits, and
include MCP schema requests in the call/time ceiling.

Validation: 46 final offline tests passed. Five additional contract tests passed
against a disposable database migrated using the actual PortTrack schema: readonly
preview; normal defaults; row/state/ownership preservation; global/repeated/concurrent
ID deduplication; invalid/ambiguous rejection; and restricted database permissions.
That DB evidence is reused after CLI/cache/collector-only fixes; enrollment code and
schema did not change. The temporary database was dropped. Wheel/sdist build and
CLI help passed. Default GitHub CI runs offline tests and skips the five explicitly
optional real-schema tests; it never contacts production or source providers.

A production limited login was provisioned and verified as documented in the access
guide. Zero production containers were inserted. No discovery, crawler, paid-provider,
model or activation trial was run. Insertion and source/API delivery remain distinct.
