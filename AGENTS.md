# Container sourcing working rules

This is a standalone, manually invoked tool. Read `README.md` and
`docs/PORTTRACK-ACCESS.md` before changing the database boundary.

- Do not import PortTrack, add a runtime dependency to PortTrack, or use a sibling
  checkout as an import path. Preserve this repository's independent installation.
- Discovery, analysis and enrollment are separate commands. Enrollment defaults to
  a dry run, recomputes current evidence eligibility, and requires `--apply` to write.
- The only database mutation is an insert of a new container into `tracked_items`.
  Preserve existing rows, ownership and defaults. Do not update/delete records,
  generate proof, enqueue jobs, create schema, or automatically renew visits.
- Skip globally known container IDs. Keep the per-ID transaction advisory lock,
  existence recheck and tuple-level conflict guard for concurrent manual writers.
- Paid managed providers and browser-recipe fallbacks are unsupported in this
  release. Preserve explicit blocked outcomes; do not silently enable paid access,
  bypass source gates, or copy a crawler engine to satisfy a source challenge.
- Use synthetic evidence, saved sanitized fixtures and mocked transports first.
  Integration tests require a disposable local database and an explicit test URL.
  Never run live discovery or production enrollment merely to validate a refactor.
- Keep credentials, `.env` files, source exports, customer records and run artifacts
  out of Git. Runtime secrets come from explicit environment/configuration.
- Keep changes and checks bounded; report selected tests, actual results and gaps.
