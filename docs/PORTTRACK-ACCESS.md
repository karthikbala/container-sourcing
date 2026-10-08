# PortTrack database access

The only integration is PostgreSQL. Neither repository imports or installs the other.
Set `CONTAINER_SOURCING_DATABASE_URL` in the shell for the manual command, or use
`uv run --env-file .env container-sourcing ...` with a local mode-0600 ignored file.
There is no default database URL and no automatic production connection.

A database owner can provision the dedicated `container_sourcing` role with
[ops/porttrack-role.sql](../ops/porttrack-role.sql). Pass `sourcing_password` through a
private psql input session; do not put it in shell arguments or commit it. The script
fails if the role already exists so it cannot silently reset another account.

Permissions are limited to reading terminal codes and existing container IDs,
inserting the enrollment columns, and allocating a tracked-item sequence ID.
The role has no UPDATE/DELETE permissions, no schema migration authority, and no
access to credentials, source payloads, recipes, mappings or spend ledgers. Do not
use PortTrack's database-owner credentials as the normal sourcing identity.

Use an existing secure network route or an SSH tunnel; do not expose PostgreSQL
publicly. For example, after confirming the database's address on the SSH host:

```sh
ssh -N -L 127.0.0.1:55432:DB_PRIVATE_HOST:5432 authorized-user@ssh-host
```

Configure the local URL with host `127.0.0.1`, port `55432`, the dedicated role and
PortTrack database name. Keep the tunnel open only for the manual session.

Run `enroll` without `--apply` first. The report distinguishes discovered IDs,
already known IDs, qualified new IDs and inserted rows. With `--apply`, the tool
inserts new rows only. Repeats never revive stopped rows, replace ownership or
change stored state. A globally known container ID is skipped even when found at
another terminal; this tool does not manage return visits. The database's unique
`(terminal_id,kind,key)` constraint also protects concurrent insert collisions.

Insertion is not proof of a current terminal visit or successful API delivery.
PortTrack's existing worker performs its normal deterministic crawl after insertion;
terminals without an active crawler remain unable to crawl until normal onboarding.

The repository contains no database credential. On 8 October 2026 a dedicated login
was provisioned for the owner's existing PortTrack deployment. Its connection URL
is stored only in the local, Git-ignored mode-0600 `.env` in this checkout. The login
was verified through an SSH tunnel: it could read 45 terminal rows and 135 container
rows, compiled a zero-row INSERT, and was denied UPDATE, DELETE and credential
reads. No containers were inserted and the verification tunnel was closed.

For this checkout, open the tunnel in a separate terminal with the existing SSH key:

```sh
SOURCING_SSH_HOST=root@37.27.51.7 SOURCING_SSH_KEY="$HOME/.ssh/buzzinga_deploy" \
  bash ops/open-porttrack-tunnel.sh
```

Then use `uv run --env-file .env container-sourcing enroll ...` in another terminal.
The helper resolves the current private Docker database address and binds only
`127.0.0.1:55432`. Ctrl-C closes it. A fresh clone has no credential; provision or
securely transfer the limited login rather than using the application owner.

Use manual production enrollment after the PortTrack decoupling PR is deployed so
newly sourced rows follow the intended ordinary tracking path. Until then the old
production worker still has sourcing-specific callbacks.
