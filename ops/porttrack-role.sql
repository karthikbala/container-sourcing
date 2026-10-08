-- Run once as the PortTrack database owner with psql variable sourcing_password.
-- Supply the password securely; never commit it or put it in a command argument.
-- Existing role names deliberately fail: do not reset another account's credential.
\set ON_ERROR_STOP on
BEGIN;
CREATE ROLE container_sourcing LOGIN PASSWORD :'sourcing_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
GRANT CONNECT ON DATABASE :"DBNAME" TO container_sourcing;
GRANT USAGE ON SCHEMA public TO container_sourcing;
GRANT SELECT (id,code) ON public.terminals TO container_sourcing;
GRANT SELECT (id,terminal_id,kind,key) ON public.tracked_items TO container_sourcing;
GRANT INSERT (terminal_id,kind,key,aux_info,created_by) ON public.tracked_items TO container_sourcing;
GRANT USAGE ON SEQUENCE public.tracked_items_id_seq TO container_sourcing;
ALTER ROLE container_sourcing SET search_path = public;
COMMIT;
