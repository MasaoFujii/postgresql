/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE TYPE resolve_foreign_prepared_xacts AS
  (status text, server name, transaction xid, gid text,
    prepared timestamp with time zone, owner name, database name);

CREATE OR REPLACE FUNCTION
  pg_foreign_prepared_xacts (server name)
  RETURNS SETOF pg_prepared_xacts AS $$
DECLARE
  sql TEXT;
BEGIN
  PERFORM * FROM pg_foreign_server WHERE srvname = server;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'server "%" does not exist', server;
  END IF;
  sql := 'SELECT * FROM pg_prepared_xacts ' ||
    'WHERE owner = current_user AND database = current_database() ' ||
    'AND gid LIKE ''pgfdw_' || current_setting('cluster_name') || '_%_%''';
  RETURN QUERY SELECT * FROM
    dblink(server, sql) AS t1
    (transaction xid, gid text, prepared timestamp with time zone,
      owner name, database name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
  pg_resolve_foreign_prepared_xacts (server name)
  RETURNS SETOF resolve_foreign_prepared_xacts AS $$
DECLARE
  r resolve_foreign_prepared_xacts;
  sql TEXT;
BEGIN
  FOR r IN SELECT NULL AS status, server, *
    FROM pg_foreign_prepared_xacts(server) LOOP
    sql := NULL;
    BEGIN
      r.status := pg_xact_status(split_part(r.gid, '_', 3)::xid8);
      CASE r.status
        WHEN 'committed' THEN
          sql := 'COMMIT PREPARED ''' || r.gid || '''';
        WHEN 'aborted' THEN
          sql := 'ROLLBACK PREPARED ''' || r.gid || '''';
      END CASE;
    EXCEPTION WHEN OTHERS THEN
    END;
    IF sql IS NOT NULL THEN
      RETURN NEXT r;
      PERFORM dblink(server, sql);
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
  pg_resolve_foreign_prepared_xacts_all()
  RETURNS SETOF resolve_foreign_prepared_xacts AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT * FROM pg_foreign_server LOOP
    RETURN QUERY SELECT * FROM pg_resolve_foreign_prepared_xacts(r.srvname);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
