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
    'AND gid LIKE ''pgfdw_%_%_%_' || current_setting('cluster_name') || '''';
  RETURN QUERY SELECT * FROM
    dblink(server, sql) AS t1
    (transaction xid, gid text, prepared timestamp with time zone,
      owner name, database name);
END;
$$ LANGUAGE plpgsql;

-- Are the foreign transactions that the local transaction with
-- the given XID or the backend with the given PID started on
-- the remote server in progress?
CREATE OR REPLACE FUNCTION
  pg_foreign_xact_is_running(txid xid, procid integer)
  RETURNS boolean AS $$
BEGIN
  PERFORM *
    FROM pg_stat_activity
    WHERE backend_xid = txid OR
      (backend_xid IS NULL AND backend_xmin IS NULL AND
        pid = procid AND state = 'active');
  IF FOUND THEN RETURN true; ELSE RETURN false; END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
  pg_resolve_foreign_prepared_xacts (server name)
  RETURNS SETOF resolve_foreign_prepared_xacts AS $$
DECLARE
  r resolve_foreign_prepared_xacts;
  sql TEXT;
  fxid xid8;
  pid integer;
BEGIN
  FOR r IN SELECT NULL AS status, server, *
    FROM pg_foreign_prepared_xacts(server) LOOP
    sql := NULL;
    BEGIN
      fxid := split_part(r.gid, '_', 2)::xid8;
      pid := split_part(r.gid, '_', 4)::integer;

      -- While the backend is committing or rollbacking the prepared
      -- transaction on the remote server, pg_stat_activity is reporting
      -- the backend's state as "active" and its XID and XMIN as NULL.
      -- In this case this function should not commit nor rollback
      -- that foreign prepared transaction. Otherwise both the backend
      -- and this function can try to end the foreign prepared transaction
      -- at the same time, and which would cause either of them to
      -- fail with an error.
      --
      -- Note that this function can skip committing or rollbacking
      -- the foreign prepared transactions that can be completed.
      -- Because pg_foreign_xact_is_running() can return true even when
      -- the backend with the given PID is running the transaction with XID
      -- other than the given one. But this is OK because basically
      -- this can rarely happen and we can just retry soon later.
      CONTINUE WHEN pg_foreign_xact_is_running(fxid::xid, pid);

      r.status := pg_xact_status(fxid);
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
