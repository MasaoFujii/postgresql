/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE SCHEMA pgfdw_plus;
GRANT USAGE ON SCHEMA pgfdw_plus TO public;

CREATE TABLE pgfdw_plus.xact_commits (
  fxid xid8 primary key,
  pid integer,
  umids oid[]
);
GRANT SELECT ON pgfdw_plus.xact_commits TO public;

CREATE TYPE resolve_foreign_prepared_xacts AS
  (status text, server name, transaction xid, gid text,
    prepared timestamp with time zone, owner name, database name);

CREATE OR REPLACE FUNCTION
  pg_foreign_prepared_xacts (server name)
  RETURNS SETOF pg_prepared_xacts AS $$
DECLARE
  sql TEXT;
BEGIN
  PERFORM * FROM pg_user_mappings
    WHERE srvname = server AND (usename = current_user OR usename = 'public');
  IF NOT FOUND THEN
    RAISE EXCEPTION 'user mapping for server "%" and user "%" not found',
      server, current_user;
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

-- Is the backend with the given PID still running the local transaction
-- with the given XID? Note that here the local transaction is considered
-- as running until it and its foreign transactions all have been completed.
--
-- We can determine that the local transaction is still running if the record
-- with the given PID and XID is found in pg_stat_activity. But
-- pg_stat_activity reports backend_xid is NULL after the local transaction
-- is completed even though its foreign transactions have not been
-- completed yet. To handle this case, we consider that the local transaction
-- is running if its backend_xid and _xmin are NULL, and its state is active.
--
-- But in the latter case, the XID of the local transaction marked as running
-- might not be the same as the given one. That is, this function may return
-- the false result, i.e., may report the transaction with the given XID is
-- running even though it's already completed. This is OK for current use of
-- this function. The caller of this is designed not to perform wrong action
-- even when such false result is returned. And then it can call this function
-- again to get the right result. Also basically such false result is less likely
-- to be returned.
CREATE OR REPLACE FUNCTION
  pg_local_and_foreign_xacts_are_running(txid xid, pid integer)
  RETURNS boolean AS $$
BEGIN
  PERFORM *
    FROM pg_stat_get_activity(pid)
    WHERE backend_xid = txid OR
      (backend_xid IS NULL AND backend_xmin IS NULL AND state = 'active');
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
      -- Because pg_local_and_foreign_xacts_are_running() can return true
      -- even when the backend with the given PID is running the transaction
      -- with XID other than the given one. But this is OK because basically
      -- this can rarely happen and we can just retry soon later.
      CONTINUE WHEN pg_local_and_foreign_xacts_are_running(fxid::xid, pid);

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
