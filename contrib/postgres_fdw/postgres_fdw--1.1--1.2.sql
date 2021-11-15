/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE SCHEMA pgfdw_plus;

CREATE TABLE pgfdw_plus.xact_commits (
  fxid xid8 primary key,
  pid integer,
  umids oid[]
);

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
  pg_resolve_foreign_prepared_xacts (server name, force boolean DEFAULT false)
  RETURNS SETOF resolve_foreign_prepared_xacts AS $$
DECLARE
  r resolve_foreign_prepared_xacts;
  sql TEXT;
  full_xid xid8;
  pid integer;
BEGIN
  FOR r IN SELECT NULL AS status, server, *
    FROM pg_foreign_prepared_xacts(server) LOOP
    sql := NULL;
    BEGIN
      full_xid := split_part(r.gid, '_', 2)::xid8;
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
      CONTINUE WHEN pg_local_and_foreign_xacts_are_running(full_xid::xid, pid);

      -- At first use pg_xact_status() to check the commit status of
      -- the transaction with full_xid. Because we can use the function
      -- for that purpose even when postgres_fdw.track_xact_commits is
      -- disabled, and using it is faster than looking up
      -- pgfdw_plus.xact_commits table.
      r.status := pg_xact_status(full_xid);
      CASE r.status
        WHEN 'committed' THEN
          sql := 'COMMIT PREPARED ''' || r.gid || '''';
        WHEN 'aborted' THEN
          sql := 'ROLLBACK PREPARED ''' || r.gid || '''';
      END CASE;
    EXCEPTION WHEN OTHERS THEN
    END;

    -- If pg_xact_status(full_xid) cannot report the commit status,
    -- look up pgfdw_plus.xact_commits. We can determine that
    -- the transaction was committed if the entry for full_xid is found in it.
    -- Otherwise we can determine that the transaction was aborted
    -- unless postgres_fdw.track_xact_commits had been disabled so far.
    -- For the case where that's disabled, by default we don't rollback
    -- the foreign prepared transaction if no entry for full_xid is found
    -- in xact_commits. If the second argument "force" is true,
    -- that transaction is forcibly rollbacked.
    IF sql IS NULL THEN
      PERFORM * FROM pgfdw_plus.xact_commits WHERE fxid = full_xid LIMIT 1;
      IF FOUND THEN
        r.status := 'committed';
        sql := 'COMMIT PREPARED ''' || r.gid || '''';
      ELSIF force THEN
        r.status := 'aborted';
        sql := 'ROLLBACK PREPARED ''' || r.gid || '''';
      ELSE
        RAISE NOTICE 'could not resolve foreign prepared transaction with gid "%"',
          r.gid USING HINT = 'Commit status of transaction with fxid "' ||
          full_xid || '" not found';
      END IF;
    END IF;

    IF sql IS NOT NULL THEN
      RETURN NEXT r;
      PERFORM dblink(server, sql);
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
  pg_resolve_foreign_prepared_xacts_all(force boolean DEFAULT false)
  RETURNS SETOF resolve_foreign_prepared_xacts AS $$
DECLARE
  r RECORD;
  orig_user name;
  errmsg TEXT;
BEGIN
  orig_user = current_user;
  FOR r IN
    SELECT * FROM pg_foreign_data_wrapper fdw,
      pg_foreign_server fs, pg_user_mappings um
    WHERE fdw.fdwname = 'postgres_fdw' AND
      fs.srvfdw = fdw.oid AND fs.oid = um.srvid
    ORDER BY fs.srvname
  LOOP
    IF r.usename = 'public' THEN
      EXECUTE 'SET ROLE ' || orig_user;
    ELSIF r.usename <> current_user THEN
      EXECUTE 'SET ROLE ' || r.usename;
    END IF;

    BEGIN
      RETURN QUERY SELECT *
        FROM pg_resolve_foreign_prepared_xacts(r.srvname, force);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
      RAISE NOTICE 'could not resolve foreign prepared transactions on server "%"',
        r.srvname USING DETAIL = 'Error message: ' || errmsg;
    END;
  END LOOP;
  EXECUTE 'SET ROLE ' || orig_user;
END;
$$ LANGUAGE plpgsql;

-- Get the minimum value of full transaction IDs assigned to running
-- local transactions and foreign prepared (unresolved yet) ones.
-- This function also returns a list of user mapping OIDs
-- corresponding to the server that it successfully fetched the minimum
-- full transaction ID from.
CREATE OR REPLACE FUNCTION
  pg_min_fxid_foreign_prepared_xacts_all(OUT fxmin xid8, OUT umids oid[])
  RETURNS record AS $$
DECLARE
  r RECORD;
  fxid xid8;
  orig_user name;
  errmsg TEXT;
BEGIN
  orig_user = current_user;
  fxmin := pg_snapshot_xmin(pg_current_snapshot());
  FOR r IN
    SELECT * FROM pg_foreign_data_wrapper fdw,
      pg_foreign_server fs, pg_user_mappings um
    WHERE fdw.fdwname = 'postgres_fdw' AND
      fs.srvfdw = fdw.oid AND fs.oid = um.srvid
    ORDER BY fs.srvname
  LOOP
    IF r.usename = 'public' THEN
      EXECUTE 'SET ROLE ' || orig_user;
    ELSIF r.usename <> current_user THEN
      EXECUTE 'SET ROLE ' || r.usename;
    END IF;

    BEGIN
      -- Use min(bigint)::text::xid8 to calculate the minimum value of
      -- full transaction IDs, for now, instead of min(xid8) because
      -- PostgreSQL core hasn't supported yet min(xid8) aggregation
      -- function.
      SELECT min(split_part(gid, '_', 2)::bigint)::text::xid8 INTO fxid
        FROM pg_foreign_prepared_xacts(r.srvname);
      IF fxid IS NOT NULL AND fxid < fxmin THEN
        fxmin := fxid;
      END IF;
      umids := array_append(umids, r.umid);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
      RAISE NOTICE 'could not retrieve minimum full transaction ID from server "%"',
        r.srvname USING DETAIL = 'Error message: ' || errmsg;
    END;
  END LOOP;
  EXECUTE 'SET ROLE ' || orig_user;
END;
$$ LANGUAGE plpgsql;

-- Delete the records no longer necessary to resolve foreign prepared
-- transactions, from pgfdw_plus.xact_commits. This function returns
-- the deleted records.
CREATE OR REPLACE FUNCTION
  pg_vacuum_xact_commits()
  RETURNS SETOF pgfdw_plus.xact_commits AS $$
DECLARE
  r record;
BEGIN
  SELECT * INTO r FROM pg_min_fxid_foreign_prepared_xacts_all();
  RETURN QUERY DELETE FROM pgfdw_plus.xact_commits xc
    WHERE xc.fxid < r.fxmin AND xc.umids <@ r.umids RETURNING *;
END;
$$ LANGUAGE plpgsql;
