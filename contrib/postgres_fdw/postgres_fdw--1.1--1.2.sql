/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE SCHEMA pgfdw_plus;

CREATE TABLE pgfdw_plus.xact_commits (
  fxid xid8 primary key,
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
  PERFORM * FROM pg_foreign_server fs, pg_foreign_data_wrapper fdw
    WHERE fs.srvfdw = fdw.oid AND fs.srvname = server AND
      fdw.fdwname = 'postgres_fdw';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'foreign data wrapper of specified server must be "postgres_fdw"';
  END IF;
  PERFORM * FROM pg_extension WHERE extname = 'dblink';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'extension "dblink" must be installed';
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
CREATE OR REPLACE FUNCTION
  pg_local_xact_is_running(target_xid xid, target_pid integer)
  RETURNS boolean AS $$
DECLARE
  r record;
BEGIN
  -- Return false if no record with the given PID is found in pg_stat_activity,
  -- i.e., the target backend is not running.
  SELECT * INTO r FROM pg_stat_get_activity(target_pid);
  IF NOT FOUND THEN RETURN false; END IF;

  -- Return true if the record with the given PID and XID is found
  -- in pg_stat_activty.
  IF r.backend_xid = target_xid THEN RETURN true; END IF;

  -- While the backend is processing the second phase of two phase commit
  -- protocol (i.e., after the local transaction is completed but before
  -- its state is changed to 'idle'), pg_stat_activity reports its backend_xid
  -- and backend_xmin are NULL and also state is 'active'. But we cannot
  -- determine from pg_stat_activity whether it's running the local transaction
  -- with the given XID or other one. So in this case we check whether
  -- the record for the lock of transactionid with the given PID and XID is
  -- found in pg_locks. If found, we can determine that the backend is
  -- running the local transaction with the given XID.
  --
  -- Note that at first we use pg_stat_get_activity() rather than pg_locks
  -- because it's faster and enables us to determine that in most cases.
  IF r.backend_xid IS NULL AND r.backend_xmin IS NULL AND r.state = 'active' THEN
    PERFORM * FROM pg_locks WHERE locktype = 'transactionid' AND
      pid = target_pid AND transactionid = target_xid;
    IF FOUND THEN RETURN true; END IF;
  END IF;

  RETURN false;
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
  connected boolean := false;
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
      -- Because pg_local_xact_is_running() can return true
      -- even when the backend with the given PID is running the transaction
      -- with XID other than the given one. But this is OK because basically
      -- this can rarely happen and we can just retry soon later.
      CONTINUE WHEN pg_local_xact_is_running(full_xid::xid, pid);

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

      -- Use dblink_connect() and dblink_exec() here instead of dblink()
      -- so that we can establish new connection to the foreign server
      -- only once and reuse that connection to execute the transaction
      -- command. This would decrease the number of connection establishments
      -- and improve the performance of this function especially
      -- when there are lots of foreign prepared transactions to resolve.
      IF NOT connected THEN
        PERFORM dblink_connect('pgfdw_plus_conn', server);
	connected := true;
      END IF;
      PERFORM dblink_exec('pgfdw_plus_conn', sql);
    END IF;
  END LOOP;

  IF connected THEN
    PERFORM dblink_disconnect('pgfdw_plus_conn');
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Set the current user to the proper user, to connect to the foreign
-- server based on the user mapping.
CREATE OR REPLACE FUNCTION
  pg_set_role_with_user_mapping(orig name, usename name)
  RETURNS void AS $$
DECLARE
  dest name := NULL;
  errmsg TEXT;
BEGIN
  IF usename = 'public' THEN
    dest := orig;
  ELSIF usename <> current_user THEN
    dest := usename;
  ELSE
    RETURN;
  END IF;

  BEGIN
    EXECUTE 'SET ROLE ' || dest;
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
    RAISE NOTICE 'could not set current user to user "%"',
      dest USING DETAIL = 'Error message: ' || errmsg;
  END;
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
    PERFORM pg_set_role_with_user_mapping(orig_user, r.usename);

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
    PERFORM pg_set_role_with_user_mapping(orig_user, r.usename);

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
  IF current_setting('transaction_read_only')::boolean THEN
    RAISE EXCEPTION 'cannot execute pg_vacuum_xact_commits() in a read-only transaction';
  END IF;
  SELECT * INTO r FROM pg_min_fxid_foreign_prepared_xacts_all();
  RETURN QUERY DELETE FROM pgfdw_plus.xact_commits xc
    WHERE xc.fxid < r.fxmin AND xc.umids <@ r.umids RETURNING *;
END;
$$ LANGUAGE plpgsql;
