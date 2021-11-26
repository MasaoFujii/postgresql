-- ===================================================================
-- Global settings
-- ===================================================================
-- Don't display CONTEXT fields in messages from the server,
-- to make the tests stable.
\set SHOW_CONTEXT never

-- Don't drop remote connections after every transaction,
-- to make the tests stable.
SET debug_discard_caches = 0;

-- ===================================================================
-- Create database users
-- ===================================================================
CREATE ROLE regress_pgfdw_local_super1 SUPERUSER;
CREATE ROLE regress_pgfdw_local_super2 SUPERUSER;
CREATE ROLE regress_pgfdw_remote_super1 SUPERUSER LOGIN;
CREATE ROLE regress_pgfdw_remote_super2 SUPERUSER LOGIN;
SET ROLE regress_pgfdw_local_super1;

-- ===================================================================
-- Create FDW objects
-- ===================================================================

-- postgres_fdw was already installed in postgres_fdw.sql.

DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER pgfdw_plus_loopback1
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$',
                     application_name 'pgfdw_plus_loopback1'
            )$$;
        EXECUTE $$CREATE SERVER pgfdw_plus_loopback2
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$',
                     application_name 'pgfdw_plus_loopback2'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR PUBLIC SERVER pgfdw_plus_loopback1
  OPTIONS (user 'regress_pgfdw_remote_super1');
CREATE USER MAPPING FOR CURRENT_USER SERVER pgfdw_plus_loopback2
  OPTIONS (user 'regress_pgfdw_remote_super2');

-- Create dummy foreign data wrapper, server and user mapping
-- to test the error cases.
CREATE FOREIGN DATA WRAPPER pgfdw_plus_dummy;
CREATE SERVER pgfdw_plus_dummy_server FOREIGN DATA WRAPPER pgfdw_plus_dummy;
CREATE USER MAPPING FOR PUBLIC SERVER pgfdw_plus_dummy_server;

-- Drop previously-created server that may have unexpected effect
-- on this test, to make the tests stable.
SET client_min_messages TO 'error';
DROP SERVER IF EXISTS testserver1 CASCADE;
RESET client_min_messages;

-- ===================================================================
-- Create objects used by local transaction or through FDW
-- pgfdw_plus_loopback1 and pgfdw_plus_loopback2 servers
-- ===================================================================
CREATE SCHEMA regress_pgfdw_plus;
SET search_path TO regress_pgfdw_plus, "$user", public;

CREATE TABLE t0 (c1 int PRIMARY KEY);
CREATE TABLE t1 (c1 int PRIMARY KEY);
CREATE TABLE t2 (c1 int PRIMARY KEY);

-- Disable autovacuum for these tables to avoid unexpected effects of that.
ALTER TABLE t0 SET (autovacuum_enabled = 'false');
ALTER TABLE t1 SET (autovacuum_enabled = 'false');
ALTER TABLE t2 SET (autovacuum_enabled = 'false');

-- ===================================================================
-- Create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE ft1 (c1 int) SERVER pgfdw_plus_loopback1
    OPTIONS (schema_name 'regress_pgfdw_plus', table_name 't1');
CREATE FOREIGN TABLE ft2 (c1 int) SERVER pgfdw_plus_loopback2
    OPTIONS (schema_name 'regress_pgfdw_plus', table_name 't2');

CREATE VIEW ftv AS SELECT * FROM t0 UNION ALL
  SELECT * FROM ft1 UNION ALL SELECT * FROM ft2;

-- ===================================================================
-- Test two phase commit
-- ===================================================================
SET postgres_fdw.two_phase_commit TO true;

-- COMMIT command on local transaction causes foreign transactions to
-- be committed via two phase commit protocol.
BEGIN;
INSERT INTO t0 VALUES (100);
INSERT INTO ft1 VALUES (100);
INSERT INTO ft2 VALUES (100);
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ftv WHERE c1 = 100;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- ROLLBACK command on local transaction causes foreign transactions to
-- be rollbacked without using two phase commit protocol.
BEGIN;
INSERT INTO t0 VALUES (110);
INSERT INTO ft1 VALUES (110);
INSERT INTO ft2 VALUES (110);
ROLLBACK;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ftv WHERE c1 = 110;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- Failure of prepare phase on one of foreign servers causes
-- all transactions to be rollbacked.
BEGIN;
INSERT INTO t0 VALUES (120);
INSERT INTO ft1 VALUES (120);
INSERT INTO ft2 VALUES (120);
SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_activity
    WHERE application_name = 'pgfdw_plus_loopback1';
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ftv WHERE c1 = 120;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- Failure of prepare phase on the other foreign server causes
-- all transactions to be rollbacked.
BEGIN;
INSERT INTO t0 VALUES (130);
INSERT INTO ft1 VALUES (130);
INSERT INTO ft2 VALUES (130);
SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_activity
    WHERE application_name = 'pgfdw_plus_loopback2';
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ftv WHERE c1 = 130;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- two_phase_commit = on causes read-only foreign transactions to
-- be committed without using two phase commit protocol.
BEGIN;
SELECT count(*) FROM t0;
SELECT count(*) FROM ft1;
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- two_phase_commit = on causes only write foreign transaction to
-- be committed via two phase commit protocol.
BEGIN;
INSERT INTO t0 VALUES (140);
INSERT INTO ft1 VALUES (140);
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ftv WHERE c1 = 140;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- two_phase_commit = always causes even read-only foreign transactions
-- to be committed via two phase commit protocol.
SET postgres_fdw.two_phase_commit TO 'always';
BEGIN;
SELECT count(*) FROM t0;
SELECT count(*) FROM ft1;
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT * FROM pg_prepared_xacts;
SELECT array_length(umids, 1) FROM pgfdw_plus.xact_commits ORDER BY fxid;

-- ===================================================================
-- Test error cases of pg_foreign_prepared_xacts and
-- pg_resolve_foreign_prepared_xacts
-- ===================================================================
-- Should fail because specified server doesn't exist.
SELECT * FROM pg_foreign_prepared_xacts('nonexistent');
SELECT * FROM pg_resolve_foreign_prepared_xacts('nonexistent');

-- Should fail because there is no user mapping for specified server and
-- current user.
SET ROLE regress_pgfdw_local_super2;
SELECT * FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback2');
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback2');
SET ROLE regress_pgfdw_local_super1;

-- Should fail because foreign data wrapper of specified server
-- is not postgres_fdw.
SELECT * FROM pg_foreign_prepared_xacts('pgfdw_plus_dummy_server');
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_dummy_server');

-- Should fail because dblink has not been installed yet.
SELECT * FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback1');

-- ===================================================================
-- Test functions to resolve foreign prepared transactions
-- ===================================================================
CREATE EXTENSION dblink;

-- These functions should return 0 rows because there are no foreign
-- prepared transactions.
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback2');
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();

-- xact_commits should be emptied because there are no foreign
-- prepared transactions.
SELECT count(*) FROM pgfdw_plus.xact_commits;
SELECT count(*) FROM pg_vacuum_xact_commits();
SELECT count(*) FROM pgfdw_plus.xact_commits;

-- Enable skip_commit_phase to create foreign prepared transactions.
-- Note that more than two foreign prepared transactions cannot be created
-- because max_prepared_xacts is set to 2 in regression test.
SET postgres_fdw.skip_commit_phase TO true;
BEGIN;
INSERT INTO ft1 VALUES (200);
INSERT INTO ft2 VALUES (200);
COMMIT;
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback2');
SELECT count(*) FROM pgfdw_plus.xact_commits;

-- Resolve foreign prepared transactions on only one of servers.
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback2');

-- xact_commits still should have one row referencing to foreign prepared
-- transactions on the other server.
SELECT count(*) FROM pg_vacuum_xact_commits();
SELECT count(*) FROM pgfdw_plus.xact_commits;

-- All foreign prepared transactions are resolved and xact_commits
-- should be empty.
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT count(*) FROM pg_foreign_prepared_xacts('pgfdw_plus_loopback2');
SELECT count(*) FROM pg_vacuum_xact_commits();
SELECT count(*) FROM pgfdw_plus.xact_commits;

-- Should fail because pg_vacuum_xact_commits() cannot be executed
-- in read-only transaction.
BEGIN READ ONLY;
SELECT count(*) FROM pg_vacuum_xact_commits();
COMMIT;

-- ===================================================================
-- Test that DEALLOCATE ALL is executed properly even when
-- two_phase_commit is on or prepare.
-- ===================================================================
-- DEALLOCATE ALL should follow COMMIT PREPARED because
-- a subtransaction is aborted.
SET postgres_fdw.skip_commit_phase TO false;
BEGIN;
SAVEPOINT s;
INSERT INTO ft1 VALUES (300);
INSERT INTO ft2 VALUES (300);
ROLLBACK TO SAVEPOINT s;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;

-- DEALLOCATE ALL should follow PREPARE TRANSACTION
-- if a subtransaction is aborted and skip_commit_phase is enabled.
SET postgres_fdw.skip_commit_phase TO true;
BEGIN;
SAVEPOINT s;
INSERT INTO ft1 VALUES (310);
INSERT INTO ft2 VALUES (310);
ROLLBACK TO SAVEPOINT s;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();

-- DEALLOCATE ALL should not run because a subtransaction is not aborted.
BEGIN;
SAVEPOINT s;
INSERT INTO ft1 VALUES (320);
INSERT INTO ft2 VALUES (320);
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%' ORDER BY query;
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();

-- ===================================================================
-- Test functions executed by non-superusers
-- ===================================================================
CREATE ROLE regress_pgfdw_local_normal1 NOSUPERUSER;
CREATE ROLE regress_pgfdw_remote_normal1 NOSUPERUSER LOGIN;

CREATE USER MAPPING FOR regress_pgfdw_local_normal1 SERVER pgfdw_plus_loopback1
  OPTIONS (user 'regress_pgfdw_remote_normal1');

GRANT ALL ON SCHEMA pgfdw_plus TO regress_pgfdw_local_normal1;
GRANT ALL ON TABLE pgfdw_plus.xact_commits TO regress_pgfdw_local_normal1;
GRANT ALL ON SCHEMA regress_pgfdw_plus TO regress_pgfdw_local_normal1;
GRANT ALL ON FOREIGN SERVER pgfdw_plus_loopback1 TO regress_pgfdw_local_normal1;

SET ROLE regress_pgfdw_local_normal1;
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();
SELECT count(*) FROM pg_vacuum_xact_commits();

SET ROLE regress_pgfdw_local_super1;
SELECT count(*) FROM pg_resolve_foreign_prepared_xacts_all();
SELECT count(*) FROM pg_vacuum_xact_commits();

-- ===================================================================
-- Reset global settings
-- ===================================================================
RESET postgres_fdw.two_phase_commit;
RESET debug_discard_caches;

\unset SHOW_CONTEXT
