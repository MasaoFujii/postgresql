-- ===================================================================
-- global settings
-- ===================================================================
-- Don't display CONTEXT fields in messages from the server,
-- to make the tests stable.
\set SHOW_CONTEXT never

-- ===================================================================
-- create database users
-- ===================================================================
CREATE ROLE regress_pgfdw_plus_super1 SUPERUSER;
CREATE ROLE regress_pgfdw_plus_super2 SUPERUSER;
SET ROLE regress_pgfdw_plus_super1;

-- ===================================================================
-- create FDW objects
-- ===================================================================

-- postgres_fdw was already installed in postgres_fdw.sql

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

CREATE USER MAPPING FOR PUBLIC SERVER pgfdw_plus_loopback1;
CREATE USER MAPPING FOR CURRENT_USER SERVER pgfdw_plus_loopback2;

-- create dummy foreign data wrapper, server and user mapping
-- to test the error cases
CREATE FOREIGN DATA WRAPPER pgfdw_plus_dummy;
CREATE SERVER pgfdw_plus_dummy_server FOREIGN DATA WRAPPER pgfdw_plus_dummy;
CREATE USER MAPPING FOR PUBLIC SERVER pgfdw_plus_dummy_server;

-- ===================================================================
-- create objects used by local transaction or through FDW
-- pgfdw_plus_loopback1 and pgfdw_plus_loopback2 servers
-- ===================================================================
CREATE SCHEMA regress_pgfdw_plus;
SET search_path TO regress_pgfdw_plus, "$user", public;

CREATE TABLE t0 (c1 int PRIMARY KEY, c2 int);
CREATE TABLE t1 (c1 int PRIMARY KEY, c2 int);
CREATE TABLE t2 (c1 int PRIMARY KEY, c2 int);

-- Disable autovacuum for these tables to avoid unexpected effects of that
ALTER TABLE t0 SET (autovacuum_enabled = 'false');
ALTER TABLE t1 SET (autovacuum_enabled = 'false');
ALTER TABLE t2 SET (autovacuum_enabled = 'false');

INSERT INTO t0 SELECT id, id FROM generate_series(1, 10) id;
INSERT INTO t1 SELECT id, id FROM generate_series(11, 20) id;
INSERT INTO t2 SELECT id, id FROM generate_series(21, 30) id;

ANALYZE t0;
ANALYZE t1;
ANALYZE t2;

-- ===================================================================
-- create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE ft1 (c1 int, c2 int) SERVER pgfdw_plus_loopback1
    OPTIONS (schema_name 'regress_pgfdw_plus', table_name 't1');
CREATE FOREIGN TABLE ft2 (c1 int, c2 int) SERVER pgfdw_plus_loopback2
    OPTIONS (schema_name 'regress_pgfdw_plus', table_name 't2');

-- ===================================================================
-- test two phase commit
-- ===================================================================
SET debug_discard_caches = 0;
SET postgres_fdw.two_phase_commit TO true;

-- All three transactions are committed via two phase commit
-- protocol because they are write transactions.
BEGIN;
INSERT INTO t0 VALUES (100, 100);
INSERT INTO ft1 VALUES (100, 100);
INSERT INTO ft2 VALUES (100, 100);
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM t0 WHERE c1 = 100;
SELECT count(*) FROM ft1 WHERE c1 = 100;
SELECT count(*) FROM ft2 WHERE c1 = 100;

-- All three transactions are committed via two phase commit protocol
-- because two (local and foreign) of them are write transactions.
BEGIN;
INSERT INTO t0 VALUES (200, 200);
INSERT INTO ft1 VALUES (200, 200);
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM t0 WHERE c1 = 200;
SELECT count(*) FROM ft1 WHERE c1 = 200;

-- All three transactions are committed via two phase commit protocol
-- because two (two foreign) of them are write transactions.
BEGIN;
SELECT count(*) FROM t0;
INSERT INTO ft1 VALUES (300, 300);
INSERT INTO ft2 VALUES (300, 300);
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM ft1 WHERE c1 = 300;
SELECT count(*) FROM ft2 WHERE c1 = 300;

-- All three transactions are committed without using two phase commit
-- protocol because only local transaction is write one.
BEGIN;
INSERT INTO t0 VALUES (400, 400);
SELECT count(*) FROM ft1;
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT count(*) FROM t0 WHERE c1 = 400;

-- All three transactions are committed without using two phase commit
-- protocol because only one foreign transaction is write one.
BEGIN;
SELECT count(*) FROM t0;
INSERT INTO ft1 VALUES (500, 500);
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT count(*) FROM ft1 WHERE c1 = 500;

-- All three transactions are committed without using two phase commit
-- protocol because there are no write transactions.
BEGIN;
SELECT count(*) FROM t0;
SELECT count(*) FROM ft1;
SELECT count(*) FROM ft2;
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';

-- When local transaction is rollbacked, foreign transactions are
-- rollbacked without using two phase commit protocol.
BEGIN;
INSERT INTO t0 VALUES (600, 600);
INSERT INTO ft1 VALUES (600, 600);
INSERT INTO ft2 VALUES (600, 600);
ROLLBACK;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT count(*) FROM t0 WHERE c1 = 600;
SELECT count(*) FROM ft1 WHERE c1 = 600;
SELECT count(*) FROM ft2 WHERE c1 = 600;

-- All three transactions are rollbacked because PREPARE TRANSACTION
-- fails on one of foreign server.
BEGIN;
INSERT INTO t0 VALUES (700, 700);
INSERT INTO ft1 VALUES (700, 700);
INSERT INTO ft2 VALUES (700, 700);
SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_activity
       WHERE application_name = 'pgfdw_plus_loopback2';
COMMIT;
SELECT split_part(query, '_', 1) FROM pg_stat_activity
    WHERE application_name LIKE 'pgfdw_plus_loopback%';
SELECT * FROM pg_prepared_xacts;
SELECT count(*) FROM t0 WHERE c1 = 700;
SELECT count(*) FROM ft1 WHERE c1 = 700;
SELECT count(*) FROM ft2 WHERE c1 = 700;

RESET postgres_fdw.two_phase_commit;
RESET debug_discard_caches;

-- ===================================================================
-- test error cases of pg_foreign_prepared_xacts
-- ===================================================================
-- should fail because foreign data wrapper of specified server
-- is not postgres_fdw
SELECT * FROM pg_foreign_prepared_xacts('pgfdw_plus_dummy_server');

-- ===================================================================
-- test pg_resolve_foreign_prepared_xacts
-- ===================================================================
CREATE EXTENSION dblink;

SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback1');
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback2');
-- This test should fail because the specified server doesn't exist.
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_nonexistent');

SET ROLE regress_pgfdw_plus_super2;
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback1');
-- This test should fail because no user mapping for the specified server
-- and regress_pgfdw_plus_super2 exists.
SELECT * FROM pg_resolve_foreign_prepared_xacts('pgfdw_plus_loopback2');
SET ROLE regress_pgfdw_plus_super1;

-- ===================================================================
-- test pg_resolve_foreign_prepared_xacts_all
-- ===================================================================
SELECT * FROM pg_resolve_foreign_prepared_xacts_all();

-- ===================================================================
-- reset global settings
-- ===================================================================
\unset SHOW_CONTEXT
