# postgres_fdw_plus

## Configuration Parameters

### postgres_fdw.two_phase_commit (enum)
If true, a local transaction and all foreign transactions that
it opened on remote servers via postgres_fdw_plus are committed
by using two phase commit protocol, when it's committed.
If false (default), they are committed as postgres_fdw currently does,
i.e., two phase commit protocol is not used. If "prepare",
all those foreign transactions are marked as prepared, but not
committed, i.e., postgres_fdw_plus issues PREPARE TRANSACTION
but not COMMIT PREPARED for them on remote servers.

Any users can change this setting.

### postgres_fdw.track_xact_commits (boolean)
If true (default), information about transactions that used two phase
commit protocol and was successfully committed are collected in
pgfdw_plus.xact_commits table. If false, no such information is collected.
This setting has no effect if postgres_fdw.two_phase_commit is false.

Any users can change this setting.
