/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL global transaction manager for foreign servers.
 *
 * This module contains the code for managing transactions started on foreign
 * servers.
 *
 * An FDW that implements both commit and rollback APIs can request to register
 * the foreign transaction by FdwXactRegisterXact() to participate it to a
 * group of distributed tranasction.  The registered foreign transactions are
 * identified by user mapping OID.  On commit and rollback, the global
 * transaction manager calls corresponding FDW API to end the foreign
 * tranasctions.
 *
 * To achieve commit among all foreign servers atomically, the global transaction
 * manager supports two-phase commit protocol, which is a type of atomic commitment
 * protocol. We WAL log the foreign transaction state so foreign transaction state
 * is crash-safe.
 *
 * FOREIGN TRANSACTION RESOLUTION
 *
 * The transaction involving multiple foreign transactions uses two-phase commit
 * protocol to commit the distributed transaction if enabled.  The basic strategy
 * is that we prepare all of the remote transactions before committing locally and
 * commit them after committing locally.
 *
 * At pre-commit of local transaction, we prepare the transactions on all foreign
 * servers after logging the information of foreign transaction.  The result of
 * distributed transaction is determined by the result of the corresponding local
 * transaction.  Once the local transaction is successfully committed, all
 * transactions on foreign servers must be committed.  In case where an error occurred
 * before the local transaction commit all transactions must be aborted.  After
 * committing or rolling back locally, we leave foreign transactions as in-doubt
 * transactions and then notify the resolver process. The resolver process asynchronously
 * resolves these foreign transactions according to the result of the corresponding local
 * transaction.  Also, the user can use pg_resolve_foreign_xact() SQL function to
 * resolve a foreign transaction manually.
 *
 * At PREPARE TRANSACTION, we prepare all transactions on foreign servers by executing
 * PrepareForeignTransaction() API for each foreign transaction regardless of data on
 * the foreign server having been modified.  At COMMIT PREPARED and ROLLBACK PREPARED,
 * we commit or rollback only the local transaction but not do anything for involved
 * foreign transactions.  The prepared foreign transactinos are resolved by a resolver
 * process asynchronously.  Also, users can use pg_resolve_foreign_xact() SQL function
 * that resolve a foreign transaction manually.
 *
 * LOCKING
 *
 * Whenever a foreign transaction is processed, the corresponding FdwXactState
 * entry is updated. To avoid holding the lock during transaction processing
 * which may take an unpredictable time the in-memory data of foreign
 * transaction follows a locking model based on the following linked concepts:
 *
 * * A process who is going to work on the foreign transaction needs to set
 *	 locking_backend of the FdwXactState entry, which prevents the entry from being
 *	 updated and removed by concurrent processes.
 * * All FdwXactState fields except for status are protected by FdwXactLock.  The
 *   status is protected by its mutex.
 *
 * RECOVERY
 *
 * During replay WAL and replication FdwXactCtl also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdwxact records happens by the following rules:
 *
 * * At the beginning of recovery, pg_fdwxacts is scanned once, filling FdwXactState
 *	 with entries marked with fdwxact->inredo and fdwxact->ondisk.	FdwXactState file
 *	 data older than the XID horizon of the redo position are discarded.
 * * On PREPARE redo, the foreign transaction is added to FdwXactCtl->xacts.
 *	 We set fdwxact->inredo to true for such entries.
 * * On Checkpoint we iterate through FdwXactCtl->xacts entries that
 *	 have fdwxact->inredo set and are behind the redo_horizon.	We save
 *	 them to disk and then set fdwxact->ondisk to true.
 * * On resolution we delete the entry from FdwXactCtl->xacts.  If
 *	 fdwxact->ondisk is true, the corresponding entry from the disk is
 *	 additionally deleted.
 * * RecoverFdwXacts() and PrescanFdwXacts() have been modified to go through
 *	 fdwxact->inredo entries that have not made it to disk.
 *
 * These replay rules are borrowed from twophase.c
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/twophase.h"
#include "access/resolver_internal.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_user_mapping.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Directory where the foreign prepared transaction files will reside */
#define FDWXACTS_DIR "pg_fdwxact"

/* Initial size of the hash table */
#define FDWXACT_HASH_SIZE	64

/* Check the FdwXactEntry supports commit (and rollback) callbacks */
#define ServerSupportTransactionCallback(fdwent) \
	(((FdwXactEntry *)(fdwent))->commit_foreign_xact_fn != NULL)

/* Check the FdwXactEntry is capable of two-phase commit  */
#define ServerSupportTwophaseCommit(fdwent) \
	(((FdwXactEntry *)(fdwent))->prepare_foreign_xact_fn != NULL)

/* Foreign twophase commit is enabled and requested by user */
#define IsForeignTwophaseCommitRequested() \
	 (foreign_twophase_commit > FOREIGN_TWOPHASE_COMMIT_DISABLED)

/*
 * Name of foreign prepared transaction file is 8 bytes xid and
 * user mapping OID separated by '_'.
 *
 * Since FdwXactState is identified by user mapping OID and it's unique
 * within a distributed transaction, the name is fairly enough to
 * ensure uniqueness.
 */
#define FDWXACT_FILE_NAME_LEN (8 + 1 + 8)
#define FdwXactStateFilePath(path, xid, umid)	\
	snprintf(path, MAXPGPATH, FDWXACTS_DIR "/%08X_%08X", \
			 xid, umid)

/*
 * Structure to bundle the foreign transaction participant.
 *
 * Participants are identified by user mapping OID, rather than pair of
 * user OID and server OID. See README.fdwxact for the discussion.
 */
typedef struct FdwXactEntry
{
	/* user mapping OID, hash key (must be first) */
	Oid	umid;

	ForeignServer *server;
	UserMapping *usermapping;

	/*
	 * Pointer to a FdwXactState entry in the global array. NULL if the entry is
	 * not inserted yet but this is registered as a participant.
	 */
	FdwXactState		fdwxact;

	/* true if modified the data on the server */
	bool		modified;

	/* Callbacks for foreign transaction */
	CommitForeignTransaction_function commit_foreign_xact_fn;
	RollbackForeignTransaction_function rollback_foreign_xact_fn;
	PrepareForeignTransaction_function prepare_foreign_xact_fn;
	GetPrepareId_function get_prepareid_fn;
} FdwXactEntry;

/*

 * The current distributed transaction state. Members of participants must
 * support at least both commit and rollback APIs
 * (ServerSupportTransactionCallback() is true)..
 */
typedef struct DistributedXactStateData
{
	bool	all_prepared; /* all participants are prepared? */

	bool	twophase_commit_required;

	/* Statistics of participants */
	int		nparticipants_no_twophase; /* how many participants doesn't support
										* two-phase commit protocol? */
	int		nparticipants_modified;		/* how many participants are modified? */

	HTAB	*participants;
} DistributedXactStateData;
static DistributedXactStateData DistributedXactState = {
	.all_prepared = false,
	.twophase_commit_required = false,
	.nparticipants_no_twophase = 0,
	.nparticipants_modified = 0,
	.participants = NULL,
};

/* Check the current transaction has at least one fdwxact participant */
#define HasFdwXactParticipant() \
	(DistributedXactState.participants != NULL && \
	 hash_get_num_entries(DistributedXactState.participants) > 0)

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;


/* Guc parameter */
int			max_prepared_foreign_xacts = 0;
int			max_foreign_xact_resolvers = 0;
int			foreign_twophase_commit = FOREIGN_TWOPHASE_COMMIT_DISABLED;

static void RemoveFdwXactEntry(Oid umid);
static void EndFdwXactEntry(FdwXactEntry *fdwent, bool isCommit,
							bool is_parallel_worker);
static char *getFdwXactIdentifier(FdwXactEntry *fdwent, TransactionId xid);
static int ForgetAllParticipants(void);

static void FdwXactPrepareForeignTransactions(TransactionId xid, bool prepare_all);
static FdwXactState FdwXactInsertEntry(TransactionId xid, FdwXactEntry *fdwent,
									   char *identifier);
static void AtProcExit_FdwXact(int code, Datum arg);
static void FdwXactComputeRequiredXmin(void);
static FdwXactStatus FdwXactGetTransactionFate(TransactionId xid);
static void FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void FdwXactRedoRemove(TransactionId xid, Oid umid, bool givewarning);
static void XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len);
static char *ProcessFdwXactBuffer(TransactionId xid, Oid umid,
								  XLogRecPtr insert_start_lsn, bool fromdisk);
static char *ReadFdwXactStateFile(TransactionId xid, Oid umid);
static void RemoveFdwXactStateFile(TransactionId xid, Oid umid, bool giveWarning);
static bool checkForeignTwophaseCommitRequired(bool local_modified);

static FdwXactState insert_fdwxact(Oid dbid, TransactionId xid, Oid umid, Oid serverid,
							  Oid owner, char *identifier);
static void remove_fdwxact(FdwXactState fdwxact);
static FdwXactState get_fdwxact(TransactionId xid, Oid umid);
static FdwXactState get_fdwxact_with_check(TransactionId xid, Oid umid,
										   bool check_two_phase);
static void pg_foreign_xact_callback(int code, Datum arg);

/*
 * Calculates the size of shared memory allocated for maintaining foreign
 * prepared transaction entries.
 */
Size
FdwXactShmemSize(void)
{
	Size		size;

	/* Size for foreign transaction information array */
	size = offsetof(FdwXactCtlData, xacts);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactState)));
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactStateData)));

	return size;
}

/*
 * Initialization of shared memory for maintaining foreign prepared transaction
 * entries. The shared memory layout is defined in definition of FdwXactCtlData
 * structure.
 */
void
FdwXactShmemInit(void)
{
	bool		found;

	FdwXactCtl = ShmemInitStruct("Foreign transactions table",
								 FdwXactShmemSize(),
								 &found);
	if (!IsUnderPostmaster)
	{
		FdwXactState		fdwxacts;
		int			cnt;

		Assert(!found);
		FdwXactCtl->free_fdwxacts = NULL;
		FdwXactCtl->num_xacts = 0;

		/* Initialize the linked list of free FDW transactions */
		fdwxacts = (FdwXactState)
			((char *) FdwXactCtl +
			 MAXALIGN(offsetof(FdwXactCtlData, xacts) +
					  sizeof(FdwXactState) * max_prepared_foreign_xacts));
		for (cnt = 0; cnt < max_prepared_foreign_xacts; cnt++)
		{
			fdwxacts[cnt].status = FDWXACT_STATUS_INVALID;
			fdwxacts[cnt].fdwxact_free_next = FdwXactCtl->free_fdwxacts;
			FdwXactCtl->free_fdwxacts = &fdwxacts[cnt];
			SpinLockInit(&fdwxacts[cnt].mutex);
		}
	}
	else
	{
		Assert(FdwXactCtl);
		Assert(found);
	}
}

/*
 * Register the given foreign transaction identified by the given user
 * mapping OID as a participant of the transaction.
 */
void
FdwXactRegisterXact(UserMapping *usermapping, bool modified)
{
	FdwXactEntry	*fdwent;
	FdwRoutine	*routine;
	Oid	umid;
	MemoryContext old_ctx;
	bool	found;

	Assert(IsTransactionState());

	if (DistributedXactState.participants == NULL)
	{
		HASHCTL	ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FdwXactEntry);

		DistributedXactState.participants = hash_create("fdw xact participants",
															FDWXACT_HASH_SIZE,
															&ctl, HASH_ELEM | HASH_BLOBS);
	}

	umid = usermapping->umid;
	fdwent = hash_search(DistributedXactState.participants,
						 (void *) &umid, HASH_ENTER, &found);

	/* Already registered */
	if (found)
	{
		/* Update statistics if necessary  */
		if (fdwent->modified && !modified)
			DistributedXactState.nparticipants_modified--;
		else if (!fdwent->modified && modified)
			DistributedXactState.nparticipants_modified++;

		fdwent->modified = modified;

		Assert(DistributedXactState.nparticipants_modified <=
		   hash_get_num_entries(DistributedXactState.participants));
		return;
	}

	/*
	 * The participant information needs to live until the end of the transaction
	 * where syscache is not available, so we save them in TopTransactionContext.
	 */
	old_ctx = MemoryContextSwitchTo(TopTransactionContext);

	fdwent->usermapping = GetUserMapping(usermapping->userid, usermapping->serverid);
	fdwent->server = GetForeignServer(usermapping->serverid);

	/*
	 * Foreign server managed by the transaction manager must implement
	 * transaction callbacks.
	 */
	routine = GetFdwRoutineByServerId(usermapping->serverid);
	if (!routine->CommitForeignTransaction)
		ereport(ERROR,
				(errmsg("cannot register foreign server not supporting transaction callback")));

	fdwent->fdwxact = NULL;
	fdwent->modified = modified;
	fdwent->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdwent->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;
	fdwent->prepare_foreign_xact_fn = routine->PrepareForeignTransaction;
	fdwent->get_prepareid_fn = routine->GetPrepareId;

	MemoryContextSwitchTo(old_ctx);

	/* Update statistics */
	if (!ServerSupportTwophaseCommit(fdwent))
		DistributedXactState.nparticipants_no_twophase++;
	if (fdwent->modified)
		DistributedXactState.nparticipants_modified++;

	Assert(DistributedXactState.nparticipants_no_twophase <=
			   hash_get_num_entries(DistributedXactState.participants));
	Assert(DistributedXactState.nparticipants_modified <=
		   hash_get_num_entries(DistributedXactState.participants));
}

/* Remove the foreign transaction from the current participants */
void
FdwXactUnregisterXact(UserMapping *usermapping)
{
	Assert(IsTransactionState());
	RemoveFdwXactEntry(usermapping->umid);
}

/*
 * Remove an FdwXactEntry identified by the given user mapping id from the
 * hash table.
 */
static void
RemoveFdwXactEntry(Oid umid)
{
	FdwXactEntry	*fdwent;

	Assert(DistributedXactState.participants != NULL);
	fdwent = hash_search(DistributedXactState.participants, (void *) &umid,
						 HASH_REMOVE, NULL);

	if (fdwent)
	{
		/* Update statistics */
		if (!ServerSupportTwophaseCommit(fdwent))
			DistributedXactState.nparticipants_no_twophase--;
		if (fdwent->modified)
			DistributedXactState.nparticipants_modified--;

		Assert(DistributedXactState.nparticipants_no_twophase <=
			   hash_get_num_entries(DistributedXactState.participants));
		Assert(DistributedXactState.nparticipants_modified <=
			   hash_get_num_entries(DistributedXactState.participants));
	}
}

/*
 * Commit or rollback all foreign transactions.
 */
void
AtEOXact_FdwXact(bool isCommit, bool is_parallel_worker)
{
	/* If there are no foreign servers involved, we have no business here */
	if (!HasFdwXactParticipant())
		return;

	Assert(!RecoveryInProgress());

	if (!isCommit)
	{
		HASH_SEQ_STATUS scan;
		FdwXactEntry *fdwent;

		/* Rollback foreign transactions in the participant list */
		hash_seq_init(&scan, DistributedXactState.participants);
		while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
		{
			FdwXactState	fdwxact = fdwent->fdwxact;
			int	status;

			/*
			 * If this foreign transaction is not prepared yet, end the foreign
			 * transaction in one-phase.
			 */
			if (!fdwxact)
			{
				Assert(ServerSupportTransactionCallback(fdwent));
				EndFdwXactEntry(fdwent, false, is_parallel_worker);

				/*
				 * Remove FdwXactState entry to prevent processing again in a recursive
				 * error case.
				 */
				RemoveFdwXactEntry(fdwent->umid);
				continue;
			}

			/*
			 * If the foreign transaction has FdwXactState entry, the foreign transaction
			 * might have been prepared.  We rollback the foreign transaction anyway
			 * to end the current transaction if the status is in-progress.  Since the
			 * transaction might have been already prepared on the foreign we set the
			 * status to aborting and leave it.
			 */
			SpinLockAcquire(&(fdwxact->mutex));
			status = fdwxact->status;
			fdwxact->status = FDWXACT_STATUS_ABORTING;
			SpinLockRelease(&(fdwxact->mutex));

			if (status == FDWXACT_STATUS_PREPARING)
				EndFdwXactEntry(fdwent, isCommit, is_parallel_worker);
		}
	}

	if (ForgetAllParticipants() > 0)
		FdwXactLaunchOrWakeupResolver();

	/* Reset all fields */
	DistributedXactState.all_prepared = false;
	DistributedXactState.twophase_commit_required = false;
	DistributedXactState.nparticipants_no_twophase = 0;
	DistributedXactState.nparticipants_modified = 0;
}

/*
 * The routine for committing or rolling back the given transaction participant.
 */
static void
EndFdwXactEntry(FdwXactEntry *fdwent, bool isCommit, bool is_parallel_worker)
{
	FdwXactInfo finfo;

	Assert(ServerSupportTransactionCallback(fdwent));

	finfo.server = fdwent->server;
	finfo.usermapping = fdwent->usermapping;
	finfo.flags = FDWXACT_FLAG_ONEPHASE |
		((is_parallel_worker) ? FDWXACT_FLAG_PARALLEL_WORKER : 0);
	finfo.identifier = NULL;

	if (isCommit)
	{
		fdwent->commit_foreign_xact_fn(&finfo);
		elog(DEBUG1, "successfully committed the foreign transaction for user mapping %u",
			 fdwent->umid);
	}
	else
	{
		fdwent->rollback_foreign_xact_fn(&finfo);
		elog(DEBUG1, "successfully rolled back the foreign transaction for user mapping %u",
			 fdwent->umid);
	}
}

/*
 * Prepare foreign transactions by PREPARE TRANSACTION command.
 *
 * In case where an error happens during parparing a foreign transaction we
 * change to rollback.  See AtEOXact_FdwXact() for details.
 */
void
AtPrepare_FdwXact(void)
{
	TransactionId xid;

	/* If there are no foreign servers involved, we have no business here */
	if (!HasFdwXactParticipant())
		return;

	/*
	 * Check if there is a server that doesn't support two-phase commit. All
	 * involved servers need to support two-phase commit as we're going to
	 *  prepare all of them.
	 */
	if (DistributedXactState.nparticipants_no_twophase > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a distributed transaction that has operated on a foreign server not supporting two-phase commit protocol")));

	/*
	 * Assign a transaction id if not yet because the local transaction id
	 * is used to determine the result of the distributed transaction. And
	 * prepare all foreign transactions.
	 */
	xid = GetTopTransactionId();
	FdwXactPrepareForeignTransactions(xid, true);

	/*
	 * Remember we already prepared all participants.  We keep participants
	 * until the transaction end so that we unlock the involved foreign transactions
	 * to abort in case of failure.
	 */
	DistributedXactState.all_prepared = true;
}

/*
 * Pre-commit processing for foreign transactions. We commit those foreign
 * transactions with one-phase.
 */
void
PreCommit_FdwXact(bool is_parallel_worker)
{
	HASH_SEQ_STATUS scan;
	FdwXactEntry *fdwent;
	TransactionId xid;
	bool		local_modified;

	/*
	 * If there is no foreign server involved or all foreign transactions
	 * are already prepared (see AtPrepare_FdwXact()), we have no business here.
	 */
	if (!HasFdwXactParticipant() ||
		DistributedXactState.all_prepared)
		return;

	Assert(!RecoveryInProgress());

	/*
	 * Check if the current transaction did writes.	 We need to include the
	 * local node to the distributed transaction participant and to regard it
	 * as modified, if the current transaction has performed WAL logging and
	 * has assigned an xid.	 The transaction can end up not writing any WAL,
	 * even if it has an xid, if it only wrote to temporary and/or unlogged
	 * tables.	It can end up having written WAL without an xid if did HOT
	 * pruning.
	 */
	xid = GetTopTransactionIdIfAny();
	local_modified = (TransactionIdIsValid(xid) && (XactLastRecEnd != 0));

	/*
	 * Perform twophase commit if required. Note that we don't support foreign
	 * twophase commit in single user mode.
	 */
	if (IsUnderPostmaster && checkForeignTwophaseCommitRequired(local_modified))
	{
		/*
		 * Two-phase commit is required.  Assign a transaction id to the
		 * current transaction if not yet because the local transaction is
		 * necessary to determine the result of the distributed transaction.
		 * Then we prepare foreign transactions on foreign servers that support
		 * two-phase commit.  Note that we keep FdwXactParticipants until the
		 * end of the transaction.
		 */
		if (!TransactionIdIsValid(xid))
			xid = GetTopTransactionId();
		FdwXactPrepareForeignTransactions(xid, false);
		DistributedXactState.twophase_commit_required = true;

		return;
	}

	/* Commit all foreign transactions in the participant list */
	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		Assert(ServerSupportTransactionCallback(fdwent));

		/*
		 * Commit the foreign transaction and remove itself from the hash table
		 * so that we don't try to abort already-closed transaction.
		 */
		EndFdwXactEntry(fdwent, true, is_parallel_worker);
		RemoveFdwXactEntry(fdwent->umid);
	}
}

/*
 * Return true if there is a prepared foreign transaction which matches
 * given arguments.
 */
bool
FdwXactExists(TransactionId xid, Oid umid)
{
	FdwXactState fdwxact;

	LWLockAcquire(FdwXactLock, LW_SHARED);
	fdwxact = get_fdwxact(xid, umid);
	LWLockRelease(FdwXactLock);

	return (fdwxact != NULL);
}

/*
 * We must fsync the foreign transaction state file that is valid or generated
 * during redo and has a inserted LSN <= the checkpoint's redo horizon.
 * The foreign transaction entries and hence the corresponding files are expected
 * to be very short-lived. By executing this function at the end, we might have
 * lesser files to fsync, thus reducing some I/O. This is similar to
 * CheckPointTwoPhase().
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because FdwXacts ordinarily have short lifespans, and so it is quite
 * possible that FdwXactStates that were valid at checkpoint start will no longer
 * exist if we wait a little bit. With typical checkpoint settings this
 * will be about 3 minutes for an online checkpoint, so as a result we
 * expect that there will be no FdwXactStates that need to be copied to disk.
 *
 * If a FdwXactState remains valid across multiple checkpoints, it will already
 * be on disk so we don't bother to repeat that write.
 */
void
CheckPointFdwXacts(XLogRecPtr redo_horizon)
{
	int			cnt;
	int			serialized_fdwxacts = 0;

	if (max_prepared_foreign_xacts == 0)
		return;					/* nothing to do */

	/*
	 * We are expecting there to be zero FdwXactState that need to be copied to
	 * disk, so we perform all I/O while holding FdwXactLock for simplicity.
	 * This presents any new foreign xacts from preparing while this occurs,
	 * which shouldn't be a problem since the presence of long-lived prepared
	 * foreign xacts indicated the transaction manager isn't active.
	 *
	 * It's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FdwXactState with a
	 * insert_end_lsn set prior to the last checkpoint yet is marked invalid,
	 * because of the efforts with delayChkpt.
	 */
	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (cnt = 0; cnt < FdwXactCtl->num_xacts; cnt++)
	{
		FdwXactState		fdwxact = FdwXactCtl->xacts[cnt];

		if ((fdwxact->valid || fdwxact->inredo) &&
			!fdwxact->ondisk &&
			fdwxact->insert_end_lsn <= redo_horizon)
		{
			char	   *buf;
			int			len;

			XlogReadFdwXactData(fdwxact->insert_start_lsn, &buf, &len);
			RecreateFdwXactFile(fdwxact->data.xid, fdwxact->data.umid, buf, len);
			fdwxact->ondisk = true;
			fdwxact->insert_start_lsn = InvalidXLogRecPtr;
			fdwxact->insert_end_lsn = InvalidXLogRecPtr;
			pfree(buf);
			serialized_fdwxacts++;
		}
	}

	LWLockRelease(FdwXactLock);

	/*
	 * Flush unconditionally the parent directory to make any information
	 * durable on disk.	 FdwXactState files could have been removed and those
	 * removals need to be made persistent as well as any files newly created.
	 */
	fsync_fname(FDWXACTS_DIR, true);

	if (log_checkpoints && serialized_fdwxacts > 0)
		ereport(LOG,
				(errmsg_plural("%u foreign transaction state file was written "
							   "for long-running prepared transactions",
							   "%u foreign transaction state files were written "
							   "for long-running prepared transactions",
							   serialized_fdwxacts,
							   serialized_fdwxacts)));
}

/* Return true if the current transaction needs to use two-phase commit */
bool
FdwXactIsForeignTwophaseCommitRequired(void)
{
	return DistributedXactState.twophase_commit_required;
}

/*
 * Return true if the current transaction modifies data on two or more servers
 * in FdwXactParticipants and local server itself.
 */
static bool
checkForeignTwophaseCommitRequired(bool local_modified)
{
	int		nserverswritten;

	if (!IsForeignTwophaseCommitRequested())
		return false;

	nserverswritten = DistributedXactState.nparticipants_modified;

	/* Did we modify the local non-temporary data? */
	if (local_modified)
		nserverswritten++;

	/*
	 * Two-phase commit is not required if the number of servers performing
	 * writes is less than 2.
	 */
	if (nserverswritten < 2)
		return false;

	if (DistributedXactState.nparticipants_no_twophase > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot process a distributed transaction that has operated on a foreign server that does not support two-phase commit protocol"),
				 errdetail("foreign_twophase_commit is \'required\' but the transaction has some foreign servers which are not capable of two-phase commit")));

	/* Two-phase commit is required. Check parameters */
	if (max_prepared_foreign_xacts == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign two-phase commit is required but prepared foreign transactions are disabled"),
				 errhint("Set max_prepared_foreign_transactions to a nonzero value.")));

	if (max_foreign_xact_resolvers == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign two-phase commit is required but prepared foreign transactions are disabled"),
				 errhint("Set max_foreign_transaction_resolvers to a nonzero value.")));

	return true;
}

/*
 * Insert FdwXactState entries and prepare foreign transactions.  If prepare_all is
 * true, we prepare all foreign transaction regardless of writes having happened
 * on the server.
 *
 * We still can change to rollback here on failure. If any error occurs, we
 * rollback non-prepared foreign transactions.
 */
static void
FdwXactPrepareForeignTransactions(TransactionId xid, bool prepare_all)
{
	FdwXactEntry *fdwent;
	HASH_SEQ_STATUS scan;

	Assert(TransactionIdIsValid(xid));

	/* Loop over the foreign connections */
	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		FdwXactInfo finfo;
		FdwXactState		fdwxact;
		char		*identifier;

		Assert(ServerSupportTwophaseCommit(fdwent));

		CHECK_FOR_INTERRUPTS();

		if (!prepare_all && !fdwent->modified)
			continue;

		/* Get prepared transaction identifier */
		identifier = getFdwXactIdentifier(fdwent, xid);
		Assert(identifier);

		/*
		 * Insert the foreign transaction entry with the
		 * FDWXACT_STATUS_PREPARING status. Registration persists this
		 * information to the disk and logs (that way relaying it on standby).
		 * Thus in case we loose connectivity to the foreign server or crash
		 * ourselves, we will remember that we might have prepared transaction
		 * on the foreign server and try to resolve it when connectivity is
		 * restored or after crash recovery.
		 *
		 * If we prepare the transaction on the foreign server before
		 * persisting the information to the disk and crash in-between these
		 * two steps, we will lost the prepared transaction on the foreign
		 * server and will not be able to resolve it after the crash recovery.
		 * Hence persist first then prepare.
		 */
		fdwxact = FdwXactInsertEntry(xid, fdwent, identifier);

		/*
		 * Prepare the foreign transaction.  Between FdwXactInsertEntry call till
		 * this backend hears acknowledge from foreign server, the backend may
		 * abort the local transaction (say, because of a signal).
		 */
		finfo.server = fdwent->server;
		finfo.usermapping = fdwent->usermapping;
		finfo.flags = 0;
		finfo.identifier = identifier;
		fdwent->prepare_foreign_xact_fn(&finfo);

		/* succeeded, update status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwxact->status = FDWXACT_STATUS_PREPARED;
		SpinLockRelease(&fdwxact->mutex);
	}
}

/*
 * Return a null-terminated foreign transaction identifier.  If the given FDW
 * supports getPrepareId callback we return the identifier returned from it.
 * Otherwise we generate an unique identifier with in the form of
 * "fx_<random number>_<xid>_<umid>" whose length is less than FDWXACT_ID_MAX_LEN.
 *
 * Returned string value is used to identify foreign transaction. The
 * identifier should not be same as any other concurrent prepared transaction
 * identifier.
 *
 * To make the foreign transactionid unique, we should ideally use something
 * like UUID, which gives unique ids with high probability, but that may be
 * expensive here and UUID extension which provides the function to generate
 * UUID is not part of the core code.
 */
static char *
getFdwXactIdentifier(FdwXactEntry *fdwent, TransactionId xid)
{
	char *id;
	int	id_len;

	/*
	 * If FDW doesn't provide the callback function, generate an unique
	 * identifier.
	 */
	if (!fdwent->get_prepareid_fn)
	{
		char		buf[FDWXACT_ID_MAX_LEN] = {0};

		snprintf(buf, FDWXACT_ID_MAX_LEN, "fx_%ld_%u_%u", Abs(random()),
				 xid, fdwent->umid);

		return pstrdup(buf);
	}

	/* Get an unique identifier from callback function */
	id = fdwent->get_prepareid_fn(xid, fdwent->server->serverid,
								  fdwent->usermapping->userid,
								  &id_len);

	if (id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 (errmsg("foreign transaction identifier is not provided"))));

	/* Check length of foreign transaction identifier */
	if (id_len > FDWXACT_ID_MAX_LEN)
	{
		id[FDWXACT_ID_MAX_LEN] = '\0';
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("foreign transaction identifier \"%s\" is too long",
						id),
				 errdetail("Foreign transaction identifier must be less than %d characters.",
						   FDWXACT_ID_MAX_LEN)));
	}

	id[id_len] = '\0';
	return pstrdup(id);
}

/*
 * This function insert a new FdwXactState entry to the global array with
 * WAL-logging. The new entry is held by the backend who inserted.
 */
static FdwXactState
FdwXactInsertEntry(TransactionId xid, FdwXactEntry *fdwent,
				   char *identifier)
{
	FdwXactStateOnDiskData *fdwxact_file_data;
	FdwXactState		fdwxact;
	Oid			owner;
	int			data_len;

	/* on first call, register the exit hook */
	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FdwXact, 0);
		fdwXactExitRegistered = true;
	}

	/*
	 * Enter the foreign transaction into the shared memory structure.
	 */
	owner = GetUserId();
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdwxact = insert_fdwxact(MyDatabaseId, xid, fdwent->umid,
							 fdwent->usermapping->serverid, owner, identifier);
	fdwxact->locking_backend = MyBackendId;
	LWLockRelease(FdwXactLock);

	fdwent->fdwxact = fdwxact;

	/*
	 * Prepare to write the entry to a file. Also add xlog entry. The contents
	 * of the xlog record are same as what is written to the file.
	 */
	data_len = offsetof(FdwXactStateOnDiskData, identifier);
	data_len = data_len + strlen(identifier) + 1;
	data_len = MAXALIGN(data_len);
	fdwxact_file_data = (FdwXactStateOnDiskData *) palloc0(data_len);
	memcpy(fdwxact_file_data, &(fdwxact->data), data_len);

	/* See note in RecordTransactionCommit */
	MyProc->delayChkpt = true;

	START_CRIT_SECTION();

	/* Add the entry in the xlog and save LSN for checkpointer */
	XLogBeginInsert();
	XLogRegisterData((char *) fdwxact_file_data, data_len);
	fdwxact->insert_end_lsn = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_INSERT);
	XLogFlush(fdwxact->insert_end_lsn);

	/* If we crash now, we have prepared: WAL replay will fix things */

	/* Store record's start location to read that later on CheckPoint */
	fdwxact->insert_start_lsn = ProcLastRecPtr;

	/* File is written completely, checkpoint can proceed with syncing */
	fdwxact->valid = true;

	/* Checkpoint can process now */
	MyProc->delayChkpt = false;

	END_CRIT_SECTION();

	pfree(fdwxact_file_data);
	return fdwxact;
}

/*
 * Insert a new entry for a given foreign transaction identified by transaction
 * id, foreign server and user mapping, into the shared memory array. Caller
 * must hold FdwXactLock in exclusive mode.
 *
 * If the entry already exists, the function raises an error.
 */
static FdwXactState
insert_fdwxact(Oid dbid, TransactionId xid, Oid umid, Oid serverid, Oid owner,
			   char *identifier)
{
	FdwXactState		fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Check for duplicated foreign transaction entry */
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		fdwxact = FdwXactCtl->xacts[i];
		if (fdwxact->valid &&
			fdwxact->data.xid == xid &&
			fdwxact->data.umid == umid)
			ereport(ERROR,
					(errmsg("could not insert a foreign transaction entry"),
					 errdetail("Duplicate entry with transaction id %u, user mapping id %u exists.",
							   xid, umid)));
	}

	/*
	 * Get a next free foreign transaction entry. Raise error if there are
	 * none left.
	 */
	if (!FdwXactCtl->free_fdwxacts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of foreign transactions reached"),
				 errhint("Increase max_prepared_foreign_transactions: \"%d\".",
						 max_prepared_foreign_xacts)));
	}
	fdwxact = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact->fdwxact_free_next;

	/* Insert the entry to shared memory array */
	Assert(FdwXactCtl->num_xacts < max_prepared_foreign_xacts);
	FdwXactCtl->xacts[FdwXactCtl->num_xacts++] = fdwxact;

	fdwxact->status = FDWXACT_STATUS_PREPARING;
	fdwxact->data.xid = xid;
	fdwxact->data.dbid = dbid;
	fdwxact->data.umid = umid;
	fdwxact->data.serverid = serverid;
	fdwxact->data.owner = owner;
	strlcpy(fdwxact->data.identifier, identifier, FDWXACT_ID_MAX_LEN);

	fdwxact->insert_start_lsn = InvalidXLogRecPtr;
	fdwxact->insert_end_lsn = InvalidXLogRecPtr;
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;

	return fdwxact;
}

/*
 * Remove the foreign prepared transaction entry from shared memory.
 * Caller must hold FdwXactLock in exclusive mode.
 */
static void
remove_fdwxact(FdwXactState fdwxact)
{
	int			i;

	Assert(fdwxact != NULL);
	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Search the slot where this entry resided */
	for (i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		if (FdwXactCtl->xacts[i] == fdwxact)
			break;
	}

	if (i >= FdwXactCtl->num_xacts)
		elog(ERROR, "failed to find %p in FdwXactState array", fdwxact);

	elog(DEBUG2, "remove fdwxact entry id %s", fdwxact->data.identifier);

	/* Remove the entry from active array */
	FdwXactCtl->num_xacts--;
	FdwXactCtl->xacts[i] = FdwXactCtl->xacts[FdwXactCtl->num_xacts];

	/* Put it back into free list */
	fdwxact->fdwxact_free_next = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact;

	/* Reset informations */
	fdwxact->status = FDWXACT_STATUS_INVALID;
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;

	if (!RecoveryInProgress())
	{
		xl_fdwxact_remove record;
		XLogRecPtr	recptr;

		/* Fill up the log record before releasing the entry */
		record.xid = fdwxact->data.xid;
		record.umid = fdwxact->data.umid;

		/*
		 * Now writing FdwXactState data to WAL. We have to set delayChkpt
		 * here, otherwise a checkpoint starting immediately after the WAL
		 * record is inserted could complete without fsync'ing our state file.
		 * (This is essentially the same kind of race condition as the
		 * COMMIT-to-clog-write case that RecordTransactionCommit uses
		 * delayChkpt for; see notes there.)
		 */
		START_CRIT_SECTION();

		MyProc->delayChkpt = true;

		/*
		 * Log that we are removing the foreign transaction entry and remove
		 * the file from the disk as well.
		 */
		XLogBeginInsert();
		XLogRegisterData((char *) &record, sizeof(xl_fdwxact_remove));
		recptr = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_REMOVE);
		XLogFlush(recptr);

		/* Now we can mark ourselves as out of the commit critical section */
		MyProc->delayChkpt = false;

		END_CRIT_SECTION();
	}
}

/*
 * When the process exits, forget all the entries.
 */
static void
AtProcExit_FdwXact(int code, Datum arg)
{
	if (ForgetAllParticipants() > 0)
		FdwXactLaunchOrWakeupResolver();
}

/*
 * Unlock all foreign transaction participants.  If we left foreign transaction,
 * update the oldest xmin of unresolved transaction to prevent the local
 * transaction id of such unresolved foreign transaction from begin truncated.
 * Returns the number of remaining foreign transactions.
 */
static int
ForgetAllParticipants(void)
{
	FdwXactEntry *fdwent;
	HASH_SEQ_STATUS scan;
	int	nremaining = 0;

	if (!HasFdwXactParticipant())
		return nremaining;

	hash_seq_init(&scan, DistributedXactState.participants);
	while ((fdwent = (FdwXactEntry *) hash_seq_search(&scan)))
	{
		FdwXactState		fdwxact = fdwent->fdwxact;

		if (fdwxact)
		{
			Assert(fdwxact->locking_backend == MyBackendId);

			/* Unlock the foreign transaction entry */
			LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
			fdwxact->locking_backend = InvalidBackendId;
			LWLockRelease(FdwXactLock);

			nremaining++;
		}

		/* Remove from the participants list */
		RemoveFdwXactEntry(fdwent->umid);
	}

	/*
	 * If we leave any FdwXactState entries, update the oldest local transaction of
	 * unresolved distributed transaction.
	 */
	if (nremaining > 0)
	{
		elog(DEBUG1, "%u foreign transactions remaining", nremaining);
		FdwXactComputeRequiredXmin();
	}

	Assert(!HasFdwXactParticipant());
	return nremaining;
}

/*
 * Commit or rollback one prepared foreign transaction, and remove FdwXactState
 * entry.
 */
void
ResolveOneFdwXact(FdwXactState fdwxact)
{
	FdwXactInfo finfo;
	FdwRoutine *routine;

	/* The FdwXactState entry must be held by me */
	Assert(fdwxact != NULL);
	Assert(fdwxact->locking_backend == MyBackendId);
	Assert(fdwxact->status == FDWXACT_STATUS_PREPARED ||
		   fdwxact->status == FDWXACT_STATUS_COMMITTING ||
		   fdwxact->status == FDWXACT_STATUS_ABORTING);

	/* Set whether we do commit or abort if not set yet */
	if (fdwxact->status == FDWXACT_STATUS_PREPARED)
	{
		FdwXactStatus new_status;

		new_status = FdwXactGetTransactionFate(fdwxact->data.xid);
		Assert(new_status == FDWXACT_STATUS_COMMITTING ||
			   new_status == FDWXACT_STATUS_ABORTING);

		/* Update the status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwxact->status = new_status;
		SpinLockRelease(&fdwxact->mutex);
	}

	routine = GetFdwRoutineByServerId(fdwxact->data.serverid);

	/* Prepare the foreign transaction information to pass to API */
	finfo.server = GetForeignServer(fdwxact->data.serverid);
	finfo.usermapping = GetUserMapping(fdwxact->data.owner, fdwxact->data.serverid);
	finfo.flags = 0;
	finfo.identifier = fdwxact->data.identifier;

	if (fdwxact->status == FDWXACT_STATUS_COMMITTING)
	{
		routine->CommitForeignTransaction(&finfo);
		elog(DEBUG1, "successfully committed the prepared foreign transaction %s",
			 fdwxact->data.identifier);
	}
	else
	{
		routine->RollbackForeignTransaction(&finfo);
		elog(DEBUG1, "successfully rolled back the prepared foreign transaction %s",
			 fdwxact->data.identifier);
	}

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	if (fdwxact->ondisk)
		RemoveFdwXactStateFile(fdwxact->data.xid, fdwxact->data.umid, true);
	remove_fdwxact(fdwxact);
	LWLockRelease(FdwXactLock);
}

/*
 * Compute the oldest xmin across all unresolved foreign transactions
 * and store it in the ProcArray.
 */
static void
FdwXactComputeRequiredXmin(void)
{
	TransactionId agg_xmin = InvalidTransactionId;

	Assert(FdwXactCtl != NULL);

	LWLockAcquire(FdwXactLock, LW_SHARED);

	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState		fdwxact = FdwXactCtl->xacts[i];

		if (!fdwxact->valid)
			continue;

		Assert(TransactionIdIsValid(fdwxact->data.xid));

		/*
		 * We can exclude entries that are marked as either committing or
		 * aborting and its state file is on disk since such entries
		 * no longer need to lookup its transaction status from the commit
		 * log.
		 */
		if (!TransactionIdIsValid(agg_xmin) ||
			TransactionIdPrecedes(fdwxact->data.xid, agg_xmin) ||
			(fdwxact->ondisk &&
			 (fdwxact->status == FDWXACT_STATUS_COMMITTING ||
			  fdwxact->status == FDWXACT_STATUS_ABORTING)))
			agg_xmin = fdwxact->data.xid;
	}

	LWLockRelease(FdwXactLock);

	ProcArraySetFdwXactUnresolvedXmin(agg_xmin);
}


/*
 * Return whether the foreign transaction associated with the given transaction
 * id should be committed or rolled back according to the result of the local
 * transaction.
 */
static FdwXactStatus
FdwXactGetTransactionFate(TransactionId xid)
{
	/*
	 * If the local transaction is already committed, commit prepared foreign
	 * transaction.
	 */
	if (TransactionIdDidCommit(xid))
		return FDWXACT_STATUS_COMMITTING;

	/*
	 * If the local transaction is already aborted, abort prepared foreign
	 * transactions.
	 */
	else if (TransactionIdDidAbort(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The local transaction is not in progress but the foreign transaction is
	 * not prepared on the foreign server. This can happen when transaction
	 * failed after registered this entry but before actual preparing on the
	 * foreign server. So let's assume it aborted.
	 */
	else if (!TransactionIdIsInProgress(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The Local transaction is in progress and foreign transaction is about
	 * to be committed or aborted.	Raise an error anyway since we cannot
	 * determine the fate of this foreign transaction according to the local
	 * transaction whose fate is also not determined.
	 */
	elog(ERROR,
		 "cannot resolve the foreign transaction associated with in-process transaction");

	pg_unreachable();
}


/*
 * Recreates a foreign transaction state file. This is used in WAL replay
 * and during checkpoint creation.
 *
 * Note: content and len don't include CRC.
 */
void
RecreateFdwXactFile(TransactionId xid, Oid umid, void *content, int len)
{
	char		path[MAXPGPATH];
	pg_crc32c	statefile_crc;
	int			fd;

	/* Recompute CRC */
	INIT_CRC32C(statefile_crc);
	COMP_CRC32C(statefile_crc, content, len);
	FIN_CRC32C(statefile_crc);

	FdwXactStateFilePath(path, xid, umid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not recreate foreign transaction state file \"%s\": %m",
						path)));

	/* Write content and CRC */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_WRITE);
	if (write(fd, content, len) != len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write foreign transaction state file: %m")));
	}
	if (write(fd, &statefile_crc, sizeof(pg_crc32c)) != sizeof(pg_crc32c))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write foreign transaction state file: %m")));
	}
	pgstat_report_wait_end();

	/*
	 * We must fsync the file because the end-of-replay checkpoint will not do
	 * so, there being no FDWXACT in shared memory yet to tell it to.
	 */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync foreign transaction state file: %m")));
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close foreign transaction file: %m")));
}


/* Apply the redo log for a foreign transaction */
void
fdwxact_redo(XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDWXACT_INSERT)
	{
		/*
		 * Add fdwxact entry and set start/end lsn of the WAL record in
		 * FdwXactState entry.
		 */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoAdd(XLogRecGetData(record), record->ReadRecPtr,
					   record->EndRecPtr);
		LWLockRelease(FdwXactLock);
	}
	else if (info == XLOG_FDWXACT_REMOVE)
	{
		xl_fdwxact_remove *record = (xl_fdwxact_remove *) rec;

		/* Delete FdwXactState entry and file if exists */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoRemove(record->xid, record->umid, false);
		LWLockRelease(FdwXactLock);
	}
	else
		elog(ERROR, "invalid log type %d in foreign transaction log record", info);

	return;
}

/*
 * Scan the shared memory entries of FdwXactState and determine the range of valid
 * XIDs present.  This is run during database startup, after we have completed
 * reading WAL.	 ShmemVariableCache->nextXid has been set to one more than
 * the highest XID for which evidence exists in WAL.

 * On corrupted two-phase files, fail immediately.	Keeping around broken
 * entries and let replay continue causes harm on the system, and a new
 * backup should be rolled in.

 * Our other responsibility is to update and return the oldest valid XID
 * among the distributed transactions. This is needed to synchronize pg_subtrans
 * startup properly.
 */
TransactionId
PrescanFdwXacts(TransactionId oldestActiveXid)
{
	FullTransactionId nextXid = ShmemVariableCache->nextXid;
	TransactionId origNextXid = XidFromFullTransactionId(nextXid);
	TransactionId result = origNextXid;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState		fdwxact = FdwXactCtl->xacts[i];
		char	   *buf;

		buf = ProcessFdwXactBuffer(fdwxact->data.xid, fdwxact->data.umid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		if (TransactionIdPrecedes(fdwxact->data.xid, result))
			result = fdwxact->data.xid;

		pfree(buf);
	}
	LWLockRelease(FdwXactLock);

	return result;
}

/*
 * Scan pg_fdwxact and fill FdwXactState depending on the on-disk data.
 * This is called once at the beginning of recovery, saving any extra
 * lookups in the future.  FdwXactState files that are newer than the
 * minimum XID horizon are discarded on the way.
 */
void
RestoreFdwXactData(void)
{
	DIR		  *cldir;
	struct dirent *clde;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	cldir = AllocateDir(FDWXACTS_DIR);
	while ((clde = ReadDir(cldir, FDWXACTS_DIR)) != NULL)
	{
		if (strlen(clde->d_name) == FDWXACT_FILE_NAME_LEN &&
			strspn(clde->d_name, "0123456789ABCDEF_") == FDWXACT_FILE_NAME_LEN)
		{
			TransactionId xid;
			Oid		   umid;
			char		  *buf;

			sscanf(clde->d_name, "%08x_%08x", &xid, &umid);

			/* Read fdwxact data from disk */
			buf = ProcessFdwXactBuffer(xid, umid, InvalidXLogRecPtr,
									   true);
			if (buf == NULL)
				continue;

			/* Add this entry into the table of foreign transactions */
			FdwXactRedoAdd(buf, InvalidXLogRecPtr, InvalidXLogRecPtr);
		}
	}

	LWLockRelease(FdwXactLock);
	FreeDir(cldir);
}

/*
 * Scan the shared memory entries of FdwXactState and valid them.
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 */
void
RecoverFdwXacts(void)
{
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState		fdwxact = FdwXactCtl->xacts[i];
		char	   *buf;

		buf = ProcessFdwXactBuffer(fdwxact->data.xid, fdwxact->data.umid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		ereport(LOG,
				(errmsg("recovering foreign prepared transaction %s from shared memory",
						fdwxact->data.identifier)));

		/* recovered, so reset the flag for entries generated by redo */
		fdwxact->inredo = false;
		fdwxact->valid = true;
		pfree(buf);
	}
	LWLockRelease(FdwXactLock);
}

/*
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdwxact entry in shared memory FdwXactData structure.
 */
static void
FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	FdwXactStateOnDiskData *fdwxact_data = (FdwXactStateOnDiskData *) buf;
	FdwXactState		fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	/*
	 * Add this entry into the table of foreign transactions. The status of
	 * the transaction is set as preparing, since we do not know the exact
	 * status right now. Resolver will set it later based on the status of
	 * local transaction which prepared this foreign transaction.
	 */
	fdwxact = insert_fdwxact(fdwxact_data->dbid, fdwxact_data->xid,
							 fdwxact_data->umid, fdwxact_data->serverid,
							 fdwxact_data->owner, fdwxact_data->identifier);

	elog(DEBUG2, "added fdwxact entry in shared memory for foreign transaction, db %u xid %u user mapping %u owner %u id %s",
		 fdwxact_data->dbid, fdwxact_data->xid,
		 fdwxact_data->umid, fdwxact_data->owner,
		 fdwxact_data->identifier);

	/*
	 * Set status as PREPARED, since we do not know the xact status right now.
	 * We will set it later based on the status of local transaction that
	 * prepared this fdwxact entry.
	 */
	fdwxact->status = FDWXACT_STATUS_PREPARED;
	fdwxact->insert_start_lsn = start_lsn;
	fdwxact->insert_end_lsn = end_lsn;
	fdwxact->inredo = true;		/* added in redo */
	fdwxact->valid = false;
	fdwxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
}

/*
 * Remove the corresponding fdwxact entry from FdwXactCtl. Also remove
 * FdwXactState file if a foreign transaction was saved via an earlier checkpoint.
 * We could not found the FdwXactState entry in the case where a crash recovery
 * starts from the point where is after added but before removed the entry.
 */
static void
FdwXactRedoRemove(TransactionId xid, Oid umid, bool givewarning)
{
	FdwXactState		fdwxact;
	int			i;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	for (i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		fdwxact = FdwXactCtl->xacts[i];

		if (fdwxact->data.xid == xid && fdwxact->data.umid == umid)
			break;
	}

	if (i >= FdwXactCtl->num_xacts)
		return;

	/* Clean up entry and any files we may have left */
	if (fdwxact->ondisk)
		RemoveFdwXactStateFile(fdwxact->data.xid, fdwxact->data.umid, givewarning);
	remove_fdwxact(fdwxact);

	elog(DEBUG2, "removed fdwxact entry from shared memory for foreign transaction %s",
		 fdwxact->data.identifier);
}

/*
 * Reads foreign transaction data from xlog. During checkpoint this data will
 * be moved to fdwxact files and ReadFdwXactStateFile should be used instead.
 *
 * Note clearly that this function accesses WAL during normal operation, similarly
 * to the way WALSender or Logical Decoding would do. It does not run during
 * crash recovery or standby processing.
 */
static void
XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	TimeLineID	save_currtli = ThisTimeLineID;

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating an XLog reading processor.")));

	XLogBeginRead(xlogreader, lsn);
	record = XLogReadRecord(xlogreader, &errormsg);

	/*
	 * Restore immediately the timeline where it was previously, as
	 * read_local_xlog_page() could have changed it if the record was read
	 * while recovery was finishing or if the timeline has jumped in-between.
	 */
	ThisTimeLineID = save_currtli;

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read foreign transaction state from xlog at %X/%X",
						(uint32) (lsn >> 32),
						(uint32) lsn)));

	if (XLogRecGetRmid(xlogreader) != RM_FDWXACT_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FDWXACT_INSERT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected foreign transaction state data is not present in xlog at %X/%X",
						(uint32) (lsn >> 32),
						(uint32) lsn)));

	if (len != NULL)
		*len = XLogRecGetDataLen(xlogreader);

	*buf = palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
	memcpy(*buf, XLogRecGetData(xlogreader), sizeof(char) * XLogRecGetDataLen(xlogreader));

	XLogReaderFree(xlogreader);
}

/*
 * Given a transaction id, userid and serverid read it either from disk
 * or read it directly via shmem xlog record pointer using the provided
 * "insert_start_lsn".
 */
static char *
ProcessFdwXactBuffer(TransactionId xid, Oid umid, XLogRecPtr insert_start_lsn,
					 bool fromdisk)
{
	TransactionId origNextXid =
	XidFromFullTransactionId(ShmemVariableCache->nextXid);
	char	   *buf;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	if (!fromdisk)
		Assert(!XLogRecPtrIsInvalid(insert_start_lsn));

	/* Reject XID if too new */
	if (TransactionIdFollowsOrEquals(xid, origNextXid))
	{
		if (fromdisk)
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state file for xid %u and user mapping %u",
							xid, umid)));
			RemoveFdwXactStateFile(xid, umid, true);
		}
		else
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state from memory for xid %u and user mapping %u",
							xid, umid)));
			FdwXactRedoRemove(xid, umid, true);
		}
		return NULL;
	}

	if (fromdisk)
	{
		/* Read and validate file */
		buf = ReadFdwXactStateFile(xid, umid);
	}
	else
	{
		/* Read xlog data */
		XlogReadFdwXactData(insert_start_lsn, &buf, NULL);
	}

	return buf;
}

/*
 * Read and validate the foreign transaction state file.
 *
 * If it looks OK (has a valid magic number and CRC), return the palloc'd
 * contents of the file, issuing an error when finding corrupted data.
 * This state can be reached when doing recovery.
 */
static char *
ReadFdwXactStateFile(TransactionId xid, Oid umid)
{
	char		path[MAXPGPATH];
	int			fd;
	FdwXactStateOnDiskData *fdwxact_file_data;
	struct stat stat;
	uint32		crc_offset;
	pg_crc32c	calc_crc;
	pg_crc32c	file_crc;
	char	   *buf;
	int			r;

	FdwXactStateFilePath(path, xid, umid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open FDW transaction state file \"%s\": %m",
						path)));

	/*
	 * Check file length.  We can determine a lower bound pretty easily. We
	 * set an upper bound to avoid palloc() failure on a corrupt file, though
	 * we can't guarantee that we won't get an out of memory error anyway,
	 * even on a valid file.
	 */
	if (fstat(fd, &stat))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat FDW transaction state file \"%s\": %m",
						path)));

	if (stat.st_size < (offsetof(FdwXactStateOnDiskData, identifier) +
						sizeof(pg_crc32c)) ||
		stat.st_size > MaxAllocSize)

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("too large FDW transaction state file \"%s\": %m",
						path)));

	crc_offset = stat.st_size - sizeof(pg_crc32c);
	if (crc_offset != MAXALIGN(crc_offset))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect alignment of CRC offset for file \"%s\"",
						path)));

	/*
	 * Ok, slurp in the file.
	 */
	buf = (char *) palloc(stat.st_size);
	fdwxact_file_data = (FdwXactStateOnDiskData *) buf;

	/* Slurp the file */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_READ);
	r = read(fd, buf, stat.st_size);
	if (r != stat.st_size)
	{
		if (r < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		else
			ereport(ERROR,
					(errmsg("could not read file \"%s\": read %d of %zu",
							path, r, (Size) stat.st_size)));
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	/*
	 * Check the CRC.
	 */
	INIT_CRC32C(calc_crc);
	COMP_CRC32C(calc_crc, buf, crc_offset);
	FIN_CRC32C(calc_crc);

	file_crc = *((pg_crc32c *) (buf + crc_offset));

	if (!EQ_CRC32C(calc_crc, file_crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						path)));

	/* Check if the contents is an expected data */
	fdwxact_file_data = (FdwXactStateOnDiskData *) buf;
	if (fdwxact_file_data->xid != xid ||
		fdwxact_file_data->umid != umid)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid foreign transaction state file \"%s\"",
						path)));

	return buf;
}

/*
 * Remove the foreign transaction file for given entry.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
static void
RemoveFdwXactStateFile(TransactionId xid, Oid umid, bool giveWarning)
{
	char		path[MAXPGPATH];

	FdwXactStateFilePath(path, xid, umid);
	if (unlink(path) < 0 && (errno != ENOENT || giveWarning))
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove foreign transaction state file \"%s\": %m",
						path)));
}

/*
 * Return the FdwXactState that matched to given arguments. Otherwise return NULL.
 * The search condition is defined by arguments with valid values for respective
 * datatypes. The caller must hold FdwXactLock.
 */
static FdwXactState
get_fdwxact(TransactionId xid, Oid umid)
{
	FdwXactState fdwxact;

	Assert(LWLockHeldByMe(FdwXactLock));

	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		fdwxact = FdwXactCtl->xacts[i];

		if (!fdwxact->valid)
			continue;

		/* xid */
		if (TransactionIdIsValid(xid) && xid != fdwxact->data.xid)
			continue;

		/* umid */
		if (OidIsValid(umid) && umid != fdwxact->data.umid)
			continue;

		/* This entry matches the condition */
		return fdwxact;
	}

	return NULL;
}

/*
 * Get FdwXact entry and do some sanity checks. If check_two_phase is true, we also
 * check if the given xid is prepared.  The caller must hold FdwXactLock.
 */
static FdwXactState
get_fdwxact_with_check(TransactionId xid, Oid umid, bool check_two_phase)
{
	FdwXactState		fdwxact;
	Oid			myuserid;

	Assert(LWLockHeldByMe(FdwXactLock));

	fdwxact = get_fdwxact(xid, umid);

	if (fdwxact->data.dbid != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction belongs to another database"),
				 errhint("Connect to the database where the transaction was created to finish it.")));

	/* permission check */
	myuserid = GetUserId();
	if (myuserid != fdwxact->data.owner && !superuser_arg(myuserid))
		ereport(ERROR,
				 (errmsg("permission denied to resolve prepared foreign transaction"),
				  errhint("Must be superuser or the user that prepared the transaction")));

	/* check if the entry is being processed by someone */
	if (fdwxact->locking_backend != InvalidBackendId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction with transaction identifier \"%s\" is busy",
						fdwxact->data.identifier)));

	if (check_two_phase && TwoPhaseExists(fdwxact->data.xid))
	{
		/*
		 * the entry's local transaction is prepared. Since we cannot know the
		 * fate of the local transaction, we cannot resolve this foreign
		 * transaction.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot resolve foreign transaction with identifier \"%s\" whose local transaction is in-progress",
						fdwxact->data.identifier),
				 errhint("Do COMMIT PREPARED or ROLLBACK PREPARED")));
	}

	return fdwxact;
}

/* Error cleanup callback for pg_foreign_resolve/remove_xact */
static void
pg_foreign_xact_callback(int code, Datum arg)
{
	FdwXactState fdwxact = (FdwXactState) DatumGetPointer(arg);

	if (fdwxact->valid)
	{
		Assert(fdwxact->locking_backend == MyBackendId);

		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		fdwxact->locking_backend = InvalidBackendId;
		LWLockRelease(FdwXactLock);
	}
}

/* Built in functions */

/*
 * Structure to hold and iterate over the foreign transactions to be displayed
 * by the built-in functions.
 */
typedef struct
{
	FdwXactState		fdwxacts;
	int			num_xacts;
	int			cur_xact;
}			WorkingStatus;

Datum
pg_foreign_xacts(PG_FUNCTION_ARGS)
{
#define PG_PREPARED_FDWXACTS_COLS	6
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (int i = 0; i < FdwXactCtl->num_xacts; i++)
	{
		FdwXactState		fdwxact = FdwXactCtl->xacts[i];
		FdwXactStatus status;
		char	   *xact_status;
		Datum		values[PG_PREPARED_FDWXACTS_COLS];
		bool		nulls[PG_PREPARED_FDWXACTS_COLS];

		if (!fdwxact->valid)
			continue;

		memset(nulls, 0, sizeof(nulls));

		SpinLockAcquire(&fdwxact->mutex);
		status = fdwxact->status;
		SpinLockRelease(&fdwxact->mutex);

		values[0] = TransactionIdGetDatum(fdwxact->data.xid);
		values[1] = ObjectIdGetDatum(fdwxact->data.umid);
		values[2] = ObjectIdGetDatum(fdwxact->data.owner);

		switch (status)
		{
			case FDWXACT_STATUS_PREPARING:
				xact_status = "preparing";
				break;
			case FDWXACT_STATUS_PREPARED:
				xact_status = "prepared";
				break;
			case FDWXACT_STATUS_COMMITTING:
				xact_status = "committing";
				break;
			case FDWXACT_STATUS_ABORTING:
				xact_status = "aborting";
				break;
			default:
				xact_status = "unknown";
				break;
		}

		values[3] = CStringGetTextDatum(xact_status);
		values[4] = CStringGetTextDatum(fdwxact->data.identifier);

		if (fdwxact->locking_backend != InvalidBackendId)
		{
			PGPROC *locker = BackendIdGetProc(fdwxact->locking_backend);
			values[5] = Int32GetDatum(locker->pid);
		}
		else
			nulls[5] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(FdwXactLock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Built-in SQL function to resolve a prepared foreign transaction.
 */
Datum
pg_resolve_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid			umid = PG_GETARG_OID(1);
	FdwXactState	fdwxact;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_fdwxact_with_check(xid, umid, true);

	/* lock it */
	fdwxact->locking_backend = MyBackendId;

	LWLockRelease(FdwXactLock);

	/*
	 * Resolve the foreign transaction.  We ensure unlocking FdwXact entry
	 * at an error or an interruption.
	 *
	 * XXX we assume that an interruption doesn't happen between locking
	 * FdwXact entry and registering the callback, especially in
	 * LWLockRelease().
	 */
	PG_ENSURE_ERROR_CLEANUP(pg_foreign_xact_callback,
							(Datum) PointerGetDatum(fdwxact));
	{
		ResolveOneFdwXact(fdwxact);
	}
	PG_END_ENSURE_ERROR_CLEANUP(pg_foreign_xact_callback,
								(Datum) PointerGetDatum(fdwxact));

	PG_RETURN_BOOL(true);
}

/*
 * Built-in function to remove a prepared foreign transaction entry without
 * resolution. The function gives a way to forget about such prepared
 * transaction in case: the foreign server where it is prepared is no longer
 * available, the user which prepared this transaction needs to be dropped.
 */
Datum
pg_remove_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid		umid = PG_GETARG_OID(1);
	FdwXactState	fdwxact;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_fdwxact_with_check(xid, umid, false);

	/* Clean up entry and any files we may have left */
	if (fdwxact->ondisk)
		RemoveFdwXactStateFile(fdwxact->data.xid, fdwxact->data.umid, true);
	remove_fdwxact(fdwxact);

	LWLockRelease(FdwXactLock);

	PG_RETURN_BOOL(true);
}
