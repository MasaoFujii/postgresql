/*
 * fdwxact.h
 *
 * PostgreSQL global transaction manager
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "access/xact.h"
#include "foreign/foreign.h"

/* Function declarations */
extern void AtEOXact_FdwXact(bool isCommit);
extern void AtPrepare_FdwXact(void);

#endif /* FDWXACT_H */
