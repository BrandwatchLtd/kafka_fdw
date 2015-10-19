/*-------------------------------------------------------------------------
 *
 * kafka_fdw.c
 *        foreign-data wrapper for access to Apache Kafka
 *
 * TODO: Copyright here
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct KafkaFdwOption
{
    const char *optname;
    Oid         optcontext;     /* Oid of catalog in which option may appear */
};

/*
 * Valid options for kafka_fdw.
 */
static const struct KafkaFdwOption valid_options[] = {
    /* Server-level options */
    {"host", ForeignServerRelationId},
    {"port", ForeignServerRelationId},
    /* Table options */
    {"topic", ForeignTableRelationId}
    {"offset", ForeignTableRelationId}
    /* Sentinel */
    {NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct KafkaFdwExecutionState
{
    char       *topic;
    List       *options;
} KafkaFdwExecutionState;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kafka_fdw_handler);
PG_FUNCTION_INFO_V1(kafka_fdw_validator);

/*
 * FDW callback routines
 */
static void kafkaGetForeignRelSize(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid
    );
static void kafkaGetForeignPaths(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid
    );
static ForeignScan *kafkaGetForeignPlan(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses
    );
static void kafkaExplainForeignScan(
        ForeignScanState *node,
        ExplainState *es
    );
static void kafkaBeginForeignScan(
        ForeignScanState *node,
        int eflags
    );
static TupleTableSlot *kafkaIterateForeignScan(
        ForeignScanState *node
    );
static void kafkaReScanForeignScan(
        ForeignScanState *node
    );
static void kafkaEndForeignScan(
        ForeignScanState *node
    );
static bool kafkaAnalyzeForeignTable(
        Relation relation,
        AcquireSampleRowsFunc *func,
        BlockNumber *totalpages
    );

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
kafka_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize   = NULL;
    fdwroutine->GetForeignPaths     = kafkaGetForeignPaths;
    fdwroutine->GetForeignPlan      = NULL;
    fdwroutine->ExplainForeignScan  = NULL;
    fdwroutine->BeginForeignScan    = kafkaBeginForeignScan;
    fdwroutine->IterateForeignScan  = NULL;
    fdwroutine->ReScanForeignScan   = NULL;
    fdwroutine->EndForeignScan      = kafkaEndForeignScan;
    fdwroutine->AnalyzeForeignTable = NULL;

    PG_RETURN_POINTER(fdwroutine);
}

