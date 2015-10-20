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
#include "catalog/pg_foreign_server.h"
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
    {"topic", ForeignTableRelationId},
    {"offset", ForeignTableRelationId},
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

static void estimate_costs(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Cost *startup_cost,
        Cost *total_cost
    );

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
kafka_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize   = kafkaGetForeignRelSize;
    fdwroutine->GetForeignPaths     = kafkaGetForeignPaths;
    fdwroutine->GetForeignPlan      = kafkaGetForeignPlan;
    fdwroutine->ExplainForeignScan  = NULL;
    fdwroutine->BeginForeignScan    = kafkaBeginForeignScan;
    fdwroutine->IterateForeignScan  = kafkaIterateForeignScan;
    fdwroutine->ReScanForeignScan   = kafkaReScanForeignScan;
    fdwroutine->EndForeignScan      = kafkaEndForeignScan;
    fdwroutine->AnalyzeForeignTable = NULL;

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Obtain relation size estimates for a foreign table. This is called at the
 * beginning of planning for a query that scans a foreign table. root is the
 * planner's global information about the query; baserel is the planner's
 * information about this table; and foreigntableid is the pg_class OID of the
 * foreign table. (foreigntableid could be obtained from the planner data
 * structures, but it's passed explicitly to save effort.)
 *
 * This function should update baserel->rows to be the expected number of rows
 * returned by the table scan, after accounting for the filtering done by the
 * restriction quals. The initial value of baserel->rows is just a constant
 * default estimate, which should be replaced if at all possible. The function
 * may also choose to update baserel->width if it can compute a better estimate
 * of the average result row width.
 *
 * See Section 53.4 for additional information.
 */
static void kafkaGetForeignRelSize(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid
    ) {
    // NOOP

    // The base estimate is the best available.
    // Making a kafka request to determine the number of values available is
    // excessive given that rows of a single unindexed column are going to be
    // returned.
}

/*
 * Create possible access paths for a scan on a foreign table. This is called
 * during query planning. The parameters are the same as for GetForeignRelSize,
 * which has already been called.
 *
 * This function must generate at least one access path (ForeignPath node) for
 * a scan on the foreign table and must call add_path to add each such path to
 * baserel->pathlist. It's recommended to use create_foreignscan_path to build
 * the ForeignPath nodes. The function can generate multiple access paths,
 * e.g., a path which has valid pathkeys to represent a pre-sorted result. Each
 * access path must contain cost estimates, and can contain any FDW-private
 * information that is needed to identify the specific scan method intended.
 *
 * See Section 53.4 for additional information.
 */
static void kafkaGetForeignPaths(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid
    ) {
    Cost startup_cost;
    Cost total_cost;
    Path *path;

    estimate_costs(root, baserel, &startup_cost, &total_cost);
    path = (Path *)create_foreignscan_path(
           root,
           baserel,
           baserel->rows,
           startup_cost,
           total_cost,
           NIL,
           NULL,
           NIL
        );
    add_path(baserel, path);
}

/*
 * Create a ForeignScan plan node from the selected foreign access path. This
 * is called at the end of query planning. The parameters are as for
 * GetForeignRelSize, plus the selected ForeignPath (previously produced by
 * GetForeignPaths), the target list to be emitted by the plan node, and the
 * restriction clauses to be enforced by the plan node.
 *
 * This function must create and return a ForeignScan plan node; it's
 * recommended to use make_foreignscan to build the ForeignScan node.
 *
 * See Section 53.4 for additional information.
 */
static ForeignScan *kafkaGetForeignPlan(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses
    ) {
    return make_foreignscan(
            tlist,
            scan_clauses,
            baserel->relid,
            NULL,
            best_path->fdw_private
        );
}

/*
 * Begin executing a foreign scan. This is called during executor startup. It
 * should perform any initialization needed before the scan can start, but not
 * start executing the actual scan (that should be done upon the first call to
 * IterateForeignScan). The ForeignScanState node has already been created, but
 * its fdw_state field is still NULL. Information about the table to scan is
 * accessible through the ForeignScanState node (in particular, from the
 * underlying ForeignScan plan node, which contains any FDW-private information
 * provided by GetForeignPlan). eflags contains flag bits describing the
 * executor's operating mode for this plan node.
 *
 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
 * should not perform any externally-visible actions; it should only do the
 * minimum required to make the node state valid for ExplainForeignScan and
 * EndForeignScan.
 */
static void kafkaBeginForeignScan(
        ForeignScanState *node,
        int eflags
    ) {
    // TODO: Implement this.
    // This should establish the connection to kafka if it is not already available.

    // Do nothing for EXPLAIN
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }
}

/*
 * Fetch one row from the foreign source, returning it in a tuple table slot
 * (the node's ScanTupleSlot should be used for this purpose). Return NULL if
 * no more rows are available. The tuple table slot infrastructure allows
 * either a physical or virtual tuple to be returned; in most cases the latter
 * choice is preferable from a performance standpoint. Note that this is called
 * in a short-lived memory context that will be reset between invocations.
 * Create a memory context in BeginForeignScan if you need longer-lived
 * storage, or use the es_query_cxt of the node's EState.
 *
 * The rows returned must match the column signature of the foreign table being
 * scanned. If you choose to optimize away fetching columns that are not
 * needed, you should insert nulls in those column positions.
 *
 * Note that PostgreSQL's executor doesn't care whether the rows returned
 * violate any NOT NULL constraints that were defined on the foreign table
 * columns — but the planner does care, and may optimize queries incorrectly if
 * NULL values are present in a column declared not to contain them. If a NULL
 * value is encountered when the user has declared that none should be present,
 * it may be appropriate to raise an error (just as you would need to do in the
 * case of a data type mismatch).
 */
static TupleTableSlot *kafkaIterateForeignScan(
        ForeignScanState *node
    ) {
    // TODO: Implement this.
    // A buffer of messages should be loaded from kafka and then read one row at a time.
    return NULL;
}

/*
 * Restart the scan from the beginning. Note that any parameters the scan
 * depends on may have changed value, so the new scan does not necessarily
 * return exactly the same rows.
 */
static void kafkaReScanForeignScan(
        ForeignScanState *node
    ) {
    // TODO: Implement this.
    // This should reset the buffer index.
}

/*
 * End the scan and release resources. It is normally not important to release
 * palloc'd memory, but for example open files and connections to remote
 * servers should be cleaned up.
 */
static void kafkaEndForeignScan(
        ForeignScanState *node
    ) {
    // TODO: Implement this.
    // This should clear the index and the buffer.
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void estimate_costs(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Cost *startup_cost,
        Cost *total_cost
    ) {
    BlockNumber pages = baserel->pages;
    double ntuples = baserel->tuples;
    Cost run_cost = 0;
    Cost cpu_per_tuple;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     */
    run_cost += seq_page_cost * pages;

    *startup_cost = baserel->baserestrictcost.startup;
    cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;
    *total_cost = *startup_cost + run_cost;
}
