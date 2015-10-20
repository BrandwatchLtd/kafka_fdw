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
#include <librdkafka/rdkafka.h>
#include <errno.h>

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

#define KAFKA_MAX_ERR_MSG 200

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
 * FDW-specific information for ForeignScanState.fdw_state or RelOptInfo.fdw_private.
 */
typedef struct KafkaFdwState
{
	ConnCacheKey     connection_credentials;
	ConnCacheEntry  *connection;
    char            *topic;
    int64            offset;
    rd_kafka_topic_t kafka_topic_handle;
} KafkaFdwState;

/*
 * Global connection cache hashtable
 */

typedef struct ConnCacheKey
{
    char       *host;
    uint16      port;
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	rd_kafka_t kafka_handle;
} ConnCacheEntry;

//static HTAB *ConnectionHash = NULL;

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

/*
 * Helper functions
 */
static bool is_valid_option(
        const char *option,
        Oid context
    );
static void estimate_costs(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Cost *startup_cost,
        Cost *total_cost
    );
    
static ConnCacheEntry *get_connection();
static void close_connection();

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
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses kafka_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum kafka_fdw_validator(PG_FUNCTION_ARGS) {
   List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
   Oid catalog = PG_GETARG_OID(1);
   ListCell *cell;

   /*
    * Check that only options supported by file_fdw, and allowed for the
    * current object type, are given.
    */
   foreach(cell, options_list) {
       DefElem *def = (DefElem *) lfirst(cell);

       if (!is_valid_option(def->defname, catalog)) {
           const struct KafkaFdwOption *opt;
           StringInfoData buf;

           /*
            * Unknown option specified, complain about it. Provide a hint
            * with list of valid options for the object.
            */
           initStringInfo(&buf);
           for (opt = valid_options; opt->optname; opt++) {
               if (catalog == opt->optcontext)
                   appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
                                    opt->optname);
           }

           ereport(ERROR,
                   (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid option \"%s\"", def->defname),
                    buf.len > 0
                    ? errhint("Valid options in this context are: %s",
                              buf.data)
                 : errhint("There are no valid options in this context.")));
       }
   }

   // TODO: check if offset fits uint64
   // TODO: check if port fits uint16
   // TODO: check if all options are set
   // TODO: check if options are not duplicated

   PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool is_valid_option(const char *option, Oid context) {
   const struct KafkaFdwOption *opt;

   for (opt = valid_options; opt->optname; opt++) {
       if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
           return true;
   }
   return false;
}

// TODO: do we need it?
/*
 * Fetch the options for a file_fdw foreign table.
 */
static void
fill_kafka_state(Oid foreigntableid, KafkaFdwState *kstate)
{
	// TODO: extract also the table structure?
	// BTW We are going to support 2 columns only: data (text/json/whatever)
	//                                             and kafka_offset (long)
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List	   *options;
	ListCell   *lc;

	/*
	 * Extract options from FDW objects.  We ignore user mappings and attributes
	 * because kafka_fdw doesn't have any options that can be specified there.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	/*
	 * Separate out the filename.
	 */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		
		if (strcmp(def->defname, "host") == 0)
		{
			kstate->connection_credentials.host = defGetString(def);
		} else if (strcmp(def->defname, "port") == 0) {
			/* already validated in kafka_fdw_validator() */
			kstate->connection_credentials.port = atoi(defGetString(def));
		} else if (strcmp(def->defname, "topic") == 0) {
			kstate->topic = defGetString(def);
		} else if (strcmp(def->defname, "offset") == 0) {
			/* already validated in kafka_fdw_validator() */
			kstate->offset = atoi(defGetString(def));
		}
	}
	
	kstate->connection = NULL;
	kstate->rd_kafka_topic_t = NULL;

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
    /*
     * if sort by offset is required we should have provided it for free
     */
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
static ForeignScan *
kafkaGetForeignPlan(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid,
                    ForeignPath *best_path,
                    List *tlist,
                    List *scan_clauses)
{
    Index       scan_relid = baserel->relid;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL,    /* no expressions to evaluate */
                            NIL     /* no custom data */);
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
    KafkaFdwState *kstate;

    // Do nothing for EXPLAIN
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }
    
	kstate = (KafkaFdwState *) palloc(sizeof(KafkaFdwState));
	fill_kafka_state(RelationGetRelid(node->ss.ss_currentRelation), kstate);
	node->fdw_state = (void *) kstate;
	
	/* Open connection if possible */
	if (kstate->connection == NULL) {
		kstate->connection = get_connection(kstate->connection_credentials);
	}
	
	/* Create topic handle if successfully connected */
	if (kstate->connection != NULL) {
		/* We will check if it was successful in kafkaIterateForeignScan*/
		kstate->rd_kafka_topic_t = rd_kafka_topic_new(kstate->connection, kstate->topic, NULL);
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
		...
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


static ConnCacheEntry *get_connection(ConnCacheKey key, char[KAFKA_MAX_ERR_MSG] errstr) {
	ConnCacheEntry *entry;
	bool            found;
    StringInfoData  brokers;
    char           *errstr;

	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("kafka_fdw connections", 8,
									 &ctl,
								   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);

	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */		
		entry->kafka_handle = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
					                       errstr, sizeof(errstr));
		if (entry->kafka_handle != NULL) {
			/* Add brokers */
			initStringInfo(&brokers);
			appendStringInfo(&brokers, '%s:%ld', key->host, key->port);
			if (!(rd_kafka_brokers_add(entry->kafka_handle, host_port.data))) {
				rd_kafka_destroy(entry->kafka_handle);
				strcpy(errstr, "No valid brokers specified");
			}
			pfree(host_port.data);
		}
		if (entry->kafka_handle == NULL) {
			hash_search(ConnectionHash, &key, HASH_REMOVE, &found);
			entry = NULL;
		}
	}
	
	return entry;
}

static void close_connection(ConnCacheKey key) {
	ConnCacheEntry *entry;
	bool            found;

	if (ConnectionHash != NULL) {
		entry = hash_search(ConnectionHash, &key, HASH_FIND, &found);
		if (found) {
			if (entry->kafka_handle != NULL) {
				rd_kafka_destroy(entry->kafka_handle);
			}
			hash_search(ConnectionHash, &key, HASH_REMOVE, NULL);
		}		
	}
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
    // TODO: maybe hardcode larger values here?
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
