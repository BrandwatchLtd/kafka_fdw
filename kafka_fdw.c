/*-------------------------------------------------------------------------
 *
 * kafka_fdw.c
 *        foreign-data wrapper for access to Apache Kafka
 *
 * TODO: Copyright here
 * TODO: Implement explicit offset committing
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
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT 9092
#define DEFAULT_OFFSET RD_KAFKA_OFFSET_BEGINNING /* -2 */
#define DEFAULT_BATCH_SIZE 30000

#define CONSUME_TIMEOUT 100
#define KAFKA_MAX_ERR_MSG 200

#define RELSIZE_ROWS_ESTIMATE 30000
#define RELSIZE_WIDTH_ESTIMATE 2000

static char default_host[] = DEFAULT_HOST;

/*
 * Info about the columns acceptable for usage in foreign tables
 */

typedef enum
{
	COLUMN_OFFSET = 0,
	COLUMN_KEY,
	COLUMN_VALUE,
	VALID_COLUMNS_NAMES_COUNT
} ValidColumnNameKey;

struct ValidColumnInfo
{
	char name[9];
	Oid  type;
};

static const struct ValidColumnInfo validColumnInfo[] = {
	{"i_offset", INT8OID},
	{"t_key"   , TEXTOID},
	{"t_value" , TEXTOID}
};

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
    {"batch_size", ForeignTableRelationId},
    /* Sentinel */
    {NULL, InvalidOid}
};

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
    ConnCacheKey key;           /* hash key (must be first) */
    rd_kafka_t  *kafka_handle;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

/*
 * FDW-specific information for ForeignScanState.fdw_state or RelOptInfo.fdw_private.
 */
 
typedef struct ColumnInfo
{
    int		 attnum; /* -1 means no such column found */
    Oid      typioparam;
    FmgrInfo pg_in_func;
    int32    atttypmod;
} ColumnInfo;
 
typedef struct KafkaFdwState
{
    ConnCacheKey          connection_credentials;
    ConnCacheEntry       *connection;
    char                 *topic;
    int64                 offset;
    size_t                batch_size;
    rd_kafka_topic_t     *kafka_topic_handle;
    rd_kafka_message_t  **buffer;
    ssize_t               buffer_count;
    ssize_t               buffer_cursor;
    ColumnInfo			  columnInfo[VALID_COLUMNS_NAMES_COUNT];
    int64                 max_offset_returned;
} KafkaFdwState;

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

static ConnCacheEntry *get_connection(ConnCacheKey key,
                                      char errstr[KAFKA_MAX_ERR_MSG]);
// TODO: maybe some time we will need to close connection explicitly
//static void close_connection(ConnCacheKey key);
static void kafka_start(KafkaFdwState *kstate);
static void kafka_stop(KafkaFdwState *kstate);
static void fill_pg_column_info(Relation foreignTableRelation,
                              ColumnInfo columnInfo[VALID_COLUMNS_NAMES_COUNT]);
static void estimate_costs(
        PlannerInfo *root,
        RelOptInfo *baserel,
        Cost *startup_cost,
        Cost *total_cost
    );
static bool foreign_table_has_option(Relation table, const char *option);

/* ************************************************************************** */

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
Datum kafka_fdw_validator(PG_FUNCTION_ARGS)
{
   List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
   Oid catalog = PG_GETARG_OID(1);
   ListCell *cell;

   /*
    * Check that only options supported by file_fdw, and allowed for the
    * current object type, are given.
    */
   foreach(cell, options_list)
   {
       DefElem *def = (DefElem *) lfirst(cell);

       if (!is_valid_option(def->defname, catalog))
       {
           const struct KafkaFdwOption *opt;
           StringInfoData buf;

           /*
            * Unknown option specified, complain about it. Provide a hint
            * with list of valid options for the object.
            */
           initStringInfo(&buf);
           for (opt = valid_options; opt->optname; opt++)
           {
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
   // TODO: check if batch_size fits size_t
   // TODO: check if port fits uint16
   // TODO: check if all options are set
   // TODO: check if options are not duplicated

   PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool is_valid_option(const char *option, Oid context)
{
   const struct KafkaFdwOption *opt;

   for (opt = valid_options; opt->optname; opt++)
   {
       if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
           return true;
   }
   return false;
}

/*
 * Fetch the options for a file_fdw foreign table.
 */
static void
fill_kafka_state(Relation foreignTableRelation, KafkaFdwState *kstate)
{
	ForeignTable       *table;
	ForeignServer      *server;
	ListCell           *lc;
	DefElem 		   *def;
	
	/* Write defaults */
	kstate->connection_credentials.host = default_host;
	kstate->connection_credentials.port = DEFAULT_PORT;
	kstate->batch_size = RELSIZE_ROWS_ESTIMATE;
	kstate->offset = DEFAULT_OFFSET;

    /*
     * Extract options from FDW objects.  We ignore user mappings and wrapper 
     * attributes because kafka_fdw doesn't have any options 
     * that can be specified there.
     */
    table = GetForeignTable(RelationGetRelid(foreignTableRelation));
    server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		def = (DefElem *) lfirst(lc);
		
		if (strcmp(def->defname, "host") == 0)
		{
			kstate->connection_credentials.host = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			/* already validated in kafka_fdw_validator() */
			kstate->connection_credentials.port = atoi(defGetString(def));
		}
	}
	
	foreach(lc, table->options)
	{
		def = (DefElem *) lfirst(lc);
		
		if (strcmp(def->defname, "topic") == 0)
		{
			kstate->topic = defGetString(def);
		}
		else if (strcmp(def->defname, "offset") == 0)
		{
			/* already validated in kafka_fdw_validator() */
			kstate->offset = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, "batch_size") == 0)
		{
			/* already validated in kafka_fdw_validator() */
			kstate->batch_size = atoi(defGetString(def));
		}
	}
	
	kstate->connection = NULL;
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
    )
{
	/* 
	 * Here we return some random values, at least not as little as default ones.
	 * Maybe they should be made configurable.
	 * 
     * Making a kafka request to determine the number of values available is
     * excessive given that rows of a single unindexed column are going to be
     * returned.
	 */
	baserel->rows = RELSIZE_ROWS_ESTIMATE;
	baserel->width = RELSIZE_WIDTH_ESTIMATE;
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
    )
{
    Cost startup_cost;
    Cost total_cost;
    Path *path;

    // TODO: fail here or in kafkaGetForeignPlan if table structure is wrong

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

    // TODO: fail here or in kafkaGetForeignPaths if table structure is wrong

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
    )
{	
    KafkaFdwState         *kstate;
    char                   kafka_errstr[KAFKA_MAX_ERR_MSG];
	rd_kafka_topic_conf_t *topic_conf;

    /* Do nothing for EXPLAIN */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
		kstate = NULL;
        return;
    }
	
	/* Initialize internal state */
	kstate = (KafkaFdwState *) palloc(sizeof(KafkaFdwState));
	fill_kafka_state(node->ss.ss_currentRelation, kstate);
	node->fdw_state = (void *) kstate;
	
	/*
	 * Init PostgreSQL-related stuff
	 */
	fill_pg_column_info(node->ss.ss_currentRelation, kstate->columnInfo);
	/* Prohibit parallel usage */
	LockRelationOid(RelationGetRelid(node->ss.ss_currentRelation), 
	                                                  ShareUpdateExclusiveLock);
	kstate->max_offset_returned = kstate->offset - 1;
	
    /*
     * Init Kafka-related stuff
     */

	/* Open connection if possible */
	if (kstate->connection == NULL)
	{
		kstate->connection = get_connection(kstate->connection_credentials, 
		                                                         kafka_errstr);
	}
	if (kstate->connection == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg_internal("kafka_fdw: Unable to connect to %s:%d", 
			                 kstate->connection_credentials.host, 
			                 kstate->connection_credentials.port),
			 errdetail("%s", kafka_errstr))
		);
	}

	/* Create topic handle */
	topic_conf = rd_kafka_topic_conf_new();
	kstate->kafka_topic_handle = rd_kafka_topic_new(
	                                 kstate->connection->kafka_handle,
	                                 kstate->topic,
	                                 topic_conf);
	if (kstate->kafka_topic_handle == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg_internal("kafka_fdw: Unable to create topic %s", 
			                                           kstate->topic))
		);
	}
	
	kstate->buffer = palloc(sizeof(rd_kafka_message_t *) * (kstate->batch_size));

	/* reset the buffer, start consuming */
    kafka_start(kstate);
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
    )
{
    KafkaFdwState      *kstate;
	rd_kafka_message_t *message;
	TupleTableSlot     *slot;
	ValidColumnNameKey  valid_column_name_key;	
	StringInfoData 		columnData;
 	ColumnInfo         *columnInfo;
	
	kstate = node->fdw_state;

	slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);
	
	/* 
	 * Request more messages 
	 * if we have already returned all the remaining ones 
	 */
	if (kstate->buffer_cursor >= kstate->buffer_count)
	{
		kstate->buffer_count = rd_kafka_consume_batch(
						kstate->kafka_topic_handle,
						0/*RD_KAFKA_PARTITION_UA*/,
						CONSUME_TIMEOUT, kstate->buffer, kstate->batch_size);
		if (kstate->buffer_count == -1)
		{
			rd_kafka_topic_destroy(kstate->kafka_topic_handle);
		}
		kstate->buffer_cursor = 0;
	}

	/* Still no data */
	if (kstate->buffer_cursor >= kstate->buffer_count)
		return slot;
	
	message = kstate->buffer[kstate->buffer_cursor];
	
	/* This also means there is no data */
	if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
	{
		ereport(NOTICE,
			(errmsg_internal("kafka_fdw has reached the end of the queue"))
		);
		return slot;
	}
	
	if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		initStringInfo(&columnData);
		appendBinaryStringInfo(&columnData, 
					  (char *)(message->payload), message->len);
		ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			errmsg_internal(
				"kafka_fdw got an error %d when fetching a message from queue", 
				message->err),
			errdetail_internal("%s", columnData.data)
			)
		);
	}
		
	/* Copy data returned by kafka to a postgres-readable format */		
	for (valid_column_name_key = 0; 
		 valid_column_name_key < VALID_COLUMNS_NAMES_COUNT;
		 valid_column_name_key++)
	{
		columnInfo = &kstate->columnInfo[valid_column_name_key];
		if (columnInfo->attnum != -1) {
			initStringInfo(&columnData);
			switch (valid_column_name_key) {
				case COLUMN_OFFSET:
					pq_sendint64(&columnData, message->offset);
					break;
				case COLUMN_KEY:
					appendBinaryStringInfo(&columnData, 
					              (char *)(message->key), message->key_len);
					break;
				case COLUMN_VALUE:
					appendBinaryStringInfo(&columnData, 
					              (char *)(message->payload), message->len);
			}
			slot->tts_values[columnInfo->attnum] = 
			        ReceiveFunctionCall(&columnInfo->pg_in_func,
			        					&columnData,
			        					columnInfo->typioparam,
			        					columnInfo->atttypmod);
			slot->tts_isnull[columnInfo->attnum] = false;
			pfree(columnData.data);
		};
	}
	ExecStoreVirtualTuple(slot);
	
	/* Remember last offset returned, beware of rescans */
	if (kstate->max_offset_returned < message->offset)
		kstate->max_offset_returned = message->offset;
	
	rd_kafka_message_destroy(message);
	kstate->buffer_cursor++;
	return slot;
}

/*
 * Restart the scan from the beginning. Note that any parameters the scan
 * depends on may have changed value, so the new scan does not necessarily
 * return exactly the same rows.
 */
static void kafkaReScanForeignScan(
        ForeignScanState *node
    )
{
    KafkaFdwState *kstate;
    
	kstate = node->fdw_state;
	kafka_stop(kstate);
	kafka_start(kstate);
}

/*
 * End the scan and release resources. It is normally not important to release
 * palloc'd memory, but for example open files and connections to remote
 * servers should be cleaned up.
 */
static void kafkaEndForeignScan(
        ForeignScanState *node
    )
{
    KafkaFdwState *kstate;	
	char 		   offsetStr[20]; /* Length sufficient to store any int64 value */
	AlterTableCmd *cmd;
    
	kstate = node->fdw_state;

	/* kstate is null if this is an EXPLAIN call */
    if (kstate == NULL)
		return;

	/* Stop consuming */
	kafka_stop(kstate);

	rd_kafka_topic_destroy(kstate->kafka_topic_handle);
	pfree(kstate->buffer);
	
	/* Execute ALTER FOREIGN TABLE ... SET OFFSET TO ... */
	cmd = makeNode(AlterTableCmd);
	cmd->subtype = AT_GenericOptions;
	snprintf(offsetStr, sizeof(offsetStr)/sizeof(char), "%ld", 
	                                           kstate->max_offset_returned + 1);
	cmd->def = (Node *) list_make1(makeDefElemExtended(
		NULL, "offset", (Node *) makeString(offsetStr), 
		foreign_table_has_option(node->ss.ss_currentRelation, "offset")
			? DEFELEM_SET : DEFELEM_ADD
	));

	AlterTableInternal(RelationGetRelid(node->ss.ss_currentRelation), 
	                                                   list_make1(cmd), false);
}

static ConnCacheEntry *get_connection(ConnCacheKey key, 
                                      char errstr[KAFKA_MAX_ERR_MSG])
{
    ConnCacheEntry  *entry;
    bool             found;
	
	// TODO: retry if needed, specify timeout policy

    if (ConnectionHash == NULL)
    {
        HASHCTL     ctl;

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
		StringInfoData   brokers;
		rd_kafka_conf_t *conf;

		/* initialize new hashtable entry (key is already filled in) */		
    
		conf = rd_kafka_conf_new();

		//if (rd_kafka_conf_set(conf, "some_key", 
		//                "some_value", errstr, KAFKA_MAX_ERR_MSG))
		//{
		//	rd_kafka_conf_destroy(conf);
		//	return NULL;
		//}
		
		entry->kafka_handle = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					                       errstr, KAFKA_MAX_ERR_MSG);
		if (entry->kafka_handle != NULL)
		{
			/* Add brokers */
			initStringInfo(&brokers);
			appendStringInfo(&brokers, "%s:%d", key.host, key.port);
			/* Check if exactly 1 broker was added */
			if (rd_kafka_brokers_add(entry->kafka_handle, brokers.data) != 1)
			{
				rd_kafka_destroy(entry->kafka_handle);
				rd_kafka_conf_destroy(conf);
				strcpy(errstr, "No valid brokers specified");
				entry->kafka_handle = NULL;
			}
			pfree(brokers.data);
		}
		if (entry->kafka_handle == NULL)
		{
			hash_search(ConnectionHash, &key, HASH_REMOVE, &found);
			entry = NULL;
		}		
	}

    return entry;
}

// TODO: maybe some time we will need to close connection explicitly
//static void close_connection(ConnCacheKey key)
//{
//    ConnCacheEntry *entry;
//    bool            found;
//
//    if (ConnectionHash != NULL)
//    {
//        entry = hash_search(ConnectionHash, &key, HASH_FIND, &found);
//        if (found)
//        {
//            if (entry->kafka_handle != NULL)
//                rd_kafka_destroy(entry->kafka_handle);
//            hash_search(ConnectionHash, &key, HASH_REMOVE, NULL);
//        }
//    }
//}

static void kafka_start(KafkaFdwState *kstate)
{
	kstate->buffer_count = 0;
	kstate->buffer_cursor = 0;
	/* Start consuming */
	PG_TRY();
	{
		if (rd_kafka_consume_start(kstate->kafka_topic_handle, 
		                           0/*RD_KAFKA_PARTITION_UA*/,
		                           kstate->offset))
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				errmsg_internal(
					"kafka_fdw could not start consuming from offset %ld", 
					kstate->offset))
			);
		}
	}
	PG_CATCH();
	{
		rd_kafka_topic_destroy(kstate->kafka_topic_handle);
	}
	PG_END_TRY();
}

static void kafka_stop(KafkaFdwState *kstate)
{
	rd_kafka_consume_stop(kstate->kafka_topic_handle, 0/*RD_KAFKA_PARTITION_UA*/);
}

static void fill_pg_column_info(Relation foreignTableRelation, 
                                ColumnInfo columnInfo[VALID_COLUMNS_NAMES_COUNT])
{
	TupleDesc          tupDesc;
	int                num_phys_attrs;
	int                attnum;
	Form_pg_attribute  attr;	
	ValidColumnNameKey valid_column_name_key;
	Oid                pg_in_func_oid;
	
	tupDesc = RelationGetDescr(foreignTableRelation);
	num_phys_attrs = tupDesc->natts;

	/* Place -1 values: this means the columns are not yet found in the table */
	for (valid_column_name_key = 0; 
	     valid_column_name_key < VALID_COLUMNS_NAMES_COUNT;
	     valid_column_name_key++)
	{
		columnInfo[valid_column_name_key].attnum = -1;
	}

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		attr = tupDesc->attrs[attnum];
		
		/* We don't need info for dropped attributes */
		if (attr->attisdropped)
			continue;
		
		/* Find out if current column is in the list of allowed ones */
		for (valid_column_name_key = 0; 
		     valid_column_name_key < VALID_COLUMNS_NAMES_COUNT; 
		     valid_column_name_key++)
		{
			if (strcmp(attr->attname.data,
			    validColumnInfo[valid_column_name_key].name) == 0)
			{
				break;
			}
		}
		
		/* Raise exception if column is not found*/
		if (valid_column_name_key == VALID_COLUMNS_NAMES_COUNT)
		{
            StringInfoData buf;
            
            /*
             * Unknown column specified, complain about it. Provide a hint
             * with list of valid columns for the object.
             */
            initStringInfo(&buf);
			for (valid_column_name_key = 0; 
				 valid_column_name_key < VALID_COLUMNS_NAMES_COUNT; 
				 valid_column_name_key++)
			{
				appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
                                   validColumnInfo[valid_column_name_key].name);
			}
			ereport(ERROR,
				(errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
				errmsg_internal("column %s is not supported by kafka_fdw", 
				                                         attr->attname.data),
				errhint("Valid column names are %s.", buf.data)
				)
			);
		}
		
		/* Raise exception if the type is wrong */
		if (validColumnInfo[valid_column_name_key].type != attr->atttypid)
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg_internal("column %s of a kafka_fdw foreign table has invalid type", 
				                                         attr->attname.data),
				errhint("It must have type %s.", 
					format_type_be(validColumnInfo[valid_column_name_key].type))
				)
			);
		}
		
		columnInfo[valid_column_name_key].attnum = attnum;
		
		/* Get proper input function for the column type */
		getTypeBinaryInputInfo(attr->atttypid, &pg_in_func_oid,
					&columnInfo[valid_column_name_key].typioparam);
		fmgr_info(pg_in_func_oid,
		            &columnInfo[valid_column_name_key].pg_in_func);
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
    )
{
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

static bool foreign_table_has_option(Relation table, const char *option)
{
	ListCell           *lc;
	DefElem 		   *def;
	
	foreach(lc, GetForeignTable(RelationGetRelid(table))->options)
	{
		def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, option) == 0)
			return true;
	}
	
	return false;
}
