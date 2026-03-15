#ifndef MEM_COLLECTOR_H
#define MEM_COLLECTOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"
#include "executor/nodeGather.h"
#include "executor/nodeGatherMerge.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"

#ifdef __cplusplus
}
#endif

#include <errno.h>
#include <string.h>
#include <unistd.h>

#define MEM_COLLECTOR_FEATURE_CAPACITY 4096
#define MEM_COLLECTOR_SAMPLE_CAPACITY 4096
#define MEM_COLLECTOR_ACTIVE_CAPACITY 128
#define MEM_COLLECTOR_TRANCHE_NAME "mem_collector"
#define MEM_COLLECTOR_BGW_DBNAME "postgres"

typedef struct QueryFeatureEntry {
    uint64 feature_id;
    uint64 query_id;
    int32 backend_id;
    int32 proc_pid;
    uint64 plan_seq;
    double max_sort_ratio;
    double max_hash_ratio;
    double max_hashagg_ratio;
    double parallel_ratio;
    bool is_ap;
    TimestampTz planned_at;
} QueryFeatureEntry;

typedef struct QuerySampleEntry {
    uint64 sample_id;
    uint64 feature_id;
    uint64 query_id;
    int32 backend_id;
    uint64 plan_seq;
    double max_sort_ratio;
    double max_hash_ratio;
    double max_hashagg_ratio;
    double parallel_ratio;
    bool is_ap;
    double peak_mem_ratio;
    bool spill_occurred;
    double estimation_error;
    TimestampTz collected_at;
    bool flushed;
} QuerySampleEntry;

typedef struct MemCollectorSharedState {
    LWLock *lock;
    uint64 next_feature_id;
    uint64 next_sample_id;
    uint64 feature_write_pos;
    uint64 sample_write_pos;
    QueryFeatureEntry feature_ring[MEM_COLLECTOR_FEATURE_CAPACITY];
    QuerySampleEntry sample_ring[MEM_COLLECTOR_SAMPLE_CAPACITY];
} MemCollectorSharedState;

typedef struct PlanFeatureAccumulator {
    double max_sort_ratio;
    double max_hash_ratio;
    double max_hashagg_ratio;
    double parallel_ratio;
    bool is_ap;
} PlanFeatureAccumulator;

typedef struct RuntimeAccumulator {
    double peak_mem_ratio;
    double estimation_error;
    bool spill_occurred;
} RuntimeAccumulator;

typedef struct ActiveQueryEntry {
    uintptr_t query_desc_addr;
    uint64 feature_id;
    uint64 query_id;
    uint64 plan_seq;
    bool in_use;
} ActiveQueryEntry;

typedef struct FeatureSnapshot {
    int count;
    QueryFeatureEntry entries[MEM_COLLECTOR_FEATURE_CAPACITY];
} FeatureSnapshot;

typedef struct SampleSnapshot {
    int count;
    QuerySampleEntry entries[MEM_COLLECTOR_SAMPLE_CAPACITY];
} SampleSnapshot;

extern MemCollectorSharedState *mem_state;
extern uint64 mem_local_plan_seq;
extern ActiveQueryEntry active_queries[MEM_COLLECTOR_ACTIVE_CAPACITY];

extern int mem_collector_window_seconds;
extern bool mem_collector_bgw_enabled;
extern char *mem_collector_bgw_dbname;
extern volatile sig_atomic_t mem_collector_got_sigterm;

void _PG_init(void);
void _PG_fini(void);
void mem_collector_bgw_main(Datum main_arg);

Datum mem_collector_features(PG_FUNCTION_ARGS);
Datum mem_collector_pending_samples(PG_FUNCTION_ARGS);
Datum mem_collector_flush_query_samples(PG_FUNCTION_ARGS);
Datum mem_collector_generate_window_sample(PG_FUNCTION_ARGS);
Datum mem_collector_reset(PG_FUNCTION_ARGS);

Size mem_collector_shmem_size(void);
double mem_collector_total_memory_bytes(void);
double mem_collector_safe_ratio(double value, double total);
double mem_collector_parallel_ratio(const Plan *plan);
double mem_collector_estimation_ratio(const Plan *plan, const Instrumentation *instrument);

void mem_collector_walk_plan_tree(Plan *plan, PlanFeatureAccumulator *acc, double total_bytes);
void mem_collector_walk_planstate_tree(PlanState *planstate, RuntimeAccumulator *acc, double total_bytes);

QueryFeatureEntry mem_collector_build_feature_from_plannedstmt(PlannedStmt *plannedstmt,
                                                               int32 backend_id,
                                                               int32 proc_pid,
                                                               uint64 plan_seq);

Size mem_collector_sort_peak_bytes(SortState *sortstate, bool *spill_occurred);
Size mem_collector_hashjoin_peak_bytes(HashJoinState *hashjoinstate, bool *spill_occurred);
Size mem_collector_hash_peak_bytes(HashState *hashstate, bool *spill_occurred);
Size mem_collector_agg_peak_bytes(AggState *aggstate, bool *spill_occurred);

void mem_collector_push_feature(QueryFeatureEntry *entry);
bool mem_collector_lookup_feature(uint64 feature_id, QueryFeatureEntry *out);
bool mem_collector_lookup_feature_by_query_id(uint64 query_id, QueryFeatureEntry *out);
void mem_collector_push_sample(QuerySampleEntry *entry);
int mem_collector_collect_samples(QuerySampleEntry *out, int limit, bool only_unflushed);
void mem_collector_mark_sample_flushed(uint64 sample_id);
FeatureSnapshot mem_collector_snapshot_features(void);
SampleSnapshot mem_collector_snapshot_samples(void);
int mem_collector_active_slot(uintptr_t query_desc_addr, bool create_if_missing);
void mem_collector_active_store(uintptr_t query_desc_addr, uint64 feature_id, uint64 query_id, uint64 plan_seq);
bool mem_collector_active_take(uintptr_t query_desc_addr, uint64 *feature_id, uint64 *query_id, uint64 *plan_seq);
void mem_collector_reset_state(void);

bool mem_collector_table_exists(const char *relname);
void mem_collector_flush_samples_via_spi(int limit, int *flushed);
void mem_collector_collect_window_snapshot(TimestampTz window_start, int window_seconds);

#endif
