#include "mem_collector.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;

PG_FUNCTION_INFO_V1(mem_collector_features);
PG_FUNCTION_INFO_V1(mem_collector_pending_samples);
PG_FUNCTION_INFO_V1(mem_collector_flush_query_samples);
PG_FUNCTION_INFO_V1(mem_collector_generate_window_sample);
PG_FUNCTION_INFO_V1(mem_collector_reset);

static inline void
mem_collector_call_prev_executor_start(QueryDesc *queryDesc, int eflags)
{
    if (prev_ExecutorStart != NULL) {
        prev_ExecutorStart(queryDesc, eflags);
    } else {
        standard_ExecutorStart(queryDesc, eflags);
    }
}

static inline void
mem_collector_call_prev_executor_end(QueryDesc *queryDesc)
{
    if (prev_ExecutorEnd != NULL) {
        prev_ExecutorEnd(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }
}

static void
mem_collector_shmem_startup(void)
{
    bool found = false;
    LWLockPadded *lock_tranche;

    if (prev_shmem_startup_hook != NULL) {
        prev_shmem_startup_hook();
    }

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    mem_state = (MemCollectorSharedState *) ShmemInitStruct("mem_collector_state", mem_collector_shmem_size(), &found);
    lock_tranche = GetNamedLWLockTranche(MEM_COLLECTOR_TRANCHE_NAME);

    if (lock_tranche == NULL) {
        LWLockRelease(AddinShmemInitLock);
        ereport(ERROR, (errmsg("mem_collector named LWLock tranche is unavailable")));
    }

    if (!found) {
        memset(mem_state, 0, mem_collector_shmem_size());
        mem_state->lock = &(lock_tranche[0].lock);
    } else if (mem_state->lock == NULL) {
        mem_state->lock = &(lock_tranche[0].lock);
    }

    LWLockRelease(AddinShmemInitLock);
}

static PlannedStmt *
mem_collector_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *plannedstmt;
    QueryFeatureEntry feature;

    if (prev_planner_hook != NULL) {
        plannedstmt = prev_planner_hook(parse, cursorOptions, boundParams);
    } else {
        plannedstmt = standard_planner(parse, cursorOptions, boundParams);
    }

    if (plannedstmt == NULL || plannedstmt->planTree == NULL) {
        return plannedstmt;
    }

    mem_local_plan_seq++;
    feature = mem_collector_build_feature_from_plannedstmt(plannedstmt, MyBackendId, MyProcPid, mem_local_plan_seq);
    mem_collector_push_feature(&feature);

    return plannedstmt;
}

static void
mem_collector_executor_start(QueryDesc *queryDesc, int eflags)
{
    QueryFeatureEntry feature;
    QueryFeatureEntry existing_feature;

    if (queryDesc == NULL || queryDesc->plannedstmt == NULL) {
        mem_collector_call_prev_executor_start(queryDesc, eflags);
        return;
    }

    queryDesc->instrument_options |= INSTRUMENT_ROWS;
    mem_collector_call_prev_executor_start(queryDesc, eflags);

    if (mem_collector_lookup_feature_by_query_id((uint64) queryDesc->plannedstmt->queryId, &existing_feature)) {
        mem_collector_active_store((uintptr_t) queryDesc, existing_feature.feature_id,
                                   existing_feature.query_id, existing_feature.plan_seq);
        return;
    }

    mem_local_plan_seq++;
    feature = mem_collector_build_feature_from_plannedstmt(queryDesc->plannedstmt, MyBackendId, MyProcPid, mem_local_plan_seq);
    mem_collector_push_feature(&feature);
    mem_collector_active_store((uintptr_t) queryDesc, feature.feature_id, feature.query_id, feature.plan_seq);
}

static void
mem_collector_executor_end(QueryDesc *queryDesc)
{
    uint64 feature_id = 0;
    uint64 query_id = 0;
    uint64 plan_seq = 0;
    QueryFeatureEntry feature;
    QuerySampleEntry sample;
    RuntimeAccumulator runtime_acc;

    if (queryDesc != NULL && queryDesc->plannedstmt != NULL &&
        mem_collector_active_take((uintptr_t) queryDesc, &feature_id, &query_id, &plan_seq)) {
        bool have_feature = false;

        if (feature_id != 0) {
            have_feature = mem_collector_lookup_feature(feature_id, &feature);
        }
        if (!have_feature && query_id != 0) {
            have_feature = mem_collector_lookup_feature_by_query_id(query_id, &feature);
        }

        if (have_feature) {
            memset(&sample, 0, sizeof(sample));
            memset(&runtime_acc, 0, sizeof(runtime_acc));

            mem_collector_walk_planstate_tree(queryDesc->planstate, &runtime_acc, mem_collector_total_memory_bytes());

            sample.feature_id = feature.feature_id;
            sample.query_id = feature.query_id;
            sample.backend_id = feature.backend_id;
            sample.plan_seq = feature.plan_seq;
            sample.max_sort_ratio = feature.max_sort_ratio;
            sample.max_hash_ratio = feature.max_hash_ratio;
            sample.max_hashagg_ratio = feature.max_hashagg_ratio;
            sample.parallel_ratio = feature.parallel_ratio;
            sample.is_ap = feature.is_ap;
            sample.peak_mem_ratio = runtime_acc.peak_mem_ratio;
            sample.spill_occurred = runtime_acc.spill_occurred;
            sample.estimation_error = runtime_acc.estimation_error;
            sample.collected_at = GetCurrentTimestamp();
            sample.flushed = false;

            mem_collector_push_sample(&sample);
        }
    }

    mem_collector_call_prev_executor_end(queryDesc);
}

void
_PG_init(void)
{
    BackgroundWorker worker;

    DefineCustomIntVariable(
        "mem_collector.window_seconds",
        "Seconds between background window snapshots.",
        NULL,
        &mem_collector_window_seconds,
        30,
        1,
        3600,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);

    DefineCustomBoolVariable(
        "mem_collector.enable_bgw",
        "Enable the mem_collector background worker.",
        NULL,
        &mem_collector_bgw_enabled,
        true,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);

    DefineCustomStringVariable(
        "mem_collector.bgw_dbname",
        "Database used by mem_collector background worker.",
        NULL,
        &mem_collector_bgw_dbname,
        MEM_COLLECTOR_BGW_DBNAME,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL);

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    RequestAddinShmemSpace(mem_collector_shmem_size());
    RequestNamedLWLockTranche(MEM_COLLECTOR_TRANCHE_NAME, 1);

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = mem_collector_shmem_startup;

    prev_planner_hook = planner_hook;
    planner_hook = mem_collector_planner;

    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = mem_collector_executor_start;

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = mem_collector_executor_end;

    if (mem_collector_bgw_enabled) {
        memset(&worker, 0, sizeof(worker));
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = mem_collector_window_seconds;
        snprintf(worker.bgw_name, BGW_MAXLEN, "mem_collector worker");
        snprintf(worker.bgw_type, BGW_MAXLEN, "mem_collector");
        snprintf(worker.bgw_library_name, BGW_MAXLEN, "mem_collector");
        snprintf(worker.bgw_function_name, BGW_MAXLEN, "mem_collector_bgw_main");
        worker.bgw_main_arg = (Datum) 0;
        worker.bgw_notify_pid = 0;
        RegisterBackgroundWorker(&worker);
    }
}

void
_PG_fini(void)
{
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
    shmem_startup_hook = prev_shmem_startup_hook;
    planner_hook = prev_planner_hook;
}
