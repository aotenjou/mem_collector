#include "mem_collector.h"

static void
mem_collector_fill_common_feature_values(const QueryFeatureEntry *entry, Datum *values, bool *nulls)
{
    memset(nulls, 0, sizeof(bool) * 11);
    values[0] = Int64GetDatum((int64) entry->feature_id);
    values[1] = Int64GetDatum((int64) entry->query_id);
    values[2] = Int32GetDatum(entry->backend_id);
    values[3] = Int32GetDatum(entry->proc_pid);
    values[4] = Int64GetDatum((int64) entry->plan_seq);
    values[5] = Float8GetDatum(entry->max_sort_ratio);
    values[6] = Float8GetDatum(entry->max_hash_ratio);
    values[7] = Float8GetDatum(entry->max_hashagg_ratio);
    values[8] = Float8GetDatum(entry->parallel_ratio);
    values[9] = BoolGetDatum(entry->is_ap);
    values[10] = TimestampTzGetDatum(entry->planned_at);
}

static void
mem_collector_fill_common_sample_values(const QuerySampleEntry *entry, Datum *values, bool *nulls)
{
    memset(nulls, 0, sizeof(bool) * 15);
    values[0] = Int64GetDatum((int64) entry->sample_id);
    values[1] = Int64GetDatum((int64) entry->feature_id);
    values[2] = Int64GetDatum((int64) entry->query_id);
    values[3] = Int32GetDatum(entry->backend_id);
    values[4] = Int64GetDatum((int64) entry->plan_seq);
    values[5] = Float8GetDatum(entry->max_sort_ratio);
    values[6] = Float8GetDatum(entry->max_hash_ratio);
    values[7] = Float8GetDatum(entry->max_hashagg_ratio);
    values[8] = Float8GetDatum(entry->parallel_ratio);
    values[9] = BoolGetDatum(entry->is_ap);
    values[10] = Float8GetDatum(entry->peak_mem_ratio);
    values[11] = BoolGetDatum(entry->spill_occurred);
    values[12] = Float8GetDatum(entry->estimation_error);
    values[13] = TimestampTzGetDatum(entry->collected_at);
    values[14] = BoolGetDatum(entry->flushed);
}

Datum
mem_collector_features(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    FeatureSnapshot snapshot;
    int i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errmsg("set-valued context required")));
    }

    if ((rsinfo->allowedModes & SFRM_Materialize) == 0) {
        ereport(ERROR, (errmsg("materialize mode required")));
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    tupdesc = CreateTemplateTupleDesc(11);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "feature_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "backend_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "proc_pid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "plan_seq", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "max_sort_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "max_hash_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 8, "max_hashagg_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 9, "parallel_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 10, "is_ap", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 11, "planned_at", TIMESTAMPTZOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    snapshot = mem_collector_snapshot_features();
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    for (i = 0; i < snapshot.count; ++i) {
        Datum values[11];
        bool nulls[11];
        mem_collector_fill_common_feature_values(&snapshot.entries[i], values, nulls);
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    MemoryContextSwitchTo(oldcontext);
    tuplestore_donestoring(tupstore);

    PG_RETURN_NULL();
}

Datum
mem_collector_pending_samples(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    SampleSnapshot snapshot;
    int i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errmsg("set-valued context required")));
    }

    if ((rsinfo->allowedModes & SFRM_Materialize) == 0) {
        ereport(ERROR, (errmsg("materialize mode required")));
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    tupdesc = CreateTemplateTupleDesc(15);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "sample_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "feature_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "backend_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "plan_seq", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "max_sort_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "max_hash_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 8, "max_hashagg_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 9, "parallel_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 10, "is_ap", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 11, "peak_mem_ratio", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 12, "spill_occurred", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 13, "estimation_error", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 14, "collected_at", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 15, "flushed", BOOLOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    snapshot = mem_collector_snapshot_samples();
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    for (i = 0; i < snapshot.count; ++i) {
        Datum values[15];
        bool nulls[15];
        mem_collector_fill_common_sample_values(&snapshot.entries[i], values, nulls);
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    MemoryContextSwitchTo(oldcontext);
    tuplestore_donestoring(tupstore);

    PG_RETURN_NULL();
}

Datum
mem_collector_flush_query_samples(PG_FUNCTION_ARGS)
{
    int limit = PG_GETARG_INT32(0);
    int flushed = 0;

    if (limit <= 0) {
        PG_RETURN_INT32(0);
    }

    mem_collector_flush_samples_via_spi(limit, &flushed);
    PG_RETURN_INT32(flushed);
}

Datum
mem_collector_generate_window_sample(PG_FUNCTION_ARGS)
{
    static const char *window_sql =
        "INSERT INTO mem_window_samples ("
        "tp_concurrency_ratio, ap_count, ap_max_predicted_mem_ratio, "
        "dynamic_pool_current_ratio, dynamic_pool_usage_ratio, shared_pool_usage_ratio, "
        "spill_rate_recent, target_dynamic_pool_ratio, window_start, window_seconds) "
        "SELECT "
        "0.0, "
        "COUNT(*)::int, "
        "COALESCE(MAX(GREATEST(max_sort_ratio, max_hash_ratio, max_hashagg_ratio, peak_mem_ratio)) * 1.2, 0.0), "
        "NULL::float8, NULL::float8, NULL::float8, "
        "COALESCE(AVG(CASE WHEN spill_occurred THEN 1.0 ELSE 0.0 END), 0.0), "
        "NULL::float8, "
        "$1, $2 "
        "FROM mem_query_samples "
        "WHERE collected_at >= $1 "
        "  AND collected_at < $1 + make_interval(secs => $2) "
        "  AND is_ap "
        "RETURNING id";
    Oid argtypes[2] = {TIMESTAMPTZOID, INT4OID};
    Datum values[2];
    char nulls[2] = {' ', ' '};
    TimestampTz window_start = PG_GETARG_TIMESTAMPTZ(0);
    int32 window_seconds = PG_GETARG_INT32(1);
    bool isnull = false;
    int spi_rc;
    Datum result = (Datum) 0;

    if (window_seconds <= 0) {
        ereport(ERROR, (errmsg("window_seconds must be positive")));
    }

    values[0] = TimestampTzGetDatum(window_start);
    values[1] = Int32GetDatum(window_seconds);

    if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("SPI_connect failed while creating window sample")));
    }

    spi_rc = SPI_execute_with_args(window_sql, 2, argtypes, values, nulls, false, 0);
    if (spi_rc != SPI_OK_INSERT_RETURNING || SPI_processed != 1 || SPI_tuptable == NULL) {
        SPI_finish();
        ereport(ERROR, (errmsg("failed to insert mem_window_samples row")));
    }

    result = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    SPI_finish();

    if (isnull) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(DatumGetInt64(result));
}

Datum
mem_collector_reset(PG_FUNCTION_ARGS)
{
    mem_collector_reset_state();
    PG_RETURN_VOID();
}
