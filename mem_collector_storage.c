#include "mem_collector.h"

bool
mem_collector_table_exists(const char *relname)
{
    Oid relid = RelnameGetRelid(relname);
    return OidIsValid(relid);
}

void
mem_collector_flush_samples_via_spi(int limit, int *flushed)
{
    static const char *insert_sql =
        "INSERT INTO mem_query_samples ("
        "feature_id, query_id, backend_id, plan_seq, "
        "max_sort_ratio, max_hash_ratio, max_hashagg_ratio, parallel_ratio, is_ap, "
        "peak_mem_ratio, spill_occurred, estimation_error, collected_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)";
    QuerySampleEntry *samples;
    int i;
    int count;
    Oid argtypes[13] = {
        INT8OID, INT8OID, INT4OID, INT8OID,
        FLOAT8OID, FLOAT8OID, FLOAT8OID, FLOAT8OID, BOOLOID,
        FLOAT8OID, BOOLOID, FLOAT8OID, TIMESTAMPTZOID
    };

    if (flushed != NULL) {
        *flushed = 0;
    }
    if (limit <= 0 || !mem_collector_table_exists("mem_query_samples")) {
        return;
    }

    samples = (QuerySampleEntry *) palloc0(sizeof(QuerySampleEntry) * limit);
    count = mem_collector_collect_samples(samples, limit, true);
    if (count == 0) {
        pfree(samples);
        return;
    }

    if (SPI_connect() != SPI_OK_CONNECT) {
        pfree(samples);
        ereport(ERROR, (errmsg("SPI_connect failed while flushing samples")));
    }

    for (i = 0; i < count; ++i) {
        Datum values[13];
        char nulls[13];
        int spi_rc;

        memset(nulls, ' ', sizeof(nulls));
        values[0] = Int64GetDatum((int64) samples[i].feature_id);
        values[1] = Int64GetDatum((int64) samples[i].query_id);
        values[2] = Int32GetDatum(samples[i].backend_id);
        values[3] = Int64GetDatum((int64) samples[i].plan_seq);
        values[4] = Float8GetDatum(samples[i].max_sort_ratio);
        values[5] = Float8GetDatum(samples[i].max_hash_ratio);
        values[6] = Float8GetDatum(samples[i].max_hashagg_ratio);
        values[7] = Float8GetDatum(samples[i].parallel_ratio);
        values[8] = BoolGetDatum(samples[i].is_ap);
        values[9] = Float8GetDatum(samples[i].peak_mem_ratio);
        values[10] = BoolGetDatum(samples[i].spill_occurred);
        values[11] = Float8GetDatum(samples[i].estimation_error);
        values[12] = TimestampTzGetDatum(samples[i].collected_at);

        spi_rc = SPI_execute_with_args(insert_sql, 13, argtypes, values, nulls, false, 0);
        if (spi_rc != SPI_OK_INSERT) {
            SPI_finish();
            pfree(samples);
            ereport(ERROR, (errmsg("failed to insert mem_query_samples row")));
        }

        mem_collector_mark_sample_flushed(samples[i].sample_id);
        if (flushed != NULL) {
            (*flushed)++;
        }
    }

    SPI_finish();
    pfree(samples);
}

void
mem_collector_collect_window_snapshot(TimestampTz window_start, int window_seconds)
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
        "  AND is_ap";
    Oid argtypes[2] = {TIMESTAMPTZOID, INT4OID};
    Datum values[2];
    char nulls[2] = {' ', ' '};
    int spi_rc;

    if (!mem_collector_table_exists("mem_window_samples") || !mem_collector_table_exists("mem_query_samples")) {
        return;
    }

    values[0] = TimestampTzGetDatum(window_start);
    values[1] = Int32GetDatum(window_seconds);

    if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("SPI_connect failed while creating window sample")));
    }

    spi_rc = SPI_execute_with_args(window_sql, 2, argtypes, values, nulls, false, 0);
    if (spi_rc != SPI_OK_INSERT) {
        SPI_finish();
        ereport(ERROR, (errmsg("failed to insert mem_window_samples row")));
    }

    SPI_finish();
}
