CREATE TABLE IF NOT EXISTS mem_query_samples (
    id                  bigserial PRIMARY KEY,
    feature_id          bigint NOT NULL,
    query_id            bigint NOT NULL,
    backend_id          integer NOT NULL,
    plan_seq            bigint NOT NULL,
    max_sort_ratio      float8 NOT NULL,
    max_hash_ratio      float8 NOT NULL,
    max_hashagg_ratio   float8 NOT NULL,
    parallel_ratio      float8 NOT NULL,
    is_ap               boolean NOT NULL,
    peak_mem_ratio      float8 NOT NULL,
    spill_occurred      boolean NOT NULL,
    estimation_error    float8 NOT NULL,
    collected_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS mem_query_samples_collected_at_idx
    ON mem_query_samples (collected_at);

CREATE INDEX IF NOT EXISTS mem_query_samples_query_key_idx
    ON mem_query_samples (query_id, backend_id, plan_seq);

CREATE TABLE IF NOT EXISTS mem_window_samples (
    id                          bigserial PRIMARY KEY,
    tp_concurrency_ratio        float8,
    ap_count                    integer NOT NULL,
    ap_max_predicted_mem_ratio  float8 NOT NULL,
    dynamic_pool_current_ratio  float8,
    dynamic_pool_usage_ratio    float8,
    shared_pool_usage_ratio     float8,
    spill_rate_recent           float8 NOT NULL,
    target_dynamic_pool_ratio   float8,
    window_start                timestamptz NOT NULL,
    window_seconds              integer NOT NULL DEFAULT 30
);

CREATE INDEX IF NOT EXISTS mem_window_samples_window_start_idx
    ON mem_window_samples (window_start);

CREATE FUNCTION mem_collector_features()
RETURNS TABLE (
    feature_id bigint,
    query_id bigint,
    backend_id integer,
    proc_pid integer,
    plan_seq bigint,
    max_sort_ratio float8,
    max_hash_ratio float8,
    max_hashagg_ratio float8,
    parallel_ratio float8,
    is_ap boolean,
    planned_at timestamptz
)
AS 'MODULE_PATHNAME', 'mem_collector_features'
LANGUAGE C STRICT;

CREATE FUNCTION mem_collector_pending_samples()
RETURNS TABLE (
    sample_id bigint,
    feature_id bigint,
    query_id bigint,
    backend_id integer,
    plan_seq bigint,
    max_sort_ratio float8,
    max_hash_ratio float8,
    max_hashagg_ratio float8,
    parallel_ratio float8,
    is_ap boolean,
    peak_mem_ratio float8,
    spill_occurred boolean,
    estimation_error float8,
    collected_at timestamptz,
    flushed boolean
)
AS 'MODULE_PATHNAME', 'mem_collector_pending_samples'
LANGUAGE C STRICT;

CREATE FUNCTION mem_collector_flush_query_samples(limit_count integer DEFAULT 1024)
RETURNS integer
AS 'MODULE_PATHNAME', 'mem_collector_flush_query_samples'
LANGUAGE C;

CREATE FUNCTION mem_collector_generate_window_sample(
    window_start timestamptz DEFAULT now(),
    window_seconds integer DEFAULT 30
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'mem_collector_generate_window_sample'
LANGUAGE C;

CREATE FUNCTION mem_collector_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'mem_collector_reset'
LANGUAGE C;
