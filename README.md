# mem_collector

`mem_collector` is a PostgreSQL 12 extension for collecting query-level memory features and window-level workload snapshots for the memtune pipeline.

It hooks into the planner and executor, keeps recent features and samples in shared-memory ring buffers, writes query samples into SQL tables, and uses a background worker to aggregate window snapshots.

## What It Collects

For each query, `mem_collector` captures:

- planner-time features for memory-heavy operators
- runtime peak memory signals from sort/hash/hash aggregate nodes
- spill hints from sort/hash execution state
- a compact query sample stored in `mem_query_samples`

For each window, `mem_collector` aggregates:

- AP query count
- predicted memory pressure
- recent spill rate
- a snapshot row stored in `mem_window_samples`

## Architecture

The extension uses four pieces:

1. `planner_hook`
   - walks the `PlannedStmt` tree
   - extracts `max_sort_ratio`, `max_hash_ratio`, `max_hashagg_ratio`, `parallel_ratio`, and `is_ap`
   - pushes planner features into a shared-memory ring buffer

2. `ExecutorStart_hook` / `ExecutorEnd_hook`
   - enables executor instrumentation
   - matches the running query to planner-time features
   - traverses the `PlanState` tree at query end
   - records runtime memory and spill signals into a sample ring buffer

3. shared-memory state
   - one feature ring buffer
   - one sample ring buffer
   - protected by a named LWLock tranche

4. background worker
   - wakes up every `mem_collector.window_seconds`
   - flushes pending query samples into `mem_query_samples`
   - builds one window row in `mem_window_samples`

## Source Layout

```text
mem_collector/
├── Makefile
├── README.md
├── mem_collector.h
├── mem_collector.control
├── mem_collector--1.0.sql
├── mem_collector.c
├── mem_collector_common.c
├── mem_collector_state.c
├── mem_collector_feature.c
├── mem_collector_runtime.c
├── mem_collector_storage.c
├── mem_collector_api.c
├── mem_collector_hooks.c
└── mem_collector_bgw.c
```

Module responsibilities:

- `mem_collector.h`: shared types, globals, and cross-file declarations
- `mem_collector_common.c`: shared helpers and common constants usage
- `mem_collector_state.c`: shared memory state, ring buffers, active query tracking
- `mem_collector_feature.c`: planner-side feature extraction
- `mem_collector_runtime.c`: executor runtime memory and spill collection
- `mem_collector_storage.c`: SPI persistence and window snapshot writes
- `mem_collector_api.c`: SQL-visible extension functions
- `mem_collector_hooks.c`: extension init/fini, hook registration, shmem startup
- `mem_collector_bgw.c`: background worker lifecycle and periodic sampling loop
- `mem_collector.c`: lightweight compile entrypoint

## Requirements

- PostgreSQL 12
- `postgresql-server-dev-12`
- `gcc`
- `make`

The default build uses:

```make
PG_CONFIG ?= /usr/lib/postgresql/12/bin/pg_config
```

## Build

```bash
cd mem_collector
make
sudo make install
```

Generated artifacts such as `*.o`, `*.bc`, and `mem_collector.so` are ignored by the repository.

## Repository Hygiene

- keep build outputs out of version control via `.gitignore`
- avoid storing database passwords or connection strings in source files
- prefer passing database credentials at runtime when using helper scripts or SQL clients

## Enable The Extension

Because `mem_collector` uses hooks, shared memory, and a background worker, it must be preloaded by PostgreSQL.

Add this to `postgresql.conf`:

```conf
shared_preload_libraries = 'mem_collector'
mem_collector.window_seconds = 30
mem_collector.enable_bgw = on
mem_collector.bgw_dbname = 'postgres'
```

Then restart PostgreSQL and create the extension:

```sql
CREATE EXTENSION mem_collector;
```

## SQL Objects

### Tables

- `mem_query_samples`
- `mem_window_samples`

### Functions

- `mem_collector_features()`
- `mem_collector_pending_samples()`
- `mem_collector_flush_query_samples(limit_count integer)`
- `mem_collector_generate_window_sample(window_start timestamptz, window_seconds integer)`
- `mem_collector_reset()`

## Quick Verification

Run a sort-heavy query:

```sql
EXPLAIN SELECT * FROM t ORDER BY id;
SELECT * FROM mem_collector_features();
```

Run executor-side checks:

```sql
SELECT * FROM mem_collector_pending_samples();
SELECT mem_collector_flush_query_samples(1024);
SELECT * FROM mem_query_samples ORDER BY id DESC LIMIT 5;
```

Run window aggregation checks:

```sql
SELECT mem_collector_generate_window_sample(now() - interval '30 seconds', 30);
SELECT * FROM mem_window_samples ORDER BY id DESC LIMIT 5;
```

If the background worker is enabled, query samples and window samples should also appear automatically over time.

## Current Implementation Notes

### Planner Features

The current planner-side extractor walks the plan tree and focuses on:

- `T_Sort`
- `T_HashJoin`
- `T_Hash`
- `T_Agg` with hashed aggregation
- `Gather` / `GatherMerge` for simplified parallel ratio estimation

### Runtime Memory Collection

The executor-side collector uses PostgreSQL 12 public structures where possible:

- `tuplesort_get_stats()` for sort memory and sort spill hints
- `HashJoinState->hj_HashTable->spacePeak` and `nbatch`
- `HashState->hashtable` and `hinstrument`
- conservative handling for hashed aggregate state

### Window Sampling

The background worker currently writes one row per time window using recent rows from `mem_query_samples`.

The following fields are intentionally left conservative or unimplemented for now:

- `tp_concurrency_ratio`
- `dynamic_pool_current_ratio`
- `dynamic_pool_usage_ratio`
- `shared_pool_usage_ratio`
- `target_dynamic_pool_ratio`

## Limitations

This version is designed to make the PostgreSQL plugin path work end-to-end, but it is not yet a perfect replacement for the original openGauss-oriented design.

- `peak_mem_ratio` is best-effort and more conservative than a kernel-private implementation
- `spill_occurred` is a heuristic based on public executor state, not a full engine-level spill truth source
- ratio denominators still use host physical memory rather than a PostgreSQL memory-pool budget
- `parallel_ratio` is simplified and only inferred from `Gather` / `GatherMerge`
- window-level TP pressure and pool-usage fields are placeholders today
- ring buffers are in shared memory and therefore volatile across restart

In short, the current plugin is good for data collection, experimentation, and pipeline integration, but not yet a production-grade memory observability extension.

## Development Status

Implemented:

- PG12 extension skeleton
- `planner_hook` feature capture
- executor-end sample capture
- shared-memory ring buffers
- SQL flush APIs
- background worker window aggregation

Recommended next steps:

1. improve hashed aggregate peak-memory accuracy
2. refine spill detection semantics
3. connect SQL fingerprinting if stable query identity is needed
4. replace host-memory denominator with a PostgreSQL-specific budget model
5. add integration tests for extension load, hook execution, and BGW output

## License

MIT. See `LICENSE`.
