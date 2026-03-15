# mem_collector PostgreSQL 12 notes

Date: 2026-03-15

## Scope

`mem_collector` is a PostgreSQL 12 extension loaded through `shared_preload_libraries`.

The current implementation focuses on an end-to-end data collection path for:

- query plan feature capture
- executor-end sample collection
- shared-memory ring buffers
- flushing query samples into SQL tables
- generating workload window samples

Code lives in `mem_collector/`.

## Build and install

Dependencies:

- PostgreSQL 12
- `postgresql-server-dev-12`
- `gcc`
- `make`

Default build setting in `mem_collector/Makefile`:

```make
PG_CONFIG ?= /usr/lib/postgresql/12/bin/pg_config
```

Build:

```bash
cd mem_collector
make
```

Install:

```bash
cd mem_collector
sudo make install
```

## Configuration

Because the extension uses hooks, shared memory, and a background worker, add it to `postgresql.conf`:

```conf
shared_preload_libraries = 'mem_collector'
mem_collector.window_seconds = 30
mem_collector.enable_bgw = on
mem_collector.bgw_dbname = 'postgres'
```

Restart PostgreSQL, then create the extension:

```sql
CREATE EXTENSION mem_collector;
```

## SQL objects

Tables:

- `mem_query_samples`
- `mem_window_samples`

Functions:

- `mem_collector_features()`
- `mem_collector_pending_samples()`
- `mem_collector_flush_query_samples(limit_count integer)`
- `mem_collector_generate_window_sample(window_start timestamptz, window_seconds integer)`
- `mem_collector_reset()`

## Suggested verification

Inspect planner-side features:

```sql
EXPLAIN SELECT * FROM t ORDER BY id;
SELECT * FROM mem_collector_features();
```

Inspect executor-side samples:

```sql
SELECT * FROM mem_collector_pending_samples();
SELECT mem_collector_flush_query_samples(1024);
SELECT * FROM mem_query_samples ORDER BY id DESC LIMIT 5;
```

Inspect window sampling:

```sql
SELECT mem_collector_generate_window_sample(now() - interval '30 seconds', 30);
SELECT * FROM mem_window_samples ORDER BY id DESC LIMIT 5;
```

## Current implementation notes

Implemented:

- planner hook feature capture
- executor-end sample capture
- shared-memory ring buffers
- SQL flush APIs
- background worker window aggregation

Current limitations:

- `peak_mem_ratio` is best-effort rather than kernel-precise
- `spill_occurred` is heuristic for public PostgreSQL 12 structures
- ratio denominators still use host physical memory
- `parallel_ratio` is simplified from `Gather` and `GatherMerge`
- several pool-related window fields are placeholders
- ring buffers are volatile across restart

## Open-source notes

- do not commit local absolute paths, passwords, or private deployment details
- generated build artifacts should stay ignored
- helper scripts should take credentials at runtime instead of embedding them in files
