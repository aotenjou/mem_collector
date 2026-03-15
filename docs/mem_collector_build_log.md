# mem_collector build log

Date: 2026-03-15

## Added in this iteration

### Extension directory

Created `mem_collector/` with:

- `Makefile`
- `mem_collector.control`
- `mem_collector--1.0.sql`
- `mem_collector.c`
- `mem_collector.h`
- `mem_collector_common.c`
- `mem_collector_state.c`
- `mem_collector_feature.c`
- `mem_collector_runtime.c`
- `mem_collector_storage.c`
- `mem_collector_api.c`
- `mem_collector_hooks.c`
- `mem_collector_bgw.c`

Implemented:

- extension control file and SQL install script
- shared memory state and ring buffers
- planner-side feature capture
- executor-end runtime sample collection
- SQL interfaces for flushing and inspection
- background worker window aggregation

### Pipeline helpers

Created `pipeline/` helpers for:

- exporting query and window samples to JSONL
- filtering query and window samples
- generating manual window samples
- labeling window samples
- splitting datasets by time order

## Validation

### Python syntax

Ran:

```bash
python3 -m py_compile \
  pipeline/export_query_samples.py \
  pipeline/filter_samples.py \
  pipeline/split_dataset.py \
  pipeline/label_window_samples.py \
  pipeline/generate_window_samples.py
```

Result: passed.

### Extension build

Ran:

```bash
cd mem_collector
make clean
make
```

Result: passed with PostgreSQL 12 development headers available locally.

## Notes

- build outputs such as `*.o`, `*.bc`, and `mem_collector.so` should remain untracked
- repository docs were normalized to remove local machine paths and stale openGauss-specific notes
- no embedded credentials were found in the extension sources or pipeline helpers
