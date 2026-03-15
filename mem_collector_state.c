#include "mem_collector.h"

void
mem_collector_push_feature(QueryFeatureEntry *entry)
{
    uint64 idx;

    if (mem_state == NULL || entry == NULL) {
        return;
    }

    LWLockAcquire(mem_state->lock, LW_EXCLUSIVE);
    entry->feature_id = ++mem_state->next_feature_id;
    idx = mem_state->feature_write_pos % MEM_COLLECTOR_FEATURE_CAPACITY;
    mem_state->feature_ring[idx] = *entry;
    mem_state->feature_write_pos++;
    LWLockRelease(mem_state->lock);
}

bool
mem_collector_lookup_feature(uint64 feature_id, QueryFeatureEntry *out)
{
    int i;
    bool found = false;

    if (mem_state == NULL || feature_id == 0 || out == NULL) {
        return false;
    }

    LWLockAcquire(mem_state->lock, LW_SHARED);
    for (i = 0; i < MEM_COLLECTOR_FEATURE_CAPACITY; ++i) {
        if (mem_state->feature_ring[i].feature_id == feature_id) {
            *out = mem_state->feature_ring[i];
            found = true;
            break;
        }
    }
    LWLockRelease(mem_state->lock);
    return found;
}

bool
mem_collector_lookup_feature_by_query_id(uint64 query_id, QueryFeatureEntry *out)
{
    int i;
    bool found = false;
    TimestampTz best_ts = 0;

    if (mem_state == NULL || query_id == 0 || out == NULL) {
        return false;
    }

    LWLockAcquire(mem_state->lock, LW_SHARED);
    for (i = 0; i < MEM_COLLECTOR_FEATURE_CAPACITY; ++i) {
        QueryFeatureEntry *entry = &mem_state->feature_ring[i];

        if (entry->feature_id == 0 || entry->query_id != query_id) {
            continue;
        }
        if (!found || entry->planned_at > best_ts) {
            *out = *entry;
            best_ts = entry->planned_at;
            found = true;
        }
    }
    LWLockRelease(mem_state->lock);
    return found;
}

void
mem_collector_push_sample(QuerySampleEntry *entry)
{
    uint64 idx;

    if (mem_state == NULL || entry == NULL) {
        return;
    }

    LWLockAcquire(mem_state->lock, LW_EXCLUSIVE);
    entry->sample_id = ++mem_state->next_sample_id;
    idx = mem_state->sample_write_pos % MEM_COLLECTOR_SAMPLE_CAPACITY;
    mem_state->sample_ring[idx] = *entry;
    mem_state->sample_write_pos++;
    LWLockRelease(mem_state->lock);
}

int
mem_collector_collect_samples(QuerySampleEntry *out, int limit, bool only_unflushed)
{
    int i;
    int count = 0;

    if (mem_state == NULL || out == NULL || limit <= 0) {
        return 0;
    }

    LWLockAcquire(mem_state->lock, LW_SHARED);
    for (i = 0; i < MEM_COLLECTOR_SAMPLE_CAPACITY && count < limit; ++i) {
        QuerySampleEntry *entry = &mem_state->sample_ring[i];
        if (entry->sample_id == 0) {
            continue;
        }
        if (only_unflushed && entry->flushed) {
            continue;
        }
        out[count++] = *entry;
    }
    LWLockRelease(mem_state->lock);
    return count;
}

void
mem_collector_mark_sample_flushed(uint64 sample_id)
{
    int i;

    if (mem_state == NULL || sample_id == 0) {
        return;
    }

    LWLockAcquire(mem_state->lock, LW_EXCLUSIVE);
    for (i = 0; i < MEM_COLLECTOR_SAMPLE_CAPACITY; ++i) {
        if (mem_state->sample_ring[i].sample_id == sample_id) {
            mem_state->sample_ring[i].flushed = true;
            break;
        }
    }
    LWLockRelease(mem_state->lock);
}

FeatureSnapshot
mem_collector_snapshot_features(void)
{
    FeatureSnapshot snapshot;
    int i;

    memset(&snapshot, 0, sizeof(snapshot));
    if (mem_state == NULL) {
        return snapshot;
    }

    LWLockAcquire(mem_state->lock, LW_SHARED);
    for (i = 0; i < MEM_COLLECTOR_FEATURE_CAPACITY; ++i) {
        if (mem_state->feature_ring[i].feature_id == 0) {
            continue;
        }
        snapshot.entries[snapshot.count++] = mem_state->feature_ring[i];
    }
    LWLockRelease(mem_state->lock);
    return snapshot;
}

SampleSnapshot
mem_collector_snapshot_samples(void)
{
    SampleSnapshot snapshot;
    int i;

    memset(&snapshot, 0, sizeof(snapshot));
    if (mem_state == NULL) {
        return snapshot;
    }

    LWLockAcquire(mem_state->lock, LW_SHARED);
    for (i = 0; i < MEM_COLLECTOR_SAMPLE_CAPACITY; ++i) {
        if (mem_state->sample_ring[i].sample_id == 0) {
            continue;
        }
        snapshot.entries[snapshot.count++] = mem_state->sample_ring[i];
    }
    LWLockRelease(mem_state->lock);
    return snapshot;
}

int
mem_collector_active_slot(uintptr_t query_desc_addr, bool create_if_missing)
{
    int free_slot = -1;
    int i;

    for (i = 0; i < MEM_COLLECTOR_ACTIVE_CAPACITY; ++i) {
        if (active_queries[i].in_use && active_queries[i].query_desc_addr == query_desc_addr) {
            return i;
        }
        if (!active_queries[i].in_use && free_slot < 0) {
            free_slot = i;
        }
    }

    if (create_if_missing) {
        return free_slot;
    }
    return -1;
}

void
mem_collector_active_store(uintptr_t query_desc_addr, uint64 feature_id, uint64 query_id, uint64 plan_seq)
{
    int slot = mem_collector_active_slot(query_desc_addr, true);

    if (slot < 0) {
        slot = 0;
    }

    active_queries[slot].query_desc_addr = query_desc_addr;
    active_queries[slot].feature_id = feature_id;
    active_queries[slot].query_id = query_id;
    active_queries[slot].plan_seq = plan_seq;
    active_queries[slot].in_use = true;
}

bool
mem_collector_active_take(uintptr_t query_desc_addr, uint64 *feature_id, uint64 *query_id, uint64 *plan_seq)
{
    int slot = mem_collector_active_slot(query_desc_addr, false);

    if (slot < 0) {
        return false;
    }

    if (feature_id != NULL) {
        *feature_id = active_queries[slot].feature_id;
    }
    if (query_id != NULL) {
        *query_id = active_queries[slot].query_id;
    }
    if (plan_seq != NULL) {
        *plan_seq = active_queries[slot].plan_seq;
    }
    memset(&active_queries[slot], 0, sizeof(active_queries[slot]));
    return true;
}

void
mem_collector_reset_state(void)
{
    if (mem_state != NULL) {
        LWLockAcquire(mem_state->lock, LW_EXCLUSIVE);
        mem_state->next_feature_id = 0;
        mem_state->next_sample_id = 0;
        mem_state->feature_write_pos = 0;
        mem_state->sample_write_pos = 0;
        memset(mem_state->feature_ring, 0, sizeof(mem_state->feature_ring));
        memset(mem_state->sample_ring, 0, sizeof(mem_state->sample_ring));
        LWLockRelease(mem_state->lock);
    }
    memset(active_queries, 0, sizeof(active_queries));
    mem_local_plan_seq = 0;
}
