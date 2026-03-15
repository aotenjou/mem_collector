#include "mem_collector.h"

double
mem_collector_parallel_ratio(const Plan *plan)
{
    long cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
    double workers = 0.0;

    if (plan == NULL) {
        return 0.0;
    }

    if (IsA(plan, Gather)) {
        workers = ((const Gather *) plan)->num_workers;
    } else if (IsA(plan, GatherMerge)) {
        workers = ((const GatherMerge *) plan)->num_workers;
    }

    if (workers <= 0.0) {
        return 0.0;
    }

    if (cpu_count <= 0) {
        cpu_count = 1;
    }

    return mem_collector_safe_ratio(workers + 1.0, (double) cpu_count);
}

void
mem_collector_walk_plan_tree(Plan *plan, PlanFeatureAccumulator *acc, double total_bytes)
{
    double estimated_bytes;
    NodeTag tag;

    if (plan == NULL || acc == NULL) {
        return;
    }

    estimated_bytes = plan->plan_rows * (double) Max(plan->plan_width, 0);
    tag = nodeTag(plan);

    acc->parallel_ratio = Max(acc->parallel_ratio, mem_collector_parallel_ratio(plan));

    if (tag == T_Sort) {
        acc->max_sort_ratio = Max(acc->max_sort_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
    } else if (tag == T_HashJoin || tag == T_Hash) {
        acc->max_hash_ratio = Max(acc->max_hash_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
    } else if (tag == T_Agg) {
        Agg *agg = (Agg *) plan;
        if (agg->aggstrategy == AGG_HASHED) {
            acc->max_hashagg_ratio = Max(acc->max_hashagg_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
            acc->is_ap = true;
        }
    }

    mem_collector_walk_plan_tree(plan->lefttree, acc, total_bytes);
    mem_collector_walk_plan_tree(plan->righttree, acc, total_bytes);
}

QueryFeatureEntry
mem_collector_build_feature_from_plannedstmt(PlannedStmt *plannedstmt, int32 backend_id, int32 proc_pid, uint64 plan_seq)
{
    QueryFeatureEntry entry;
    PlanFeatureAccumulator acc;
    double total_bytes = mem_collector_total_memory_bytes();

    memset(&entry, 0, sizeof(entry));
    memset(&acc, 0, sizeof(acc));

    if (plannedstmt != NULL) {
        mem_collector_walk_plan_tree(plannedstmt->planTree, &acc, total_bytes);
        entry.query_id = (uint64) plannedstmt->queryId;
    }

    entry.backend_id = backend_id;
    entry.proc_pid = proc_pid;
    entry.plan_seq = plan_seq;
    entry.max_sort_ratio = acc.max_sort_ratio;
    entry.max_hash_ratio = acc.max_hash_ratio;
    entry.max_hashagg_ratio = acc.max_hashagg_ratio;
    entry.parallel_ratio = acc.parallel_ratio;
    entry.is_ap = acc.is_ap;
    entry.planned_at = GetCurrentTimestamp();

    return entry;
}
