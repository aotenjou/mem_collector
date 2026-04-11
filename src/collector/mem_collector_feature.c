#include "mem_collector.h"

static void
mem_collector_mark_ap_features(Plan *plan, PlanFeatureAccumulator *acc, double estimated_bytes, double total_bytes)
{
    NodeTag tag;

    if (plan == NULL || acc == NULL) {
        return;
    }

    tag = nodeTag(plan);

    if (tag == T_Sort) {
        acc->max_sort_ratio = Max(acc->max_sort_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
        return;
    }

    if (tag == T_HashJoin || tag == T_Hash) {
        acc->max_hash_ratio = Max(acc->max_hash_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
        return;
    }

    if (tag == T_MergeJoin || tag == T_WindowAgg || tag == T_Group || tag == T_Unique || tag == T_SetOp) {
        acc->max_sort_ratio = Max(acc->max_sort_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
        return;
    }

    if (tag == T_Agg) {
        acc->max_hashagg_ratio = Max(acc->max_hashagg_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
        return;
    }

    if (tag == T_Material) {
        acc->max_sort_ratio = Max(acc->max_sort_ratio, mem_collector_safe_ratio(estimated_bytes, total_bytes));
        acc->is_ap = true;
    }
}

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
    ListCell *lc;

    if (plan == NULL || acc == NULL) {
        return;
    }

    estimated_bytes = plan->plan_rows * (double) Max(plan->plan_width, 0);
    tag = nodeTag(plan);

    acc->parallel_ratio = Max(acc->parallel_ratio, mem_collector_parallel_ratio(plan));

    mem_collector_mark_ap_features(plan, acc, estimated_bytes, total_bytes);

    mem_collector_walk_plan_tree(plan->lefttree, acc, total_bytes);
    mem_collector_walk_plan_tree(plan->righttree, acc, total_bytes);

    if (tag == T_Append) {
        Append *append = (Append *) plan;
        foreach(lc, append->appendplans)
        {
            mem_collector_walk_plan_tree((Plan *) lfirst(lc), acc, total_bytes);
        }
    } else if (tag == T_MergeAppend) {
        MergeAppend *merge_append = (MergeAppend *) plan;
        foreach(lc, merge_append->mergeplans)
        {
            mem_collector_walk_plan_tree((Plan *) lfirst(lc), acc, total_bytes);
        }
    } else if (tag == T_BitmapAnd) {
        BitmapAnd *bitmap_and = (BitmapAnd *) plan;
        foreach(lc, bitmap_and->bitmapplans)
        {
            mem_collector_walk_plan_tree((Plan *) lfirst(lc), acc, total_bytes);
        }
    } else if (tag == T_BitmapOr) {
        BitmapOr *bitmap_or = (BitmapOr *) plan;
        foreach(lc, bitmap_or->bitmapplans)
        {
            mem_collector_walk_plan_tree((Plan *) lfirst(lc), acc, total_bytes);
        }
    } else if (tag == T_SubqueryScan) {
        SubqueryScan *subquery_scan = (SubqueryScan *) plan;
        mem_collector_walk_plan_tree(subquery_scan->subplan, acc, total_bytes);
    } else if (tag == T_CustomScan) {
        CustomScan *custom_scan = (CustomScan *) plan;
        foreach(lc, custom_scan->custom_plans)
        {
            mem_collector_walk_plan_tree((Plan *) lfirst(lc), acc, total_bytes);
        }
    }
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
