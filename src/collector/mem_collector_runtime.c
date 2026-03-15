#include "mem_collector.h"

double
mem_collector_estimation_ratio(const Plan *plan, const Instrumentation *instrument)
{
    double estimated_rows;
    double actual_rows;

    if (plan == NULL || instrument == NULL) {
        return 0.0;
    }

    estimated_rows = plan->plan_rows;
    actual_rows = instrument->ntuples;

    if (estimated_rows <= 0.0 || actual_rows <= 0.0) {
        return 0.0;
    }

    if (actual_rows > estimated_rows) {
        return actual_rows / estimated_rows;
    }
    return estimated_rows / actual_rows;
}

Size
mem_collector_sort_peak_bytes(SortState *sortstate, bool *spill_occurred)
{
    Size peak = 0;

    if (sortstate == NULL || sortstate->tuplesortstate == NULL) {
        return 0;
    }

    if (sortstate->shared_info != NULL) {
        int i;

        for (i = 0; i < sortstate->shared_info->num_workers; ++i) {
            TuplesortInstrumentation *stats = &sortstate->shared_info->sinstrument[i];
            Size bytes = (Size) stats->spaceUsed * (Size) 1024;

            peak = Max(peak, bytes);
            if (spill_occurred != NULL && stats->spaceType == SORT_SPACE_TYPE_DISK) {
                *spill_occurred = true;
            }
        }
    }

    {
        TuplesortInstrumentation stats;
        memset(&stats, 0, sizeof(stats));
        tuplesort_get_stats((Tuplesortstate *) sortstate->tuplesortstate, &stats);
        peak = Max(peak, (Size) stats.spaceUsed * (Size) 1024);
        if (spill_occurred != NULL && stats.spaceType == SORT_SPACE_TYPE_DISK) {
            *spill_occurred = true;
        }
    }

    return peak;
}

Size
mem_collector_hashjoin_peak_bytes(HashJoinState *hashjoinstate, bool *spill_occurred)
{
    if (hashjoinstate == NULL || hashjoinstate->hj_HashTable == NULL) {
        return 0;
    }

    if (spill_occurred != NULL && hashjoinstate->hj_HashTable->nbatch > 1) {
        *spill_occurred = true;
    }

    return hashjoinstate->hj_HashTable->spacePeak;
}

Size
mem_collector_hash_peak_bytes(HashState *hashstate, bool *spill_occurred)
{
    Size peak = 0;

    if (hashstate == NULL) {
        return 0;
    }

    if (hashstate->hashtable != NULL) {
        peak = Max(peak, hashstate->hashtable->spacePeak);
        if (spill_occurred != NULL && hashstate->hashtable->nbatch > 1) {
            *spill_occurred = true;
        }
    }

    if (hashstate->hinstrument != NULL) {
        peak = Max(peak, (Size) hashstate->hinstrument->space_peak);
        if (spill_occurred != NULL && hashstate->hinstrument->nbatch > 1) {
            *spill_occurred = true;
        }
    }

    return peak;
}

Size
mem_collector_agg_peak_bytes(AggState *aggstate, bool *spill_occurred)
{
    Size peak = 0;

    if (aggstate == NULL) {
        return 0;
    }

    if (aggstate->aggstrategy != AGG_HASHED && aggstate->aggstrategy != AGG_MIXED) {
        return 0;
    }

    if (spill_occurred != NULL && aggstate->sort_out != NULL) {
        TuplesortInstrumentation stats;
        memset(&stats, 0, sizeof(stats));
        tuplesort_get_stats(aggstate->sort_out, &stats);
        if (stats.spaceType == SORT_SPACE_TYPE_DISK) {
            *spill_occurred = true;
        }
        peak = Max(peak, (Size) stats.spaceUsed * (Size) 1024);
    }

    return peak;
}

void
mem_collector_walk_planstate_tree(PlanState *planstate, RuntimeAccumulator *acc, double total_bytes)
{
    Plan *plan;
    Instrumentation *instrument;
    double error_ratio;
    Size operator_bytes = 0;
    bool operator_spill = false;

    if (planstate == NULL || acc == NULL) {
        return;
    }

    plan = planstate->plan;
    instrument = planstate->instrument;

    if (plan != NULL && instrument != NULL) {
        error_ratio = mem_collector_estimation_ratio(plan, instrument);
        if (error_ratio > acc->estimation_error) {
            acc->estimation_error = error_ratio;
        }
    }

    if (IsA(planstate, SortState)) {
        operator_bytes = mem_collector_sort_peak_bytes((SortState *) planstate, &operator_spill);
    } else if (IsA(planstate, HashJoinState)) {
        operator_bytes = mem_collector_hashjoin_peak_bytes((HashJoinState *) planstate, &operator_spill);
    } else if (IsA(planstate, HashState)) {
        operator_bytes = mem_collector_hash_peak_bytes((HashState *) planstate, &operator_spill);
    } else if (IsA(planstate, AggState)) {
        operator_bytes = mem_collector_agg_peak_bytes((AggState *) planstate, &operator_spill);
        if (operator_bytes == 0 && plan != NULL && IsA(plan, Agg) && ((Agg *) plan)->aggstrategy == AGG_HASHED) {
            operator_bytes = (Size) Max(plan->plan_rows, 0.0) * (Size) Max(plan->plan_width, 0);
        }
    }

    if (operator_bytes > 0) {
        double operator_ratio = mem_collector_safe_ratio((double) operator_bytes, total_bytes);
        if (operator_ratio > acc->peak_mem_ratio) {
            acc->peak_mem_ratio = operator_ratio;
        }
    }
    if (operator_spill) {
        acc->spill_occurred = true;
    }

    mem_collector_walk_planstate_tree(planstate->lefttree, acc, total_bytes);
    mem_collector_walk_planstate_tree(planstate->righttree, acc, total_bytes);
}
