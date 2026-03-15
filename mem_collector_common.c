#include "mem_collector.h"

MemCollectorSharedState *mem_state = NULL;
uint64 mem_local_plan_seq = 0;
ActiveQueryEntry active_queries[MEM_COLLECTOR_ACTIVE_CAPACITY];

int mem_collector_window_seconds = 30;
bool mem_collector_bgw_enabled = true;
char *mem_collector_bgw_dbname = MEM_COLLECTOR_BGW_DBNAME;
volatile sig_atomic_t mem_collector_got_sigterm = false;

Size
mem_collector_shmem_size(void)
{
    return MAXALIGN(sizeof(MemCollectorSharedState));
}

double
mem_collector_total_memory_bytes(void)
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGESIZE);

    if (pages <= 0 || page_size <= 0) {
        return 1.0;
    }

    return (double) pages * (double) page_size;
}

double
mem_collector_safe_ratio(double value, double total)
{
    double ratio;

    if (total <= 0.0 || value <= 0.0) {
        return 0.0;
    }

    ratio = value / total;
    if (ratio < 0.0) {
        return 0.0;
    }
    if (ratio > 1.0) {
        return 1.0;
    }
    return ratio;
}
