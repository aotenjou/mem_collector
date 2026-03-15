#include "mem_collector.h"

static void
mem_collector_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;

    mem_collector_got_sigterm = true;
    if (MyLatch != NULL) {
        SetLatch(MyLatch);
    }

    errno = save_errno;
}

void
mem_collector_bgw_main(Datum main_arg)
{
    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, mem_collector_sigterm);

    BackgroundWorkerInitializeConnection(mem_collector_bgw_dbname, NULL, 0);

    while (!mem_collector_got_sigterm) {
        TimestampTz now;
        TimestampTz window_start;

        ResetLatch(MyLatch);

        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        mem_collector_flush_samples_via_spi(MEM_COLLECTOR_SAMPLE_CAPACITY, NULL);

        now = GetCurrentTimestamp();
        window_start = TimestampTzPlusMilliseconds(now, -((int64) mem_collector_window_seconds * 1000));
        mem_collector_collect_window_snapshot(window_start, mem_collector_window_seconds);

        PopActiveSnapshot();
        CommitTransactionCommand();

        if (mem_collector_got_sigterm) {
            break;
        }

        if (WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                      mem_collector_window_seconds * 1000L,
                      PG_WAIT_EXTENSION) & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }
    }

    proc_exit(0);
}
