
#include "util.h"

/* 2^n size of event ring buffer (in pages) */
#define BUF_SIZE_SHIFT 8

struct perf_fd;

pid_t execute(char **argv, int argc);
void wait_for_child_process(pid_t  pid);
void wait_for_other_process(pid_t  pid);
void write_result_samples(char* filename, struct perf_fd *pfd);
void write_result_nosamples(char* filename, long long cycles, long long cycles_ref, long long inst_retired, long long llc_ref, long long llc_miss);

void open_perf_counter(struct perf_fd *loads, pid_t pid, u64 perf_counter);
void open_perf_counter_load_sampling(struct perf_fd *loads, pid_t pid);

void start_perf_counter(struct perf_fd *loads);
void stop_perf_counter(struct perf_fd *loads);
void close_perf_counter(struct perf_fd *loads);
