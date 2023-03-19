#include "utils.hpp"

#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <cstdlib>
#include "log.hpp"

void threadBindCore(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    int result = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    DLOG_ASSERT(result == 0, "Error: Failed to bind thread to core %d", core_id);
}

uint64_t rdtsc() {
    uint32_t low, high;
    asm volatile("rdtsc" : "=a"(low), "=d"(high));
    return ((uint64_t)high << 32) | low;
}

uint64_t getTimestamp() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return ((uint64_t)tv.tv_sec << 32) | tv.tv_usec;
}