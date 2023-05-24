#include "utils.hpp"

#include <pthread.h>
#include <sched.h>
#include <sys/time.h>

#include <cstdlib>
#include <cstring>

#include "log.hpp"

void threadBindCore(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    int result = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    DLOG_ASSERT(result == 0, "Error: Failed to bind thread to core %d", core_id);
}

uint64_t getTimestamp() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return ((uint64_t)tv.tv_sec * 1000000) + tv.tv_usec;
}

IPv4String::IPv4String(const std::string &ip) { strcpy(raw.ipstr, ip.c_str()); }
IPv4String &IPv4String::operator=(const std::string &ip) {
    strcpy(raw.ipstr, ip.c_str());
    return *this;
}