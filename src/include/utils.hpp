#pragma once

#include <cstdint>

#define LIKELY __glibc_likely
#define UNLIKELY __glibc_unlikely

void thread_bind_core(int core_id);

uint64_t rdtsc();
uint64_t get_now_time();