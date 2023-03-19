#pragma once

#include <cstdint>
#include <config.hpp>

#define CACHE_ALIGN __attribute__((aligned(cache_line_size)))

#define LIKELY __glibc_likely
#define UNLIKELY __glibc_unlikely

void threadBindCore(int core_id);
uint64_t rdtsc();
uint64_t getTimestamp();