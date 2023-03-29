#pragma once

#include <config.hpp>
#include <cstdint>

#define CACHE_ALIGN __attribute__((aligned(cache_line_size)))

#define LIKELY __glibc_likely
#define UNLIKELY __glibc_unlikely

template <typename D>
D div_floor(D x, uint64_t div) {
    return (x + div - 1) / div;
}

template <typename D>
D align_by(D x, uint64_t aligned) {
    return div_floor(x, aligned) * aligned;
}

void threadBindCore(int core_id);
uint64_t rdtsc();
uint64_t getTimestamp();