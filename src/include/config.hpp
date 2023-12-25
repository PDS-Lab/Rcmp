#pragma once

#include <cstddef>

#include "rcmp.hpp"

#define MSGQ_SINGLE_FIFO_ON 0

constexpr static size_t page_size = 4ul << 10;
constexpr static size_t cache_line_size = 64;
constexpr static size_t min_slab_size = 64;
constexpr static size_t mem_region_aligned_size = 2ul << 30;

constexpr static size_t offset_bits = __builtin_ffsl(page_size) - 1;
constexpr static size_t page_id_bits = sizeof(rcmp::GAddr) * 8 - offset_bits;

constexpr static size_t msgq_ring_buf_len = 16ul << 20;
constexpr static size_t msgq_ring_depth = 256;
constexpr static size_t write_batch_buffer_size = 64ul << 20;
constexpr static size_t write_batch_buffer_overflow_size = 2ul << 20;

constexpr static size_t get_page_cxl_ref_or_proxy_write_raw_max_size = UINT64_MAX;

/**
 * @brief Intervals before and after heat statisticsus
 */
constexpr static size_t hot_stat_freq_timeout_interval = 100;