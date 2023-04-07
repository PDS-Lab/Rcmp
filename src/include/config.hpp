#pragma once

#include <cstddef>
#include "rchms.hpp"

/**
 * @brief 页面大小2MB
 */
constexpr static size_t page_size = 2ul << 20;
constexpr static size_t cache_line_size = 64;
constexpr static size_t min_slab_size = 64;

constexpr static size_t offset_bits = __builtin_ffsl(page_size) - 1;
constexpr static size_t page_id_bits = sizeof(rchms::GAddr) * 8 - offset_bits;
