#pragma once

#include <cstddef>

#include "common.hpp"
#include "rchms.hpp"

/**
 * @brief 页面大小2MB
 */
constexpr static size_t page_size = 2ul << 20;
constexpr static size_t cache_line_size = 64;

constexpr static size_t page_id_bits = __builtin_ffsl(page_size) - 2;
constexpr static size_t offset_bits = sizeof(rchms::GAddr) * 8 - page_id_bits;
