#pragma once

#include <cstdint>

#include "config.hpp"
#include "rchms.hpp"

using page_id_t = uint64_t;
using offset_t = uint64_t;
using mac_id_t = uint32_t;

enum SystemRole {
    CN = 1,
    CXL_CN = 2,
    DAEMON = 3,
    CXL_DAEMON = 4,
};

constexpr static mac_id_t master_id = 0;
