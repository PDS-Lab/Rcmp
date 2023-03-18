#pragma once

#include <cstdint>

#include "config.hpp"
#include "rchms.hpp"

namespace rchms {

using page_id_t = uint64_t;
using offset_t = uint64_t;

enum SystemRole {
    CN = 1,
    CXL_CN = 2,
    DAEMON = 3,
    CXL_DAEMON = 4,
};

}  // namespace rchms