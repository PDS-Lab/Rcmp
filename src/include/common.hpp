#pragma once

#include <cstdint>

#include "config.hpp"
#include "rchms.hpp"

using page_id_t = uint64_t;
using offset_t = uint64_t;
using mac_id_t = uint32_t;
using rack_id_t = uint32_t;

enum SystemRole {
    CN = 1,
    CXL_CN = 2,
    DAEMON = 3,
    CXL_DAEMON = 4,
};

constexpr static mac_id_t master_id = 0;
constexpr static page_id_t invalid_page_id = -1;

union GAddrCombineUnion {
    struct {
        page_id_t p : page_id_bits;
        offset_t off : offset_bits;
    };
    rchms::GAddr gaddr;
};

inline static page_id_t GetPageID(rchms::GAddr gaddr) {
    GAddrCombineUnion u;
    u.gaddr = gaddr;
    return u.p;
}
inline static offset_t GetPageOffset(rchms::GAddr gaddr) {
    GAddrCombineUnion u;
    u.gaddr = gaddr;
    return u.off;
}
inline static rchms::GAddr GetGAddr(page_id_t page_id, offset_t offset) {
    GAddrCombineUnion u;
    u.p = page_id;
    u.off = offset;
    return u.gaddr;
}