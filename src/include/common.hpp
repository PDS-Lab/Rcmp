#pragma once

#include <cstdint>

#include "config.hpp"
#include "rcmp.hpp"

using page_id_t = uint64_t;
using offset_t = uint64_t;
using mac_id_t = uint32_t;
using rack_id_t = uint32_t;

enum SystemRole : uint8_t {
    MN = 1,
    CN = 2,
    CXL_CN = 3,
    DAEMON = 4,
    CXL_DAEMON = 5,
};

constexpr static mac_id_t master_id = 0;
constexpr static page_id_t invalid_page_id = -1;

union GAddrCombineUnion {
    struct {
        offset_t off : offset_bits;
        page_id_t p : page_id_bits;
    };
    rcmp::GAddr gaddr;
};

inline static page_id_t GetPageID(rcmp::GAddr gaddr) {
    GAddrCombineUnion u;
    u.gaddr = gaddr;
    return u.p;
}
inline static offset_t GetPageOffset(rcmp::GAddr gaddr) {
    GAddrCombineUnion u;
    u.gaddr = gaddr;
    return u.off;
}
inline static rcmp::GAddr GetGAddr(page_id_t page_id, offset_t offset) {
    GAddrCombineUnion u;
    u.p = page_id;
    u.off = offset;
    return u.gaddr;
}

struct RDMARCConnectParam {
    SystemRole role;
    mac_id_t mac_id;
};