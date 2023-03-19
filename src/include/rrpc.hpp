#pragma once

#include <cstdint>

struct RdmaRef {
    uintptr_t addr;
    uint32_t rkey;
};