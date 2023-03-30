#pragma once

#include "common.hpp"

struct RequestMsg {
    mac_id_t mac_id;

    virtual ~RequestMsg() = default;
};

struct ResponseMsg {
    virtual ~ResponseMsg() = default;
};