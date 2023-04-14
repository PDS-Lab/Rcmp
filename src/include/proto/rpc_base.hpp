#pragma once

#include <functional>

#include "common.hpp"

struct RequestMsg {
    mac_id_t mac_id;

    virtual ~RequestMsg() = default;
};

struct ResponseMsg {
    virtual ~ResponseMsg() = default;
};

namespace detail {

template <typename T>
struct RawResponseReturn {
    T* alloc_flex_resp(size_t flex_size) { return (*__func_flex)(flex_size); }
    std::function<T*(size_t)>* __func_flex;
};

}  // namespace detail
