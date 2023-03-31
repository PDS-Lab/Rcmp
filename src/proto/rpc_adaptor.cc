#include "proto/rpc_adaptor.hpp"

#include <future>

void erpc_general_promise_flag_cb(void *, void *pr) {
    std::promise<void> *pro = reinterpret_cast<std::promise<void> *>(pr);
    pro->set_value();
}