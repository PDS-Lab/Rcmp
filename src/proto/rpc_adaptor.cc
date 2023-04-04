#include "proto/rpc_adaptor.hpp"

#include <future>

void erpc_general_promise_flag_cb(void *, void *pr) {
    std::promise<void> *pro = reinterpret_cast<std::promise<void> *>(pr);
    pro->set_value();
}

void msgq_general_promise_flag_cb(msgq::MsgBuffer &resp, void *pr) {
    std::promise<msgq::MsgBuffer> *pro = reinterpret_cast<std::promise<msgq::MsgBuffer> *>(pr);
    pro->set_value(resp);
}