#include "proto/rpc_adaptor.hpp"

#include <atomic>
#include <future>

#include "log.hpp"
#include "utils.hpp"

void erpc_general_promise_flag_cb(void *, void *pr) {
    std::promise<void> *pro = reinterpret_cast<std::promise<void> *>(pr);
    pro->set_value();
}

void erpc_general_bool_flag_cb(void *, void *pr) {
    SpinPromise<void> *pro = reinterpret_cast<SpinPromise<void> *>(pr);
    pro->set_value();
}

void msgq_general_promise_flag_cb(msgq::MsgBuffer &resp, void *pr) {
    std::promise<msgq::MsgBuffer> *pro = reinterpret_cast<std::promise<msgq::MsgBuffer> *>(pr);
    pro->set_value(resp);
}

void msgq_general_bool_flag_cb(msgq::MsgBuffer &resp, void *pr) {
    SpinPromise<msgq::MsgBuffer> *pro = reinterpret_cast<SpinPromise<msgq::MsgBuffer> *>(pr);
    pro->set_value(resp);
}