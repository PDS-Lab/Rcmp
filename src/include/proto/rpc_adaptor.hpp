#pragma once

#include <type_traits>

#include "cort_sched.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "proto/rpc_base.hpp"
#include "utils.hpp"

namespace detail {

template <
    typename EFW, bool ESTABLISH,
    typename std::enable_if<!std::is_base_of<detail::RawResponseReturn<typename EFW::ResponseType>,
                                             typename EFW::RequestType>::value,
                            bool>::type = true>
void erpc_call_target(erpc::ReqHandle *req_handle, void *context) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(context);

    CortScheduler &cort_sched = self_ctx->get_cort_sched();
    cort_sched.addTask([self_ctx, req_handle]() {
        erpc::ReqHandleWrap req_wrap(req_handle);

        auto resp_raw = req_wrap.get_pre_resp_msgbuf();
        auto req_raw = req_wrap.get_req_msgbuf();

        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());
        auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;
        auto &rpc = self_ctx->get_erpc();

        if (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
        } else {
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
        }

        *resp = EFW::func(*self_ctx, *peer_connection, *req);

        rpc.resize_msg_buffer(resp_raw, sizeof(typename EFW::ResponseType));
        rpc.enqueue_response(req_wrap, resp_raw);
    });
}

template <
    typename EFW, bool ESTABLISH,
    typename std::enable_if<std::is_base_of<detail::RawResponseReturn<typename EFW::ResponseType>,
                                            typename EFW::RequestType>::value,
                            bool>::type = true>
void erpc_call_target(erpc::ReqHandle *req_handle, void *context) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(context);

    CortScheduler &cort_sched = self_ctx->get_cort_sched();
    cort_sched.addTask([self_ctx, req_handle]() {
        erpc::ReqHandleWrap req_wrap(req_handle);

        auto resp_raw = req_wrap.get_pre_resp_msgbuf();
        auto req_raw = req_wrap.get_req_msgbuf();

        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());
        auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;
        auto &rpc = self_ctx->get_erpc();

        auto alloc_fn = std::function<typename EFW::ResponseType *(size_t)>([&](size_t s) {
            rpc.resize_msg_buffer(resp_raw, sizeof(typename EFW::ResponseType) + s);
            return reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());
        });
        req->__func_flex = &alloc_fn;

        if (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
        } else {
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
        }

        EFW::func(*self_ctx, *peer_connection, *req);
        rpc.enqueue_response(req_wrap, resp_raw);
    });
}

template <typename RpcFunc>
struct ErpcFuncWrapper {
    using FT = function_traits<RpcFunc>;
    using SelfContext = typename std::remove_reference<typename FT::template args_type<0>>::type;
    using PeerContext = typename std::remove_reference<typename FT::template args_type<1>>::type;
    using RequestType = typename std::remove_reference<typename FT::template args_type<2>>::type;
    using ResponseType = typename std::remove_reference<typename FT::result_type>::type;

    static RpcFunc func;
    static bool registed;
};

template <typename RpcFunc>
bool ErpcFuncWrapper<RpcFunc>::registed = false;
template <typename RpcFunc>
RpcFunc ErpcFuncWrapper<RpcFunc>::func;

template <
    typename EFW, bool ESTABLISH,
    typename std::enable_if<!std::is_base_of<detail::RawResponseReturn<typename EFW::ResponseType>,
                                             typename EFW::RequestType>::value,
                            bool>::type = true>
void msgq_call_target(msgq::MsgBuffer &req_raw, void *ctx) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(ctx);

    CortScheduler &cort_sched = self_ctx->get_cort_sched();
    // mutable防止引用析构
    cort_sched.addTask([self_ctx, req_raw]() mutable {
        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;
        msgq::MsgQueueRPC *rpc;
        msgq::MsgBuffer resp_raw;
        if (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
            auto reply = EFW::func(*self_ctx, *peer_connection, *req);
            rpc = peer_connection->msgq_rpc;
            resp_raw = rpc->alloc_msg_buffer(sizeof(typename EFW::ResponseType));
            auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());

            *resp = reply;
        } else {
            /**
             * 为了减少从func返回值到msg buffer的拷贝，总是优先申请，然后将返回值直接填入
             * ?
             * 可能存在func是一个长请求，导致优先申请的msg暂时无法发送，使得后面的msg发送阻塞（队头阻塞）
             */
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
            rpc = peer_connection->msgq_rpc;
            resp_raw = rpc->alloc_msg_buffer(sizeof(typename EFW::ResponseType));
            auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());
            *resp = EFW::func(*self_ctx, *peer_connection, *req);
        }

        rpc->enqueue_response(req_raw, resp_raw);

        // 发送端的buffer将由接收端释放
        rpc->free_msg_buffer(req_raw);
    });
}

template <
    typename EFW, bool ESTABLISH,
    typename std::enable_if<std::is_base_of<detail::RawResponseReturn<typename EFW::ResponseType>,
                                            typename EFW::RequestType>::value,
                            bool>::type = true>
void msgq_call_target(msgq::MsgBuffer &req_raw, void *ctx) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(ctx);

    CortScheduler &cort_sched = self_ctx->get_cort_sched();
    // mutable防止引用析构
    cort_sched.addTask([self_ctx, req_raw]() mutable {
        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;
        msgq::MsgQueueRPC *rpc;
        msgq::MsgBuffer resp_raw;

        auto alloc_fn = std::function<typename EFW::ResponseType *(size_t)>([&](size_t s) {
            resp_raw =
                peer_connection->msgq_rpc->alloc_msg_buffer(sizeof(typename EFW::ResponseType) + s);
            return reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());
        });
        req->__func_flex = &alloc_fn;

        if (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
        } else {
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
        }
        rpc = peer_connection->msgq_rpc;
        EFW::func(*self_ctx, *peer_connection, *req);
        rpc->enqueue_response(req_raw, resp_raw);

        // 发送端的buffer将由接收端释放
        rpc->free_msg_buffer(req_raw);
    });
}

template <typename RpcFunc>
struct MsgqRpcFuncWrapper {
    using FT = function_traits<RpcFunc>;
    using SelfContext = typename std::remove_reference<typename FT::template args_type<0>>::type;
    using PeerContext = typename std::remove_reference<typename FT::template args_type<1>>::type;
    using RequestType = typename std::remove_reference<typename FT::template args_type<2>>::type;
    using ResponseType = typename std::remove_reference<typename FT::result_type>::type;

    static RpcFunc func;
    static bool registed;
};

template <typename RpcFunc>
bool MsgqRpcFuncWrapper<RpcFunc>::registed = false;
template <typename RpcFunc>
RpcFunc MsgqRpcFuncWrapper<RpcFunc>::func;

}  // namespace detail

/**
 * @brief 使用erpc的处理函数类型，绑定proto中定义的rpc处理函数。
 *
 * @warning 每种func仅能bind一次，且要求不同的rpc func有不同的入参类型。这代表每种rpc
 * handler都需要定义`Request`与`Response`结构体。
 *
 * @tparam ESTABLISH 是否为建立通信的RPC请求。在`ESTABLISH`为`true`时，hanlder会从heap中申请peer
 * connection加入到连接表中。
 * @tparam RpcFunc
 * @param func
 * @return auto erpc的处理函数类型
 */
template <bool ESTABLISH, typename RpcFunc>
auto bind_erpc_func(RpcFunc func) {
    DLOG_ASSERT(!detail::ErpcFuncWrapper<RpcFunc>::registed, "function %s has been registed",
                __func__);
    detail::ErpcFuncWrapper<RpcFunc>::func = func;
    return detail::erpc_call_target<detail::ErpcFuncWrapper<RpcFunc>, ESTABLISH>;
}

template <bool ESTABLISH, typename RpcFunc>
auto bind_msgq_rpc_func(RpcFunc func) {
    DLOG_ASSERT(!detail::MsgqRpcFuncWrapper<RpcFunc>::registed, "function %s has been registed",
                __func__);
    detail::MsgqRpcFuncWrapper<RpcFunc>::func = func;
    return detail::msgq_call_target<detail::MsgqRpcFuncWrapper<RpcFunc>, ESTABLISH>;
}

/**
 * @brief 作为通用的erpc回调函数
 *
 * @param pr std::promise<void>
 */
void erpc_general_promise_flag_cb(void *, void *pr);

void erpc_general_bool_flag_cb(void *, void *b);

void msgq_general_promise_flag_cb(msgq::MsgBuffer &resp, void *arg);

void msgq_general_bool_flag_cb(msgq::MsgBuffer &resp, void *pr);