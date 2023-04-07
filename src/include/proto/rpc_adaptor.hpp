#pragma once

#include <type_traits>

#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "utils.hpp"

namespace detail {

template <typename EFW, bool ESTABLISH>
void erpc_call_target(erpc::ReqHandle *req_handle, void *context) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(context);
    erpc::ReqHandleWrap req_wrap(req_handle);

    auto resp_raw = req_wrap.get_pre_resp_msgbuf();
    auto req_raw = req_wrap.get_req_msgbuf();

    auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());
    auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());

    typename EFW::PeerContext *peer_connection = nullptr;
    if (ESTABLISH) {
        peer_connection = new typename EFW::PeerContext();
    } else {
        peer_connection =
            dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
    }

    *resp = EFW::func(*self_ctx, *peer_connection, *req);

    auto rpc = self_ctx->get_erpc();
    rpc.resize_msg_buffer(resp_raw, sizeof(typename EFW::ResponseType));
    rpc.enqueue_response(req_wrap, resp_raw);
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

template <typename EFW, bool ESTABLISH>
void msgq_call_target(msgq::MsgBuffer &req_raw, void *ctx) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(ctx);

    auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());

    typename EFW::PeerContext *peer_connection = nullptr;
    msgq::MsgQueueRPC *rpc;
    msgq::MsgBuffer resp_raw;
    if (ESTABLISH) {
        peer_connection = new typename EFW::PeerContext();
        auto reply = EFW::func(*self_ctx, *peer_connection, *req);
        rpc = peer_connection->msgq_rpc;
        
        do
        {
            resp_raw = rpc->alloc_msg_buffer(sizeof(typename EFW::ResponseType));
        } while (!resp_raw.m_msg);
        
        auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());
        
        *resp = reply;
    } else {
        peer_connection =
            dynamic_cast<typename EFW::PeerContext *>(self_ctx->get_connection(req->mac_id));
        rpc = peer_connection->msgq_rpc;
        do
        {
            resp_raw = rpc->alloc_msg_buffer(sizeof(typename EFW::ResponseType));
        } while (!resp_raw.m_msg);
        auto resp = reinterpret_cast<typename EFW::ResponseType *>(resp_raw.get_buf());
        *resp = EFW::func(*self_ctx, *peer_connection, *req);
    }
    rpc->enqueue_response(req_raw, resp_raw);

    // 发送端的buffer将由接收端释放
    rpc->free_msg_buffer(req_raw);
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
    detail::ErpcFuncWrapper<RpcFunc>::registed = true;
    return detail::erpc_call_target<detail::ErpcFuncWrapper<RpcFunc>, ESTABLISH>;
}

template <bool ESTABLISH, typename RpcFunc>
auto bind_msgq_rpc_func(RpcFunc func) {
    DLOG_ASSERT(!detail::MsgqRpcFuncWrapper<RpcFunc>::registed, "function %s has been registed",
                __func__);
    detail::MsgqRpcFuncWrapper<RpcFunc>::func = func;
    detail::MsgqRpcFuncWrapper<RpcFunc>::registed = true;
    return detail::msgq_call_target<detail::MsgqRpcFuncWrapper<RpcFunc>, ESTABLISH>;
}

/**
 * @brief 作为通用的erpc回调函数
 *
 * @param pr std::promise<void>
 */
void erpc_general_promise_flag_cb(void *, void *pr);

void msgq_general_promise_flag_cb(msgq::MsgBuffer &resp, void *arg);