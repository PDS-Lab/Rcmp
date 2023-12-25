#pragma once

#include <boost/fiber/future/async.hpp>
#include <future>
#include <type_traits>

#include "allocator.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "proto/rpc_caller.hpp"
#include "utils.hpp"

template <typename ResponseType>
struct ResponseHandle {
    virtual void Init(size_t flex_size = 0) = 0;
    virtual ResponseType &Get() = 0;
};

struct ErpcClient;
struct MsgQClient;

namespace detail {

template <typename ResponseType>
struct ErpcResponseHandle : public ResponseHandle<ResponseType> {
    ErpcResponseHandle(erpc::IBRpcWrap &rpc, erpc::ReqHandleWrap req_wrap)
        : state(false), rpc(&rpc), req_wrap(req_wrap), resp_raw(nullptr) {}

    virtual void Init(size_t flex_size = 0) override {
        DLOG_ASSERT(state == false, "Double Init");

        resp_raw = req_wrap.get_pre_resp_msgbuf();
        rpc->resize_msg_buffer(resp_raw, sizeof(ResponseType) + flex_size);
        state = true;
    }

    virtual ResponseType &Get() override {
        DLOG_ASSERT(state == true, "Not init yet");
        return *reinterpret_cast<ResponseType *>(resp_raw.get_buf());
    }

    erpc::MsgBufferWrap GetBuffer() const {
        DLOG_ASSERT(state == true, "Not init yet");
        return resp_raw;
    }

    bool state;
    erpc::IBRpcWrap *rpc;
    erpc::ReqHandleWrap req_wrap;
    erpc::MsgBufferWrap resp_raw;
};

template <typename ResponseType>
struct RawResponseHandle : public ResponseHandle<ResponseType>, NOCOPYABLE {
    RawResponseHandle() : state(false), resp_raw(nullptr) {}
    ~RawResponseHandle() {
        if (resp_raw) {
            ::operator delete(resp_raw);
        }
    }

    virtual void Init(size_t flex_size = 0) override {
        DLOG_ASSERT(state == false, "Double Init");

        init_size = sizeof(ResponseType) + flex_size;

        resp_raw = ::operator new(init_size);
        state = true;
    }

    virtual ResponseType &Get() override {
        DLOG_ASSERT(state == true, "Not init yet");
        return *reinterpret_cast<ResponseType *>(resp_raw);
    }

    void *GetBuffer() {
        DLOG_ASSERT(state == true, "Not init yet");
        return resp_raw;
    }

    size_t GetSize() const { return init_size; }

    bool state;
    size_t init_size;
    void *resp_raw;
};

template <typename ResponseType>
struct MsgQResponseHandle : public ResponseHandle<ResponseType> {
    MsgQResponseHandle(msgq::MsgQueueRPC *rpc) : state(false), rpc(rpc) {}

    virtual void Init(size_t flex_size = 0) override {
        DLOG_ASSERT(state == false, "Double Init");

        resp_raw = rpc->alloc_msg_buffer(sizeof(ResponseType) + flex_size);
        state = true;
    }

    virtual ResponseType &Get() override {
        DLOG_ASSERT(state == true, "Not init yet");
        return *reinterpret_cast<ResponseType *>(resp_raw.get_buf());
    }

    msgq::MsgBuffer &GetBuffer() {
        DLOG_ASSERT(state == true, "Not init yet");
        return resp_raw;
    }

    bool state;
    msgq::MsgQueueRPC *rpc;
    msgq::MsgBuffer resp_raw;
};

template <typename EFW, bool ESTABLISH>
void erpc_call_target(erpc::ReqHandle *req_handle, void *context) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(context);
    uint64_t perf_stat_timer;
    self_ctx->m_stats.start_sample(perf_stat_timer);

    self_ctx->GetFiberPool().EnqueueTask([self_ctx, req_handle, perf_stat_timer]() mutable {
        auto &rpc = self_ctx->GetErpc();
        erpc::ReqHandleWrap req_wrap(req_handle);
        ErpcResponseHandle<typename EFW::ResponseType> resp_handle(rpc, req_wrap);

        auto req_raw = req_wrap.get_req_msgbuf();
        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;

        if constexpr (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
        } else {
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->GetConnection(req->mac_id));
        }

        EFW::func(*self_ctx, *peer_connection, *req, resp_handle);
        rpc.enqueue_response(req_wrap, resp_handle.GetBuffer());

        self_ctx->m_stats.rpc_exec_sample(perf_stat_timer);
    });
}

template <typename EFW, bool ESTABLISH>
void msgq_call_target(msgq::MsgBuffer &req_raw, void *ctx) {
    auto self_ctx = reinterpret_cast<typename EFW::SelfContext *>(ctx);
    uint64_t perf_stat_timer;
    self_ctx->m_stats.start_sample(perf_stat_timer);

    self_ctx->GetFiberPool().EnqueueTask([self_ctx, req_raw, perf_stat_timer]() mutable {
        auto req = reinterpret_cast<typename EFW::RequestType *>(req_raw.get_buf());

        typename EFW::PeerContext *peer_connection = nullptr;
        msgq::MsgQueueRPC *rpc;
        msgq::MsgBuffer resp_raw;

        if constexpr (ESTABLISH) {
            peer_connection = new typename EFW::PeerContext();
            RawResponseHandle<typename EFW::ResponseType> resp_handle;
            EFW::func(*self_ctx, *peer_connection, *req, resp_handle);

            rpc = peer_connection->GetMsgQ();
            MsgQResponseHandle<typename EFW::ResponseType> resp_handle_(rpc);

            resp_handle_.Init(resp_handle.GetSize());
            typename EFW::ResponseType &resp = resp_handle_.Get();
            memcpy(&resp, resp_handle.GetBuffer(), resp_handle.GetSize());
            rpc->enqueue_response(req_raw, resp_handle_.GetBuffer());
        } else {
            peer_connection =
                dynamic_cast<typename EFW::PeerContext *>(self_ctx->GetConnection(req->mac_id));
            rpc = peer_connection->GetMsgQ();

            MsgQResponseHandle<typename EFW::ResponseType> resp_handle(rpc);
            EFW::func(*self_ctx, *peer_connection, *req, resp_handle);
            rpc->enqueue_response(req_raw, resp_handle.GetBuffer());
        }

        // The buffer on the sending side will be released by the receiving side
        rpc->free_msg_buffer(req_raw);

        self_ctx->m_stats.rpc_exec_sample(perf_stat_timer);
    });
}

}  // namespace detail

/**
 * @brief Use the erpc handler type to bind the rpc handler function defined in proto.
 *
 * @warning Each func can only bind once, and requires different input types for different rpc func.
 * This means that each rpc handler needs to define `Request` and `Response` structures.
 *
 * @tparam ESTABLISH Whether RPC request to establish communication. When `ESTABLISH` is `true`,
 * hanlder requests a peer connection from the heap to add to the connection table.
 * @tparam RpcFunc
 * @param func
 * @return auto Types of handler functions for erpc
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

template <typename ResponseType, typename PromiseType>
struct ErpcFuture {
    ErpcFuture() : pro(nullptr), rpc(nullptr), req_raw(nullptr), resp_raw(nullptr) {}

    ErpcFuture(const ErpcFuture &) = delete;
    ErpcFuture &operator=(const ErpcFuture &) = delete;

    ErpcFuture(ErpcFuture &&other) : ErpcFuture() { swap(other); }
    ErpcFuture &operator=(ErpcFuture &&other) {
        swap(other);
        return *this;
    }

    ~ErpcFuture() {
        if (rpc) {
            rpc->free_msg_buffer(req_raw);
            rpc->free_msg_buffer(resp_raw);
        }
        if (pro) {
            pro->~PromiseType();
            ObjectPoolAllocator<PromiseType>().deallocate(pro, 1);
        }
    }

    auto &get() {
        auto fu = pro->get_future();
        fu.get();
        return *reinterpret_cast<ResponseType *>(resp_raw.get_buf());
    }

    void wait() {
        auto fu = pro->get_future();
        fu.wait();
    }

    template <typename _Rep, typename _Period>
    auto wait_for(const std::chrono::duration<_Rep, _Period> &__rel) const {
        auto fu = pro->get_future();
        return fu.wait_for(__rel);
    }

    void swap(ErpcFuture &other) {
        std::swap(pro, other.pro);
        std::swap(rpc, other.rpc);
        std::swap(req_raw, other.req_raw);
        std::swap(resp_raw, other.resp_raw);
    }

    PromiseType *pro;
    erpc::IBRpcWrap *rpc;
    erpc::MsgBufferWrap req_raw;
    erpc::MsgBufferWrap resp_raw;
};

struct ErpcClient {
    ErpcClient(erpc::IBRpcWrap &rpc, std::string ip, uint16_t port) : rpc(rpc) {
        std::string server_uri = erpc::concat_server_uri(ip, port);
        peer_session = rpc.create_session(server_uri, 0);
    }

    ErpcClient(erpc::IBRpcWrap &rpc, int peer_session) : rpc(rpc), peer_session(peer_session) {}

    template <template <typename T> class PromiseTType, typename RpcFuncType,
              typename Fn = std::remove_reference_t<RpcFuncType>>
    auto call(RpcFuncType &&_, typename ::detail::RpcCallerWrapper<Fn>::RequestType &&req) {
        using RpcCallerWrapper = ::detail::RpcCallerWrapper<Fn>;
        using RequestType = typename RpcCallerWrapper::RequestType;

        return call<PromiseTType>(
            std::move(_), sizeof(RequestType),
            [](RequestType *req_buf, RequestType &&req) { *req_buf = std::move(req); },
            std::move(req));
    }

    template <template <typename T> class PromiseTType, typename RpcFuncType, typename CopyFn,
              typename Fn = std::remove_reference_t<RpcFuncType>, typename... Args>
    auto call(RpcFuncType &&, size_t req_size, CopyFn &&copy_fn, Args &&...args) {
        using RpcCallerWrapper = ::detail::RpcCallerWrapper<Fn>;
        using RequestType = typename RpcCallerWrapper::RequestType;
        using ResponseType = typename RpcCallerWrapper::ResponseType;
        using PromiseType = PromiseTType<void>;

        ErpcFuture<ResponseType, PromiseType> fu;
        fu.rpc = &rpc;
        fu.pro = new (ObjectPoolAllocator<PromiseType>().allocate(1)) PromiseType();

        fu.req_raw = rpc.alloc_msg_buffer_or_die(req_size);
        fu.resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(ResponseType) + 64);

        auto req_buf = reinterpret_cast<RequestType *>(fu.req_raw.get_buf());
        copy_fn(req_buf, std::move(args...));

        rpc.enqueue_request(peer_session, RpcCallerWrapper::rpc_type, fu.req_raw, fu.resp_raw,
                            erpc_general_promise_cb<PromiseType>, static_cast<void *>(fu.pro));

        return fu;
    }

    template <typename PromiseType>
    static void erpc_general_promise_cb(void *, void *pr) {
        PromiseType *pro = reinterpret_cast<PromiseType *>(pr);
        pro->set_value();
    }

    erpc::IBRpcWrap &rpc;
    int peer_session;
};

template <typename ResponseType, typename PromiseType>
struct MsgQFuture {
    MsgQFuture() : pro(nullptr), rpc(nullptr) {}

    MsgQFuture(const MsgQFuture &) = delete;
    MsgQFuture &operator=(const MsgQFuture &) = delete;

    MsgQFuture(MsgQFuture &&other) : MsgQFuture() { swap(other); }
    MsgQFuture &operator=(MsgQFuture &&other) {
        swap(other);
        return *this;
    }

    ~MsgQFuture() {
        if (rpc) {
            rpc->free_msg_buffer(resp_raw);
        }
        if (pro) {
            pro->~PromiseType();
            ObjectPoolAllocator<PromiseType>().deallocate(pro, 1);
        }
    }

    auto &get() {
        auto fu = pro->get_future();
        resp_raw = fu.get();
        return *reinterpret_cast<ResponseType *>(resp_raw.get_buf());
    }

    void wait() {
        auto fu = pro->get_future();
        fu.wait();
    }

    template <typename _Rep, typename _Period>
    auto wait_for(const std::chrono::duration<_Rep, _Period> &__rel) const {
        auto fu = pro->get_future();
        return fu.wait_for(__rel);
    }

    void swap(MsgQFuture &other) {
        std::swap(pro, other.pro);
        std::swap(rpc, other.rpc);
        std::swap(req_raw, other.req_raw);
        std::swap(resp_raw, other.resp_raw);
    }

    PromiseType *pro;
    msgq::MsgQueueRPC *rpc;
    msgq::MsgBuffer req_raw;
    msgq::MsgBuffer resp_raw;
};

struct MsgQClient {
    MsgQClient(msgq::MsgQueueRPC rpc) : rpc(rpc) {}

    template <template <typename T> class PromiseTType, typename RpcFuncType,
              typename Fn = std::remove_reference_t<RpcFuncType>>
    auto call(RpcFuncType &&_, typename ::detail::RpcCallerWrapper<Fn>::RequestType &&req) {
        using RpcCallerWrapper = ::detail::RpcCallerWrapper<Fn>;
        using RequestType = typename RpcCallerWrapper::RequestType;

        return call<PromiseTType>(
            std::move(_), sizeof(RequestType),
            [](RequestType *req_buf, RequestType &&req) { *req_buf = std::move(req); },
            std::move(req));
    }

    template <template <typename T> class PromiseTType, typename RpcFuncType, typename CopyFn,
              typename Fn = std::remove_reference_t<RpcFuncType>, typename... Args>
    auto call(RpcFuncType &&, size_t req_size, CopyFn &&copy_fn, Args &&...args) {
        using RpcCallerWrapper = ::detail::RpcCallerWrapper<Fn>;
        using RequestType = typename RpcCallerWrapper::RequestType;
        using ResponseType = typename RpcCallerWrapper::ResponseType;
        using PromiseType = PromiseTType<msgq::MsgBuffer>;

        MsgQFuture<ResponseType, PromiseType> fu;
        fu.rpc = &rpc;
        fu.pro = new (ObjectPoolAllocator<PromiseType>().allocate(1)) PromiseType();

        fu.req_raw = rpc.alloc_msg_buffer(req_size);

        auto req_buf = reinterpret_cast<RequestType *>(fu.req_raw.get_buf());
        copy_fn(req_buf, std::move(args)...);

        rpc.enqueue_request(RpcCallerWrapper::rpc_type, fu.req_raw,
                            msgq_general_promise_cb<PromiseType>, static_cast<void *>(fu.pro));

        return fu;
    }

    template <typename PromiseType>
    static void msgq_general_promise_cb(msgq::MsgBuffer &resp, void *pr) {
        PromiseType *pro = reinterpret_cast<PromiseType *>(pr);
        pro->set_value(resp);
    }

    msgq::MsgQueueRPC rpc;
};