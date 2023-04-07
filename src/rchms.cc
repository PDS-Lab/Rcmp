#include "rchms.hpp"

#include <cstdint>
#include <future>

#include "common.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "msg_queue.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_daemon.hpp"
#include "status.hpp"

using namespace std::chrono_literals;

namespace rchms {

PoolContext::PoolContext(ClientOptions options) {
    __impl = new PoolContextImpl();
    DLOG_ASSERT(__impl != nullptr, "Can't alloc ContextImpl");
    __impl->m_options = options;

    // 1. 打开cxl设备
    __impl->m_cxl_memory_addr =
        cxl_open_simulate(__impl->m_options.cxl_devdax_path, __impl->m_options.cxl_memory_size,
                          &__impl->m_cxl_devdax_fd);

    cxl_memory_open(__impl->format, __impl->m_cxl_memory_addr);

    // 2. 与daemon建立连接
    __impl->udp_conn_recver.reset(
        new UDPServer<msgq::MsgUDPConnPacket>(__impl->m_options.client_port, 1000));

    __impl->msgq_nexus.reset(new msgq::MsgQueueNexus(__impl->format.msgq_zone_start_addr));
    __impl->msgq_rpc.reset(new msgq::MsgQueueRPC(__impl->msgq_nexus.get(), __impl));
    __impl->msgq_rpc->m_send_queue = __impl->msgq_nexus->m_public_msgq;

    // 3. 发送join rack rpc
    using JoinRackRPC = RPC_TYPE_STRUCT(rpc_daemon::joinRack);
    msgq::MsgBuffer req_raw = __impl->msgq_rpc->alloc_msg_buffer(sizeof(JoinRackRPC::RequestType));
    auto req = reinterpret_cast<JoinRackRPC::RequestType *>(req_raw.get_buf());
    strcpy(req->client_ipv4, __impl->m_options.client_ip.c_str());
    req->client_port = __impl->m_options.client_port;
    req->rack_id = __impl->m_options.rack_id;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->msgq_rpc->enqueue_request(JoinRackRPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                      static_cast<void *>(&pro));

    // 4. daemon会发送udp消息，告诉recv queue的偏移地址
    msgq::MsgUDPConnPacket msg;
    __impl->udp_conn_recver->recv_blocking(msg);

    __impl->msgq_rpc->m_recv_queue = reinterpret_cast<msgq::MsgQueue *>(
        (reinterpret_cast<uintptr_t>(__impl->m_cxl_memory_addr) + msg.recv_q_off));

    // 5. 正式接收rpc消息
    while (fu.wait_for(1ns) == std::future_status::timeout) {
        __impl->msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<JoinRackRPC::ResponseType *>(resp_raw.get_buf());

    __impl->m_client_id = resp->client_mac_id;
    __impl->m_local_rack_daemon_connection.daemon_id = resp->daemon_mac_id;

    __impl->msgq_rpc->free_msg_buffer(resp_raw);

    DLOG("Connect with rack %d daemon %d success, my id is %d", __impl->m_options.rack_id,
         __impl->m_local_rack_daemon_connection.daemon_id, __impl->m_client_id);
}

PoolContext::~PoolContext() {}

PoolContext *Open(ClientOptions options) {
    PoolContext *pool_ctx = new PoolContext(options);
    return pool_ctx;
}

void Close(PoolContext *pool_ctx) {
    // TODO: 关闭连接

    delete pool_ctx;
}

GAddr PoolContext::Alloc(size_t size) {
    using AllocRPC = RPC_TYPE_STRUCT(rpc_daemon::alloc);
    msgq::MsgBuffer req_raw = __impl->msgq_rpc->alloc_msg_buffer(sizeof(AllocRPC::RequestType));
    auto req = reinterpret_cast<AllocRPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->size = size;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->msgq_rpc->enqueue_request(AllocRPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                      static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        __impl->msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<AllocRPC::ResponseType *>(resp_raw.get_buf());

    GAddr gaddr = resp->gaddr;

    __impl->msgq_rpc->free_msg_buffer(resp_raw);

    return gaddr;
}

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    offset_t offset;
    bool ret = __impl->m_page_table_cache.find(page_id, &offset);
    if (!ret) {
        // TODO: locally get ref?

        using GetPageRefRPC = RPC_TYPE_STRUCT(rpc_daemon::getPageRef);
        msgq::MsgBuffer req_raw =
            __impl->msgq_rpc->alloc_msg_buffer(sizeof(GetPageRefRPC::RequestType));
        auto req = reinterpret_cast<GetPageRefRPC::RequestType *>(req_raw.get_buf());
        req->mac_id = __impl->m_client_id;
        req->page_id = page_id;

        std::promise<msgq::MsgBuffer> pro;
        std::future<msgq::MsgBuffer> fu = pro.get_future();
        __impl->msgq_rpc->enqueue_request(GetPageRefRPC::rpc_type, req_raw,
                                          msgq_general_promise_flag_cb, static_cast<void *>(&pro));

        while (fu.wait_for(1ns) == std::future_status::timeout) {
            __impl->msgq_rpc->run_event_loop_once();
        }

        msgq::MsgBuffer resp_raw = fu.get();
        auto resp = reinterpret_cast<GetPageRefRPC::ResponseType *>(resp_raw.get_buf());

        offset = resp->offset;
        __impl->msgq_rpc->free_msg_buffer(resp_raw);

        __impl->m_page_table_cache.insert(page_id, offset);

        DLOG("get ref: %ld --- %lx", page_id, offset);
    }

    memcpy(buf,
           reinterpret_cast<const void *>(
               reinterpret_cast<uintptr_t>(__impl->format.page_data_start_addr) + offset + in_page_offset),
           size);
    return Status::OK;
}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    offset_t offset;
    bool ret = __impl->m_page_table_cache.find(page_id, &offset);
    if (!ret) {
        // TODO: locally get ref?

        using GetPageRefRPC = RPC_TYPE_STRUCT(rpc_daemon::getPageRef);
        msgq::MsgBuffer req_raw =
            __impl->msgq_rpc->alloc_msg_buffer(sizeof(GetPageRefRPC::RequestType));
        auto req = reinterpret_cast<GetPageRefRPC::RequestType *>(req_raw.get_buf());
        req->mac_id = __impl->m_client_id;
        req->page_id = page_id;

        std::promise<msgq::MsgBuffer> pro;
        std::future<msgq::MsgBuffer> fu = pro.get_future();
        __impl->msgq_rpc->enqueue_request(GetPageRefRPC::rpc_type, req_raw,
                                          msgq_general_promise_flag_cb, static_cast<void *>(&pro));

        while (fu.wait_for(1ns) == std::future_status::timeout) {
            __impl->msgq_rpc->run_event_loop_once();
        }

        msgq::MsgBuffer resp_raw = fu.get();
        auto resp = reinterpret_cast<GetPageRefRPC::ResponseType *>(resp_raw.get_buf());

        offset = resp->offset;
        __impl->msgq_rpc->free_msg_buffer(resp_raw);

        __impl->m_page_table_cache.insert(page_id, offset);

        DLOG("get ref: %ld --- %lx", page_id, offset);
    }

    memcpy(reinterpret_cast<void *>(
               reinterpret_cast<uintptr_t>(__impl->format.page_data_start_addr) + offset + in_page_offset),
           buf, size);
    return Status::OK;
}

Status PoolContext::Free(GAddr gaddr, size_t size) { DLOG_FATAL("Not Support"); }

/*********************** for test **************************/

Status PoolContext::__TestDataSend1(int *array, size_t size) {
    using DataSendRPC = RPC_TYPE_STRUCT(rpc_daemon::__testdataSend1);
    msgq::MsgBuffer req_raw = __impl->msgq_rpc->alloc_msg_buffer(sizeof(DataSendRPC::RequestType));
    auto req = reinterpret_cast<DataSendRPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->size = size;
    for (size_t i = 0; i < req->size; i++) {
        req->data[i] = i + req->mac_id;
    }
    memcpy(req->data, array, size * sizeof(int));

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->msgq_rpc->enqueue_request(DataSendRPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                      static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        __impl->msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<DataSendRPC::ResponseType *>(resp_raw.get_buf());
    // check
    assert(resp->size == size);
    for (size_t i = 0; i < resp->size; i++) {
        assert(resp->data[i] == array[i]);
    }
    __impl->msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

Status PoolContext::__TestDataSend2(int *array, size_t size) {
    using DataSend1RPC = RPC_TYPE_STRUCT(rpc_daemon::__testdataSend2);
    msgq::MsgBuffer req_raw = __impl->msgq_rpc->alloc_msg_buffer(sizeof(DataSend1RPC::RequestType));
    auto req = reinterpret_cast<DataSend1RPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->size = size;
    for (size_t i = 0; i < req->size; i++) {
        req->data[i] = i + req->mac_id;
    }
    memcpy(req->data, array, size * sizeof(int));

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->msgq_rpc->enqueue_request(DataSend1RPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                      static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        __impl->msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<DataSend1RPC::ResponseType *>(resp_raw.get_buf());
    // check
    for (size_t i = 0; i < resp->size; i++) {
        assert(resp->data[i] == array[i]);
    }
    __impl->msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

}  // namespace rchms