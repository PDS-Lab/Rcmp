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
    PoolContextImpl *impl = new PoolContextImpl();
    DLOG_ASSERT(impl != nullptr, "Can't alloc ContextImpl");
    impl->m_options = options;

    // 1. 打开cxl设备
    impl->m_cxl_memory_addr = cxl_open_simulate(
        impl->m_options.cxl_devdax_path, impl->m_options.cxl_memory_size, &impl->m_cxl_devdax_fd);

    cxl_memory_open(impl->format, impl->m_cxl_memory_addr);

    // 2. 与daemon建立连接
    impl->udp_conn_recver.reset(
        new UDPServer<msgq::MsgUDPConnPacket>(impl->m_options.client_port, 1000));

    impl->msgq_nexus.reset(new msgq::MsgQueueNexus(impl->format.msgq_zone_start_addr));
    impl->msgq_rpc.reset(new msgq::MsgQueueRPC(impl->msgq_nexus.get(), impl));
    impl->msgq_rpc->m_send_queue = impl->msgq_nexus->m_public_msgq;

    // 3. 发送join rack rpc
    using JoinRackRPC = RPC_TYPE_STRUCT(rpc_daemon::joinRack);
    msgq::MsgBuffer req_raw = impl->msgq_rpc->alloc_msg_buffer(sizeof(JoinRackRPC::RequestType));
    auto req = reinterpret_cast<JoinRackRPC::RequestType *>(req_raw.get_buf());
    strcpy(req->client_ipv4, impl->m_options.client_ip.c_str());
    req->client_port = impl->m_options.client_port;
    req->rack_id = impl->m_options.rack_id;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    impl->msgq_rpc->enqueue_request(JoinRackRPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                    static_cast<void *>(&pro));

    // 4. daemon会发送udp消息，告诉recv queue的偏移地址
    msgq::MsgUDPConnPacket msg;
    impl->udp_conn_recver->recv_blocking(msg);

    impl->msgq_rpc->m_recv_queue = reinterpret_cast<msgq::MsgQueue *>(
        (reinterpret_cast<uintptr_t>(impl->m_cxl_memory_addr) + msg.recv_q_off));

    // 5. 正式接收rpc消息
    while (fu.wait_for(1ns) == std::future_status::timeout) {
        impl->msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<JoinRackRPC::ResponseType *>(resp_raw.get_buf());

    impl->m_client_id = resp->client_mac_id;
    impl->m_local_rack_daemon_connection.daemon_id = resp->daemon_mac_id;

    impl->msgq_rpc->free_msg_buffer(resp_raw);

    DLOG("Connect with rack %d daemon %d success, my id is %d", impl->m_options.rack_id,
         impl->m_local_rack_daemon_connection.daemon_id, impl->m_client_id);
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
    // TODO: 向daemon发送alloc(size)请求
    DLOG_FATAL("Not Support");
    return 0;
}

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t offset;
    bool ret = __impl->m_page_table_cache.find(page_id, &offset);
retry:
    if (ret) {
        memcpy(buf,
               reinterpret_cast<const void *>(
                   reinterpret_cast<uintptr_t>(__impl->m_cxl_memory_addr) + offset),
               size);
        return Status::OK;
    } else {
        // TODO: 向daemon发送getPageRef(page_id)请求
        DLOG_FATAL("Not Support");
    }
}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t offset;
    bool ret = __impl->m_page_table_cache.find(page_id, &offset);
retry:
    if (ret) {
        memcpy(reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(__impl->m_cxl_memory_addr) +
                                        offset),
               buf, size);
        return Status::OK;
    } else {
        // TODO: 向daemon发送getPageRef(page_id)请求
        DLOG_FATAL("Not Support");
    }
}

Status PoolContext::Free(GAddr gaddr, size_t size) { DLOG_FATAL("Not Support"); }
}  // namespace rchms