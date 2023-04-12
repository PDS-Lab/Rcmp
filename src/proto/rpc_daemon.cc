#include "proto/rpc_daemon.hpp"

#include <chrono>
#include <cstdint>
#include <future>

#include "common.hpp"
#include "config.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_master.hpp"
#include "stats.hpp"
#include "udp_client.hpp"
#include "utils.hpp"

using namespace std::chrono_literals;
namespace rpc_daemon {

JoinRackReply joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                       JoinRackRequest& req) {
    DLOG_ASSERT(req.rack_id == daemon_context.m_options.rack_id,
                "Can't join different rack %d ---> %d", req.rack_id,
                daemon_context.m_options.rack_id);

    // 1. 通知master获取mac id
    auto rpc = daemon_context.get_erpc();

    using JoinClientRPC = RPC_TYPE_STRUCT(rpc_master::joinClient);
    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinClientRPC::RequestType));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinClientRPC::ResponseType));

    auto join_req = reinterpret_cast<JoinClientRPC::RequestType*>(req_raw.get_buf());
    join_req->rack_id = daemon_context.m_options.rack_id;

    std::promise<void> pro;
    std::future<void> fu = pro.get_future();
    rpc.enqueue_request(daemon_context.m_erpc_ctx.master_session, JoinClientRPC::rpc_type, req_raw,
                        resp_raw, erpc_general_promise_flag_cb, static_cast<void*>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto resp = reinterpret_cast<JoinClientRPC::ResponseType*>(resp_raw.get_buf());

    client_connection.client_id = resp->mac_id;

    daemon_context.m_client_connect_table.push_back(&client_connection);

    rpc.free_msg_buffer(req_raw);
    rpc.free_msg_buffer(resp_raw);

    // 2. 分配msg queue
    msgq::MsgQueue* q = daemon_context.msgq_manager.allocQueue();
    client_connection.msgq_rpc =
        new msgq::MsgQueueRPC(daemon_context.msgq_manager.nexus.get(), &daemon_context);
    client_connection.msgq_rpc->m_recv_queue = daemon_context.msgq_manager.nexus->m_public_msgq;
    client_connection.msgq_rpc->m_send_queue = q;

    // 3. 通过UDP通知client创建msgq
    msgq::MsgUDPConnPacket pkt;
    pkt.recv_q_off = reinterpret_cast<uintptr_t>(q) -
                     reinterpret_cast<uintptr_t>(daemon_context.m_cxl_memory_addr);
    UDPClient<msgq::MsgUDPConnPacket> udp_cli;
    udp_cli.send(req.client_ipv4.get_string(), req.client_port, pkt);
    daemon_context.m_connect_table.insert(client_connection.client_id, &client_connection);

    DLOG("Connect with client [rack:%d --- id:%d]", daemon_context.m_options.rack_id,
         client_connection.client_id);

    JoinRackReply reply;
    reply.client_mac_id = client_connection.client_id;
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    return reply;
}

GetPageRefOrProxyReply getPageRefOrProxy(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, GetPageRefOrProxyRequest& req) {
    PageMetadata* page_metadata;
    page_id_t page_id = GetPageID(req.gaddr);
    bool ret = daemon_context.m_page_table.find(page_id, &page_metadata);

    if (!ret) {
        FreqStats* stats;
        ret = daemon_context.m_hot_stats.find(page_id, &stats);

        if (!ret || stats->freq() < page_hot_dio_swap_watermark) {
            if (!ret) {
                stats = new FreqStats(page_hot_dio_swap_watermark * 2);
                daemon_context.m_hot_stats.insert(page_id, stats);
            }

            // TODO: DIO

            /**
             * 1. 向mn发送LatchPage(page_id)，获取mn上page的daemon，并锁定该page
             * 2. 与该daemon建立RDMA RC连接
             *      2.1 若是write，与此同时向cn发送
             * 3. 发送daemon请求rdmaIODirect(page_id, my_buf_addr, my_size, my_rkey)
             * 4. 读时，daemon单边写；写时，daemon单边读
             * 5. RPC返回后，给mn发送UnLatchPage(page_id)解锁
             */

            stats->add(getTimestamp());
        } else {
            // TODO: page swap

            /**
             * 1. 向mn发送LatchPage(page_id)，获取mn上page的daemon，并锁定该page
             *      1.1 如果本地page不够，与此同时向所有cn发起getPagePastAccessFreq()获取最久远的swapout page
             * 2. 与自己建立RDMA RC连接
             * 3. mn向daemon发送tryMigratePage(page_id, swapout meta, swapin meta)
             *      3.1 如果本地page少，则无swapout meta
             * 4. daemonRDMA单边读将page_addr读过来、daemonRDMA单边写将自己page写过去swap_addr
             *      4.1 如果无swapout meta，则不写
             *      4.2 如果拒绝，则自己需要DIO访问
             * 5. daemon删除page meta，返回RPC
             * 6. 向mn发送unLatchPageAndBalance，更改page dir，返回RPC
             * 7. 更改page meta
             * 8. 返回ref
             */
        }

        // TODO: 本地缺页
        DLOG_FATAL("Not Support");
    }

    page_metadata->ref_client.insert(&client_connection);

    GetPageRefOrProxyReply reply;
    reply.offset = page_metadata->cxl_memory_offset;
    return reply;
}

AllocPageMemoryReply allocPageMemory(DaemonContext& daemon_context,
                                     DaemonToMasterConnection& master_connection,
                                     AllocPageMemoryRequest& req) {
    DLOG_ASSERT(daemon_context.m_current_used_page_num < daemon_context.m_max_data_page_num,
                "Can't allocate more page memory");

    offset_t cxl_memory_offset = daemon_context.m_cxl_page_allocator->allocate(1);
    DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

    PageMetadata* page_metadata = new PageMetadata(req.slab_size);
    page_metadata->cxl_memory_offset = cxl_memory_offset;

    daemon_context.m_page_table.insert(req.page_id, page_metadata);

    DLOG("new page %ld ---> %#x", req.page_id, cxl_memory_offset);

    AllocPageMemoryReply reply;
    reply.ret = true;
    return reply;
}

AllocReply alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                 AllocRequest& req) {
    // alloc size aligned by cache line
    size_t aligned_size = align_ceil(req.size, min_slab_size);

    size_t slab_cls = aligned_size / min_slab_size - 1;
    std::list<page_id_t>& slab_list = daemon_context.m_can_alloc_slab_class_lists[slab_cls];

    // TODO: 不再做slab分配

    if (slab_list.empty()) {
        DLOG("alloc a new page");

        size_t slab_size = (slab_cls + 1) * min_slab_size;

        // 向Master调用allocPage(slab_size)
        auto rpc = daemon_context.get_erpc();

        using PageAllocRPC = RPC_TYPE_STRUCT(rpc_master::allocPage);

        auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::RequestType));
        auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::ResponseType));

        auto page_alloc_req = reinterpret_cast<PageAllocRPC::RequestType*>(req_raw.get_buf());
        page_alloc_req->mac_id = daemon_context.m_daemon_id;
        page_alloc_req->slab_size = page_alloc_req->slab_size;

        std::promise<void> pro;
        std::future<void> fu = pro.get_future();
        rpc.enqueue_request(daemon_context.m_erpc_ctx.master_session, PageAllocRPC::rpc_type,
                            req_raw, resp_raw, erpc_general_promise_flag_cb,
                            static_cast<void*>(&pro));

        // 等待期间可能出现由于本地page不足而发生page swap

        while (fu.wait_for(1ns) == std::future_status::timeout) {
            rpc.run_event_loop_once();
        }

        auto resp = reinterpret_cast<PageAllocRPC::ResponseType*>(resp_raw.get_buf());

        AllocPageMemoryRequest inner_req;
        inner_req.page_id = resp->page_id;
        inner_req.slab_size = slab_size;
        inner_req.mac_id = daemon_context.m_daemon_id;
        allocPageMemory(daemon_context, daemon_context.m_master_connection, inner_req);

        slab_list.push_back(resp->page_id);

        rpc.free_msg_buffer(req_raw);
        rpc.free_msg_buffer(resp_raw);
    }

    page_id_t page_id = slab_list.front();
    PageMetadata* page_metadata;
    bool ret = daemon_context.m_page_table.find(page_id, &page_metadata);
    DLOG_ASSERT(ret, "Can't find page id %lu", page_id);

    DLOG_ASSERT(!page_metadata->slab_allocator.full(), "Can't allocate the page %lu continuely",
                page_id);

    offset_t page_offset = page_metadata->slab_allocator.allocate(1);
    DLOG_ASSERT(page_offset != -1, "Can't alloc page slab, because page %lu is full", page_id);

    if (page_metadata->slab_allocator.full()) {
        slab_list.erase(slab_list.begin());
    }

    AllocReply reply;
    reply.gaddr = GetGAddr(page_id, page_offset);
    return reply;
}

FreeReply free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               FreeRequest& req) {
    DLOG_FATAL("Not Support");
}

/************************ for test ***************************/

__TestDataSend1Reply __testdataSend1(DaemonContext& daemon_context,
                                     DaemonToClientConnection& client_connection,
                                     __TestDataSend1Request& req) {
    __TestDataSend1Reply reply;
    reply.size = req.size;
    assert(req.size == 64);

    memcpy(reply.data, req.data, reply.size * sizeof(int));
    return reply;
}
__TestDataSend2Reply __testdataSend2(DaemonContext& daemon_context,
                                     DaemonToClientConnection& client_connection,
                                     __TestDataSend2Request& req) {
    __TestDataSend2Reply reply;
    reply.size = req.size;
    assert(req.size == 72);

    memcpy(reply.data, req.data, reply.size * sizeof(int));
    return reply;
}

}  // namespace rpc_daemon
