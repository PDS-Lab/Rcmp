#include "proto/rpc_daemon.hpp"

#include <chrono>
#include <future>

#include "common.hpp"
#include "config.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_master.hpp"
#include "utils.hpp"

using namespace std::chrono_literals;
namespace rpc_daemon {

JoinRackReply joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                       JoinRackRequest& req) {
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

    DLOG("Connect with client [rack:%d --- id:%d]", daemon_context.m_options.rack_id,
         client_connection.client_id);

    JoinRackReply reply;
    reply.client_mac_id = client_connection.client_id;
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    return reply;
}

GetPageRefReply getPageRef(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, GetPageRefRequest& req) {
    PageMetadata* page_metadata;
    bool ret = daemon_context.m_page_table.find(req.page_id, &page_metadata);

    if (!ret) {
        // TODO: 本地缺页
        DLOG_FATAL("Not Support");
    }

    page_metadata->ref_client.insert(&client_connection);

    GetPageRefReply reply;
    reply.local_get = true;
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

    AllocPageMemoryReply reply;
    reply.ret = true;
    return reply;
}

AllocReply alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                 AllocRequest& req) {
    // alloc size aligned by cache line
    size_t n = div_floor(req.n, min_slab_size);

    size_t slab_cls = n / min_slab_size - 1;
    std::list<page_id_t>& slab_list = daemon_context.m_can_alloc_slab_class_lists[slab_cls];

    if (slab_list.empty()) {
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

}  // namespace rpc_daemon
