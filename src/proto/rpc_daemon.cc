#include "proto/rpc_daemon.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>

#include "common.hpp"
#include "config.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_client.hpp"
#include "proto/rpc_master.hpp"
#include "rdma_rc.hpp"
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
    auto& rpc = daemon_context.get_erpc();

    using JoinClientRPC = RPC_TYPE_STRUCT(rpc_master::joinClient);
    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinClientRPC::RequestType));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinClientRPC::ResponseType));

    auto join_req = reinterpret_cast<JoinClientRPC::RequestType*>(req_raw.get_buf());
    join_req->rack_id = daemon_context.m_options.rack_id;

    std::promise<void> pro;
    std::future<void> fu = pro.get_future();
    rpc.enqueue_request(daemon_context.m_master_connection.peer_session, JoinClientRPC::rpc_type,
                        req_raw, resp_raw, erpc_general_promise_flag_cb, static_cast<void*>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto resp = reinterpret_cast<JoinClientRPC::ResponseType*>(resp_raw.get_buf());

    client_connection.client_id = resp->mac_id;

    daemon_context.m_client_connect_table.push_back(&client_connection);
    daemon_context.m_connect_table.insert(client_connection.client_id, &client_connection);

    rpc.free_msg_buffer(req_raw);
    rpc.free_msg_buffer(resp_raw);

    // 2. 分配msg queue
    msgq::MsgQueue* q = daemon_context.m_msgq_manager.allocQueue();
    client_connection.msgq_rpc =
        new msgq::MsgQueueRPC(daemon_context.m_msgq_manager.nexus.get(), &daemon_context);
    client_connection.msgq_rpc->m_recv_queue = daemon_context.m_msgq_manager.nexus->m_public_msgq;
    client_connection.msgq_rpc->m_send_queue = q;

    // 3. 通过UDP通知client创建msgq
    msgq::MsgUDPConnPacket pkt;
    pkt.recv_q_off = reinterpret_cast<uintptr_t>(q) -
                     reinterpret_cast<uintptr_t>(daemon_context.m_cxl_memory_addr);
    UDPClient<msgq::MsgUDPConnPacket> udp_cli;
    udp_cli.send(req.client_ipv4.get_string(), req.client_port, pkt);

    DLOG("Connect with client [rack:%d --- id:%d]", daemon_context.m_options.rack_id,
         client_connection.client_id);

    JoinRackReply reply;
    reply.client_mac_id = client_connection.client_id;
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    return reply;
}

CrossRackConnectReply crossRackConnect(DaemonContext& daemon_context,
                                       DaemonToDaemonConnection& daemon_connection,
                                       CrossRackConnectRequest& req) {
    DLOG_ASSERT(req.conn_mac_id == daemon_context.m_daemon_id, "Can't connect this daemon");

    daemon_context.m_connect_table.insert(req.mac_id, &daemon_connection);
    daemon_context.m_other_daemon_connect_table.push_back(&daemon_connection);

    daemon_connection.daemon_id = req.mac_id;
    daemon_connection.rack_id = req.rack_id;
    daemon_connection.ip = req.ip.get_string();
    daemon_connection.port = req.port;
    daemon_connection.peer_session = daemon_context.get_erpc().create_session(
        daemon_connection.ip + ":" + std::to_string(daemon_connection.port), 0);

    DLOG("Connect with daemon [rack:%d --- id:%d]", daemon_connection.rack_id,
         daemon_connection.daemon_id);

    auto local_addr = daemon_context.m_listen_conn.get_local_addr();

    CrossRackConnectReply reply;
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    reply.rdma_ipv4 = local_addr.first;
    reply.rdma_port = local_addr.second;
    return reply;
}

GetPageCXLRefOrProxyReply getPageCXLRefOrProxy(DaemonContext& daemon_context,
                                               DaemonToClientConnection& client_connection,
                                               GetPageCXLRefOrProxyRequest& req) {
    PageMetadata* page_metadata;
    page_id_t page_id = GetPageID(req.gaddr);
    offset_t page_offset = GetPageOffset(req.gaddr);
    GetPageCXLRefOrProxyReply* reply_ptr;
retry:
    bool ret = daemon_context.m_page_table.find(page_id, &page_metadata);

    if (ret) {
        page_metadata->ref_client.insert(&client_connection);

        reply_ptr = req.alloc_flex_resp(0);

        reply_ptr->refs = true;
        reply_ptr->offset = page_metadata->cxl_memory_offset;
        return {};
    }

    RemotePageMetaCache* rem_page_md_cache;
    ret = daemon_context.m_hot_stats.find(page_id, &rem_page_md_cache);

    if (!ret) {
        rem_page_md_cache = new RemotePageMetaCache(8);
        daemon_context.m_hot_stats.insert(page_id, rem_page_md_cache);
    }

    if (rem_page_md_cache->stats.freq() < page_hot_dio_swap_watermark) {
        // 启动DirectIO流程

        auto& rpc = daemon_context.get_erpc();

        uintptr_t my_data_buf;
        uint32_t my_rkey;
        uint32_t my_size;

        // 1. 获取mn上page的daemon，并锁定该page
        using LatchRemotePageRPC = RPC_TYPE_STRUCT(rpc_master::latchRemotePage);
        auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::RequestType));
        auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::ResponseType));

        auto latch_req = reinterpret_cast<LatchRemotePageRPC::RequestType*>(req_raw.get_buf());
        latch_req->mac_id = daemon_context.m_daemon_id;
        latch_req->page_id = page_id;

        SpinPromise<void> pro;
        SpinFuture<void> fu = pro.get_future();
        rpc.enqueue_request(daemon_context.m_master_connection.peer_session,
                            LatchRemotePageRPC::rpc_type, req_raw, resp_raw,
                            erpc_general_bool_flag_cb, static_cast<void*>(&pro));

        switch (req.type) {
            case GetPageCXLRefOrProxyRequest::WRITE: {
                // 1.1 如果是写操作，则并行获取cn的write buf
                using GetCurrentWriteDataRPC = RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData);
                auto wd_req_raw = client_connection.msgq_rpc->alloc_msg_buffer(
                    sizeof(LatchRemotePageRPC::RequestType));
                auto wd_req =
                    reinterpret_cast<GetCurrentWriteDataRPC::RequestType*>(wd_req_raw.get_buf());
                wd_req->mac_id = daemon_context.m_daemon_id;
                wd_req->dio_write_buf = req.cn_write_buf;
                wd_req->dio_write_size = req.cn_write_size;

                SpinPromise<msgq::MsgBuffer> pro;
                SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
                client_connection.msgq_rpc->enqueue_request(GetCurrentWriteDataRPC::rpc_type,
                                                            wd_req_raw, msgq_general_bool_flag_cb,
                                                            static_cast<void*>(&pro));

                while (fu.wait_for(0s) == std::future_status::timeout) {
                    client_connection.msgq_rpc->run_event_loop_once();
                }

                msgq::MsgBuffer wd_resp_raw = fu.get();
                auto wd_resp =
                    reinterpret_cast<GetCurrentWriteDataRPC::ResponseType*>(wd_resp_raw.get_buf());

                ibv_mr* mr = daemon_context.get_mr(wd_resp->data);

                my_data_buf = reinterpret_cast<uintptr_t>(wd_resp->data);
                my_rkey = mr->rkey;
                my_size = req.cn_write_size;

                client_connection.msgq_rpc->free_msg_buffer(wd_resp_raw);

                // 必须在msgq enqueue之后alloc resp，防止发送阻塞
                reply_ptr = req.alloc_flex_resp(0);
                break;
            }
            case GetPageCXLRefOrProxyRequest::READ: {
                // 1.2 如果是读操作，则动态申请读取resp buf
                reply_ptr = req.alloc_flex_resp(req.cn_read_size);

                ibv_mr* mr = daemon_context.get_mr(reply_ptr->read_data);
                my_data_buf = reinterpret_cast<uintptr_t>(reply_ptr->read_data);
                my_rkey = mr->rkey;
                my_size = req.cn_read_size;
                break;
            }
        }

        // 1.3 一起等待latch完成
        while (fu.wait_for(0s) == std::future_status::timeout) {
            rpc.run_event_loop_once();
        }

        auto resp = reinterpret_cast<LatchRemotePageRPC::ResponseType*>(resp_raw.get_buf());

        // 2. 获取对端连接
        DaemonConnection* dest_daemon_conn_tmp;
        ret = daemon_context.m_connect_table.find(resp->dest_daemon_id, &dest_daemon_conn_tmp);

        if (!ret) {
            // 与该daemon建立erpc与RDMA RC
            DaemonToDaemonConnection* dd_conn = new DaemonToDaemonConnection();
            dest_daemon_conn_tmp = dd_conn;
            std::string server_uri = resp->dest_daemon_ipv4.get_string() + ":" +
                                     std::to_string(resp->dest_daemon_erpc_port);
            dd_conn->peer_session = rpc.create_session(server_uri, 0);
            dd_conn->daemon_id = resp->dest_daemon_id;
            dd_conn->rack_id = resp->dest_rack_id;
            dd_conn->ip = resp->dest_daemon_ipv4.get_string();
            dd_conn->port = resp->dest_daemon_erpc_port;

            daemon_context.m_connect_table.insert(resp->dest_daemon_id, dest_daemon_conn_tmp);

            using CrossRackConnectRPC = RPC_TYPE_STRUCT(rpc_daemon::crossRackConnect);

            auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(CrossRackConnectRPC::RequestType));
            auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(CrossRackConnectRPC::ResponseType));

            auto req = reinterpret_cast<CrossRackConnectRPC::RequestType*>(req_raw.get_buf());
            req->mac_id = daemon_context.m_daemon_id;
            req->ip = daemon_context.m_options.daemon_ip;
            req->port = daemon_context.m_options.daemon_port;
            req->rack_id = daemon_context.m_options.rack_id;
            req->conn_mac_id = resp->dest_daemon_id;

            std::promise<void> pro;
            std::future<void> fu = pro.get_future();
            rpc.enqueue_request(dd_conn->peer_session, CrossRackConnectRPC::rpc_type, req_raw,
                                resp_raw, erpc_general_promise_flag_cb, static_cast<void*>(&pro));

            while (fu.wait_for(1ns) == std::future_status::timeout) {
                rpc.run_event_loop_once();
            }

            auto resp = reinterpret_cast<CrossRackConnectRPC::ResponseType*>(resp_raw.get_buf());

            std::string peer_ip(resp->rdma_ipv4.get_string());
            uint16_t peer_port(resp->rdma_port);

            rpc.free_msg_buffer(req_raw);
            rpc.free_msg_buffer(resp_raw);

            RDMARCConnectParam param;
            param.mac_id = daemon_context.m_daemon_id;
            param.role = CXL_DAEMON;

            dd_conn->rdma_conn = new rdma_rc::RDMAConnection();
            dd_conn->rdma_conn->connect(peer_ip, peer_port, &param, sizeof(param));

            DLOG("Connection with daemon %d OK", dd_conn->daemon_id);
        }

        uint64_t page_cur_version = resp->version;

        rpc.free_msg_buffer(req_raw);
        rpc.free_msg_buffer(resp_raw);

        DaemonToDaemonConnection* dest_daemon_conn =
            dynamic_cast<DaemonToDaemonConnection*>(dest_daemon_conn_tmp);

        // 3. 获取远端内存rdma ref
        // 通过比较page version减少获取rdma ref次数
        if (rem_page_md_cache->page_version != page_cur_version) {
            using GetPageRDMARefRPC = RPC_TYPE_STRUCT(rpc_daemon::getPageRDMARef);

            auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(GetPageRDMARefRPC::RequestType));
            auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(GetPageRDMARefRPC::ResponseType));

            auto ref_req = reinterpret_cast<GetPageRDMARefRPC::RequestType*>(req_raw.get_buf());
            ref_req->mac_id = daemon_context.m_daemon_id;
            ref_req->page_id = page_id;

            std::promise<void> pro;
            std::future<void> fu = pro.get_future();
            rpc.enqueue_request(dest_daemon_conn->peer_session, GetPageRDMARefRPC::rpc_type,
                                req_raw, resp_raw, erpc_general_promise_flag_cb,
                                static_cast<void*>(&pro));

            while (fu.wait_for(1ns) == std::future_status::timeout) {
                rpc.run_event_loop_once();
            }

            auto resp = reinterpret_cast<GetPageRDMARefRPC::ResponseType*>(resp_raw.get_buf());
            rem_page_md_cache->page_version = page_cur_version;
            rem_page_md_cache->remote_page_addr = resp->addr + page_offset;
            rem_page_md_cache->remote_page_rkey = resp->rkey;

            rpc.free_msg_buffer(req_raw);
            rpc.free_msg_buffer(resp_raw);
        }

        // 4. 调用dio读写远端内存
        {
            static thread_local rdma_rc::RDMABatch ba;
            ba.clear();
            switch (req.type) {
                case GetPageCXLRefOrProxyRequest::READ:
                    dest_daemon_conn->rdma_conn->prep_read(
                        ba, my_data_buf, my_rkey, my_size, rem_page_md_cache->remote_page_addr,
                        rem_page_md_cache->remote_page_rkey, false);
                    // DLOG("read size %u remote addr [%#lx, %u] to local addr [%#lx, %u]", my_size,
                    //      rem_page_md_cache->remote_page_addr,
                    //      rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE:
                    dest_daemon_conn->rdma_conn->prep_write(
                        ba, my_data_buf, my_rkey, my_size, rem_page_md_cache->remote_page_addr,
                        rem_page_md_cache->remote_page_rkey, false);
                    // DLOG("write size %u remote addr [%#lx, %u] to local addr [%#lx, %u]",
                    // my_size,
                    //      rem_page_md_cache->remote_page_addr,
                    //      rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
            }
            auto fu = dest_daemon_conn->rdma_conn->submit(ba);
            while (fu.try_get() == 1) {
            }
        }

        // 5. unlatch
        {
            using UnLatchRemotePageRPC = RPC_TYPE_STRUCT(rpc_master::unLatchRemotePage);
            auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(UnLatchRemotePageRPC::RequestType));
            auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(UnLatchRemotePageRPC::ResponseType));

            auto unlatch_req =
                reinterpret_cast<UnLatchRemotePageRPC::RequestType*>(req_raw.get_buf());
            unlatch_req->mac_id = daemon_context.m_daemon_id;
            unlatch_req->page_id = page_id;

            SpinPromise<void> pro;
            SpinFuture<void> fu = pro.get_future();
            rpc.enqueue_request(daemon_context.m_master_connection.peer_session,
                                UnLatchRemotePageRPC::rpc_type, req_raw, resp_raw,
                                erpc_general_bool_flag_cb, static_cast<void*>(&pro));

            while (fu.wait_for(0s) == std::future_status::timeout) {
                rpc.run_event_loop_once();
            }

            rpc.free_msg_buffer(req_raw);
            rpc.free_msg_buffer(resp_raw);
        }

        rem_page_md_cache->stats.add(getTimestamp());

        reply_ptr->refs = false;
        return {};
    }

    // TODO: page swap

    /**
     * 1. 向mn发送LatchPage(page_id)，获取mn上page的daemon，并锁定该page
     *      1.1
     * 如果本地page不够，与此同时向所有cn发起getPagePastAccessFreq()获取最久远的swapout page
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

    DLOG_FATAL("Not Support");
    goto retry;
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

    DLOG("new page %ld ---> %#lx", req.page_id, cxl_memory_offset);

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
        auto& rpc = daemon_context.get_erpc();

        using PageAllocRPC = RPC_TYPE_STRUCT(rpc_master::allocPage);

        auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::RequestType));
        auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::ResponseType));

        auto page_alloc_req = reinterpret_cast<PageAllocRPC::RequestType*>(req_raw.get_buf());
        page_alloc_req->mac_id = daemon_context.m_daemon_id;
        page_alloc_req->slab_size = page_alloc_req->slab_size;

        std::promise<void> pro;
        std::future<void> fu = pro.get_future();
        rpc.enqueue_request(daemon_context.m_master_connection.peer_session, PageAllocRPC::rpc_type,
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

GetPageRDMARefReply getPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   GetPageRDMARefRequest& req) {
    PageMetadata* page_meta;
    bool ret = daemon_context.m_page_table.find(req.page_id, &page_meta);
    DLOG_ASSERT(ret, "Can't find page %lu", req.page_id);

    uintptr_t local_addr =
        reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
        page_meta->cxl_memory_offset;
    ibv_mr* mr = daemon_context.get_mr(reinterpret_cast<void*>(local_addr));

    DLOG_ASSERT(mr->addr != nullptr, "The page %lu isn't registered to rdma memory", req.page_id);

    DLOG("get page %lu rdma ref [%#lx, %u]", req.page_id, local_addr, mr->rkey);

    GetPageRDMARefReply reply;
    reply.addr = local_addr;
    reply.rkey = mr->rkey;
    return reply;
}

TryMigratePageReply tryMigratePage(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   TryMigratePageRequest& req) {
    // TODO: page migrate

    TryMigratePageReply reply;
    return reply;
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

__notifyPerfReply __notifyPerf(DaemonContext& daemon_context,
                               DaemonToClientConnection& client_connection,
                               __notifyPerfRequest& req) {
    __DEBUG_START_PERF();
    return {};
}

__stopPerfReply __stopPerf(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, __stopPerfRequest& req) {
    exit(0);
}

}  // namespace rpc_daemon
