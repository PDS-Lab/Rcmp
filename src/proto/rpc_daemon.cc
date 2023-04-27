#include "proto/rpc_daemon.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>

#include "common.hpp"
#include "config.hpp"
#include "cort_sched.hpp"
#include "eRPC/erpc.h"
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

/**
 * @brief 在swap区申请一个page物理地址空间，只在需要迁移页面且本地的page区已满时调用
 *
 * @param daemon_context
 * @param req
 * @return void
 */
void allocSwapPageMemory(DaemonContext& daemon_context, AllocPageMemoryRequest& req);

/**
 * @brief 广播有当前page的ref的DN，删除其ref；并通知当前rack下所有访问过该page的client删除相应的缓存
 *
 *
 * @param daemon_context
 * @param page_id
 * @param page_meta
 * @return void
 */
void delPageRefAndCacheBroadcast(DaemonContext& daemon_context, page_id_t page_id,
                                 PageMetadata* page_meta);

/**
 * @brief 通知当前rack下所有访问过该page的client删除相应的缓存
 *
 * @param daemon_context
 * @param page_id
 * @param page_meta
 * @return void
 */
void delPageCacheBroadcast(DaemonContext& daemon_context, page_id_t page_id,
                           PageMetadata* page_meta);

/*************************************************************/

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

    this_cort::reset_resume_cond(
        [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
    this_cort::yield();

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

    DLOG("Connect with daemon [rack:%d --- id:%d], port = %d", daemon_connection.rack_id,
         daemon_connection.daemon_id, daemon_connection.port);

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
    SharedMutex* ref_lock;

    auto p_lock =
        daemon_context.m_page_ref_lock.find_or_emplace(page_id, []() { return new SharedMutex(); });
    ref_lock = p_lock.first->second;

    // 给page ref加读锁
    if (!ref_lock->try_lock_shared()) {
        this_cort::reset_resume_cond([&ref_lock]() { return ref_lock->try_lock_shared(); });
        this_cort::yield();
    }

    auto it = daemon_context.m_page_table.find(page_id);

    if (it != daemon_context.m_page_table.end()) {
        page_metadata = it->second;
        DLOG("insert ref_client for page %lu", page_id);
        page_metadata->ref_client.insert(&client_connection);

        reply_ptr = req.alloc_flex_resp(0);

        reply_ptr->refs = true;
        reply_ptr->offset = page_metadata->cxl_memory_offset;

        ref_lock->unlock_shared();
        return {};
    }

    DaemonToDaemonConnection* dest_daemon_conn;
    auto& rpc = daemon_context.get_erpc();

    auto p = daemon_context.m_hot_stats.find_or_emplace(page_id, [&]() {
        RemotePageMetaCache* rem_page_md_cache = new RemotePageMetaCache(8);

        // 如果是第一次访问该page，走DirectIO流程（rem_page_md_cache不存在时，说明一定时第一次访问）
        // 1. 获取mn上page的daemon，并锁定该page
        using LatchRemotePageRPC = RPC_TYPE_STRUCT(rpc_master::latchRemotePage);
        auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::RequestType));
        auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::ResponseType));

        auto latch_req = reinterpret_cast<LatchRemotePageRPC::RequestType*>(req_raw.get_buf());
        latch_req->mac_id = daemon_context.m_daemon_id;
        latch_req->page_id = page_id;
        latch_req->page_id_swap = 0;
        latch_req->isWriteLock = false;

        SpinPromise<void> pro;
        SpinFuture<void> fu = pro.get_future();
        rpc.enqueue_request(daemon_context.m_master_connection.peer_session,
                            LatchRemotePageRPC::rpc_type, req_raw, resp_raw,
                            erpc_general_bool_flag_cb, static_cast<void*>(&pro));

        // 1.1 一起等待latch完成
        this_cort::reset_resume_cond(
            [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        auto resp = reinterpret_cast<LatchRemotePageRPC::ResponseType*>(resp_raw.get_buf());

        // 2. 获取对端连接
        DaemonConnection* dest_daemon_conn_tmp;
        auto p = daemon_context.m_connect_table.find_or_emplace(resp->dest_daemon_id, [&]() {
            // 与该daemon建立erpc与RDMA RC
            DaemonToDaemonConnection* dd_conn = new DaemonToDaemonConnection();
            std::string server_uri = resp->dest_daemon_ipv4.get_string() + ":" +
                                     std::to_string(resp->dest_daemon_erpc_port);
            DLOG("server_uri = %s", server_uri.c_str());
            dd_conn->peer_session = rpc.create_session(server_uri, 0);
            dd_conn->daemon_id = resp->dest_daemon_id;
            dd_conn->rack_id = resp->dest_rack_id;
            dd_conn->ip = resp->dest_daemon_ipv4.get_string();
            dd_conn->port = resp->dest_daemon_erpc_port;
            DLOG("First connect daemon: %u. peer_session = %d", dd_conn->daemon_id,
            dd_conn->peer_session);

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

            this_cort::reset_resume_cond(
                [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
            this_cort::yield();

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

            return dd_conn;
        });

        dest_daemon_conn_tmp = p.first->second;

        rpc.free_msg_buffer(req_raw);
        rpc.free_msg_buffer(resp_raw);

        dest_daemon_conn = dynamic_cast<DaemonToDaemonConnection*>(dest_daemon_conn_tmp);

        // 3. 获取远端内存rdma ref
        {
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

            this_cort::reset_resume_cond(
                [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
            this_cort::yield();

            auto resp = reinterpret_cast<GetPageRDMARefRPC::ResponseType*>(resp_raw.get_buf());
            rem_page_md_cache->remote_page_addr = resp->addr;
            rem_page_md_cache->remote_page_rkey = resp->rkey;
            rem_page_md_cache->remote_page_daemon_conn = dest_daemon_conn;

            rpc.free_msg_buffer(req_raw);
            rpc.free_msg_buffer(resp_raw);
            DLOG("Get rdma ref\n");
        }

        // 4. unlatch
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

            this_cort::reset_resume_cond(
                [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
            this_cort::yield();

            rpc.free_msg_buffer(req_raw);
            rpc.free_msg_buffer(resp_raw);
        }

        return rem_page_md_cache;
    });

    RemotePageMetaCache* rem_page_md_cache = p.first->second;

    // 只有刚好等于水位线时，才进行迁移
    if (rem_page_md_cache->stats.freq() != 2) {
    // if (rem_page_md_cache->stats.freq() == page_hot_dio_swap_watermark) {
        // 启动DirectIO流程
        if (rem_page_md_cache->stats.freq() > 0) {
            dest_daemon_conn = rem_page_md_cache->remote_page_daemon_conn;
        }

        // printf("freq = %ld, rkey = %d, addr = %ld\n", rem_page_md_cache->stats.freq(),
        //    rem_page_md_cache->remote_page_rkey, rem_page_md_cache->remote_page_addr);

        // 5. 申请resp
        uintptr_t my_data_buf;
        uint32_t my_rkey;
        uint32_t my_size;

        switch (req.type) {
            case GetPageCXLRefOrProxyRequest::WRITE: {
                // 5.1 如果是写操作,等待获取CN上的数据
                using GetCurrentWriteDataRPC = RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData);
                auto wd_req_raw = client_connection.msgq_rpc->alloc_msg_buffer(
                    sizeof(GetCurrentWriteDataRPC::RequestType));
                auto wd_req =
                    reinterpret_cast<GetCurrentWriteDataRPC::RequestType*>(wd_req_raw.get_buf());
                wd_req->mac_id = daemon_context.m_daemon_id;
                wd_req->dio_write_buf = req.cn_write_buf;
                wd_req->dio_write_size = req.cn_write_size;

                SpinPromise<msgq::MsgBuffer> wd_pro;
                SpinFuture<msgq::MsgBuffer> wd_fu = wd_pro.get_future();

                client_connection.msgq_rpc->enqueue_request(GetCurrentWriteDataRPC::rpc_type,
                                                            wd_req_raw, msgq_general_bool_flag_cb,
                                                            static_cast<void*>(&wd_pro));
                this_cort::reset_resume_cond(
                    [&wd_fu]() { return wd_fu.wait_for(0s) != std::future_status::timeout; });
                this_cort::yield();

                msgq::MsgBuffer wd_resp_raw = wd_fu.get();
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
                // 5.2 如果是读操作，则动态申请读取resp buf
                reply_ptr = req.alloc_flex_resp(req.cn_read_size);

                ibv_mr* mr = daemon_context.get_mr(reply_ptr->read_data);
                my_data_buf = reinterpret_cast<uintptr_t>(reply_ptr->read_data);
                my_rkey = mr->rkey;
                my_size = req.cn_read_size;
                break;
            }
            case GetPageCXLRefOrProxyRequest::WRITE_RAW: {
                ibv_mr* mr = daemon_context.get_mr(req.cn_write_raw_buf);

                my_data_buf = reinterpret_cast<uintptr_t>(req.cn_write_raw_buf);
                my_rkey = mr->rkey;
                my_size = req.cn_write_size;
                DLOG("WRITE_RAW: my_rkey = %u", my_rkey);
                reply_ptr = req.alloc_flex_resp(0);
                break;
            }
        }

        // 6. 调用dio读写远端内存
        {
            rdma_rc::RDMABatchFixed<1> ba;
            switch (req.type) {
                case GetPageCXLRefOrProxyRequest::READ:
                    dest_daemon_conn->rdma_conn->prep_read(
                        ba, my_data_buf, my_rkey, my_size,
                        (rem_page_md_cache->remote_page_addr + page_offset),
                        rem_page_md_cache->remote_page_rkey, false);
                    DLOG("read size %u remote addr [%#lx, %u] to local addr [%#lx, %u]", my_size,
                         rem_page_md_cache->remote_page_addr,
                         rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE:
                case GetPageCXLRefOrProxyRequest::WRITE_RAW:
                    dest_daemon_conn->rdma_conn->prep_write(
                        ba, my_data_buf, my_rkey, my_size,
                        (rem_page_md_cache->remote_page_addr + page_offset),
                        rem_page_md_cache->remote_page_rkey, false);
                    DLOG("write size %u remote addr [%#lx, %u] to local addr [%#lx, %u]",
                    my_size,
                         rem_page_md_cache->remote_page_addr,
                         rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
            }
            auto fu = dest_daemon_conn->rdma_conn->submit(ba);

            this_cort::reset_resume_cond([&fu]() { return fu.try_get() == 0; });
            this_cort::yield();
        }

        rem_page_md_cache->stats.add(getTimestamp());

        // 给page ref取消读锁
        ref_lock->unlock_shared();

        reply_ptr->refs = false;
        return {};
    } else  // page swap
    {
        // 给page ref取消读锁
        ref_lock->unlock_shared();

        // 清空其访问的记录，避免多个CN的读写引起同时对一个页的迁移
        // TODO： freq改为原子变量
        rem_page_md_cache->stats.clear();

        // 1 为page swap的区域准备内存，并确定是否需要换出页
        dest_daemon_conn = rem_page_md_cache->remote_page_daemon_conn;

        bool isSwap;
        page_id_t swap_page_id = 0;
        uintptr_t swapin_addr, swapout_addr = 0;
        uint32_t swapin_key, swapout_key = 0;
        SharedMutex* ref_lock_swapout;
        ibv_mr* mr;
        PageMetadata* swap_page_metadata;

        // 交换的情况，需要将自己的一个page交换到对方, 这个读写过程由对方完成
        AllocPageMemoryRequest inner_req;
        inner_req.start_page_id = page_id;  // 此时以要换进页的page id来申请分配一个页
        inner_req.count = 1;  // 此时以要换进页的page id来申请分配一个页
        inner_req.mac_id = daemon_context.m_daemon_id;

        // 首先为即将迁移到本地的page申请内存
        AllocPageMemoryReply allocPageMemoryReply =
            allocPageMemory(daemon_context, daemon_context.m_master_connection, inner_req);
        page_metadata = allocPageMemoryReply.pageMetaVec.front();
        DLOG("allocPageMemory: page_metadata->offset = %#lx", page_metadata->cxl_memory_offset);

        if (daemon_context.m_max_data_page_num == daemon_context.m_current_used_page_num) {
            /* 1.1
             * 若本地page不够（询问master），与此同时向所有cn发起getPagePastAccessFreq()获取最久远的swap
             * out page */
            uint64_t oldest_time = UINT64_MAX;
            uint64_t last_time_tmp = 0;
            for (size_t i = 0; i < daemon_context.m_client_connect_table.size(); i++) {
                DaemonToClientConnection* client_conn = daemon_context.m_client_connect_table[i];
                using GetPagePastAccessFreqRPC = RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq);
                auto wd_req_raw = client_conn->msgq_rpc->alloc_msg_buffer(
                    sizeof(GetPagePastAccessFreqRPC::RequestType));
                auto wd_req =
                    reinterpret_cast<GetPagePastAccessFreqRPC::RequestType*>(wd_req_raw.get_buf());
                wd_req->mac_id = daemon_context.m_daemon_id;

                SpinPromise<msgq::MsgBuffer> pro;
                SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
                client_conn->msgq_rpc->enqueue_request(GetPagePastAccessFreqRPC::rpc_type,
                                                       wd_req_raw, msgq_general_bool_flag_cb,
                                                       static_cast<void*>(&pro));
                this_cort::reset_resume_cond(
                    [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
                this_cort::yield();

                msgq::MsgBuffer resp_raw = fu.get();
                auto resp =
                    reinterpret_cast<GetPagePastAccessFreqRPC::ResponseType*>(resp_raw.get_buf());

                last_time_tmp = resp->last_access_ts;
                if (oldest_time > last_time_tmp) {
                    oldest_time = last_time_tmp;  // 越小，越旧
                    swap_page_id = resp->oldest_page_id;
                }
                client_conn->msgq_rpc->free_msg_buffer(resp_raw);
            }

            /* 1.2 注册换出页的地址，并获取rkey */
            auto p_swap_page_meta = daemon_context.m_page_table.find(swap_page_id);
            swap_page_metadata = p_swap_page_meta->second;
            swapout_addr =
                reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
                swap_page_metadata->cxl_memory_offset;
            mr = daemon_context.get_mr(reinterpret_cast<void*>(swapout_addr));
            swapout_key = mr->rkey;

            // 给即将换出页的page_meta上写锁
            auto p_lock_swapout = daemon_context.m_page_ref_lock.find_or_emplace(
                page_id, []() { return new SharedMutex(); });
            ref_lock_swapout = p_lock_swapout.first->second;

            if (!ref_lock_swapout->try_lock()) {
                this_cort::reset_resume_cond(
                    [&ref_lock_swapout]() { return ref_lock_swapout->try_lock(); });
                this_cort::yield();
            }
        }

        swapin_addr =
            reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
            page_metadata->cxl_memory_offset;
        mr = daemon_context.get_mr(reinterpret_cast<void*>(swapin_addr));
        swapin_key = mr->rkey;

        /* 2. 向mn发送LatchPage(page_id)，获取mn上page的daemon，并锁定该page */
        using LatchRemotePageRPC = RPC_TYPE_STRUCT(rpc_master::latchRemotePage);
        auto latch_req_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::RequestType));
        auto latch_resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(LatchRemotePageRPC::ResponseType));

        auto latch_req =
            reinterpret_cast<LatchRemotePageRPC::RequestType*>(latch_req_raw.get_buf());
        latch_req->mac_id = daemon_context.m_daemon_id;
        latch_req->page_id = page_id;
        latch_req->page_id_swap = swap_page_id;
        latch_req->isWriteLock = true;

        SpinPromise<void> latch_pro;
        SpinFuture<void> latch_fu = latch_pro.get_future();
        rpc.enqueue_request(daemon_context.m_master_connection.peer_session,
                            LatchRemotePageRPC::rpc_type, latch_req_raw, latch_resp_raw,
                            erpc_general_bool_flag_cb, static_cast<void*>(&latch_pro));

        // 2.1 一起等待latch完成
        // 2.1.1
        // 在等待latche完成的过程，如果有需要换出的页，则广播有当前要swapout的page的ref的DN，删除其ref，并通知当前rack下所有访问过该page的client删除相应的缓存
        if (swap_page_id) {
            delPageRefAndCacheBroadcast(daemon_context, swap_page_id, page_metadata);
        }
        // 2.1.2 等待latch完成
        this_cort::reset_resume_cond(
            [&latch_fu]() { return latch_fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        rpc.free_msg_buffer(latch_req_raw);
        rpc.free_msg_buffer(latch_resp_raw);

        /* 3. 向daemon发送page迁移（tryMigratePage），等待其完成迁移，返回RPC */
        using TryMigratePageRPC = RPC_TYPE_STRUCT(rpc_daemon::tryMigratePage);

        auto migrate_req_raw = rpc.alloc_msg_buffer_or_die(sizeof(TryMigratePageRPC::RequestType));
        auto migrate_resp_raw =
            rpc.alloc_msg_buffer_or_die(sizeof(TryMigratePageRPC::ResponseType));

        auto migrate_req =
            reinterpret_cast<TryMigratePageRPC::RequestType*>(migrate_req_raw.get_buf());
        migrate_req->mac_id = daemon_context.m_daemon_id;
        migrate_req->score = 0x2342345;
        migrate_req->page_id = page_id;  // 期望迁移的page
        migrate_req->swap_page_id = swap_page_id;
        migrate_req->swapin_page_addr = swapin_addr;
        migrate_req->swapin_page_rkey = swapin_key;
        migrate_req->swapout_page_addr = swapout_addr;
        migrate_req->swapout_page_rkey = swapout_key;

        DLOG(
            "DN %u: Expect inPage %lu (from DN: %u) outPage %lu. swapin_addr = %ld, swapin_key = "
            "%d",
            daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id, swap_page_id,
            swapin_addr, swapin_key);

        std::promise<void> migrate_pro;
        std::future<void> migrate_fu = migrate_pro.get_future();
        rpc.enqueue_request(dest_daemon_conn->peer_session, TryMigratePageRPC::rpc_type,
                            migrate_req_raw, migrate_resp_raw, erpc_general_promise_flag_cb,
                            static_cast<void*>(&migrate_pro));

        this_cort::reset_resume_cond(
            [&migrate_fu]() { return migrate_fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        auto migrate_resp =
            reinterpret_cast<TryMigratePageRPC::ResponseType*>(migrate_resp_raw.get_buf());

        isSwap = migrate_resp->swaped;

        rpc.free_msg_buffer(migrate_req_raw);
        rpc.free_msg_buffer(migrate_resp_raw);
        // DLOG("DN %u: recv migrate reply!", daemon_context.m_daemon_id);

        // 迁移完成，更新tlb
        daemon_context.m_current_used_page_num++;
        daemon_context.m_page_table.insert(page_id, page_metadata);
        if (isSwap) {
            // TODO: 回收迁移走的page，然后将swap区的page，换到正常的page区
            // 回收迁移走的页面
            daemon_context.m_cxl_page_allocator->deallocate(swap_page_metadata->cxl_memory_offset);
            daemon_context.m_current_used_page_num--;
            // 清除迁移走的page位于当前DN上的元数据
            daemon_context.m_page_table.erase(swap_page_id);
            // 换出页已迁移完毕，解锁
            ref_lock_swapout->unlock();
        }

        /* 4. 向mn发送unLatchPageAndBalance，更改page dir，返回RPC*/
        // DLOG("DN %u: unLatchPageAndBalance!", daemon_context.m_daemon_id);
        using unLatchPageAndBalanceRPC = RPC_TYPE_STRUCT(rpc_master::unLatchPageAndBalance);
        auto unlatchB_req_raw =
            rpc.alloc_msg_buffer_or_die(sizeof(unLatchPageAndBalanceRPC::RequestType));
        auto unlatchB_resp_raw =
            rpc.alloc_msg_buffer_or_die(sizeof(unLatchPageAndBalanceRPC::ResponseType));

        auto unlatchB_req =
            reinterpret_cast<unLatchPageAndBalanceRPC::RequestType*>(unlatchB_req_raw.get_buf());
        unlatchB_req->mac_id = daemon_context.m_daemon_id;
        unlatchB_req->page_id = page_id;  // 换入的page(原本在远端)
        unlatchB_req->new_rack_id = daemon_context.m_options.rack_id;  // 自己的rack id
        unlatchB_req->new_daemon_id = daemon_context.m_daemon_id;      // 自己的daemon id
        unlatchB_req->page_id_swap = swap_page_id;  // 换出的page(原本在本地)，若为0，则表示无换出页
        unlatchB_req->new_rack_id_swap = dest_daemon_conn->rack_id;
        ;                                                                // 对方的rack id
        unlatchB_req->new_daemon_id_swap = dest_daemon_conn->daemon_id;  // 对方的daemon id

        SpinPromise<void> unlatchB_pro;
        SpinFuture<void> unlatchB_fu = unlatchB_pro.get_future();
        rpc.enqueue_request(daemon_context.m_master_connection.peer_session,
                            unLatchPageAndBalanceRPC::rpc_type, unlatchB_req_raw, unlatchB_resp_raw,
                            erpc_general_bool_flag_cb, static_cast<void*>(&unlatchB_pro));

        this_cort::reset_resume_cond(
            [&unlatchB_fu]() { return unlatchB_fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        rpc.free_msg_buffer(unlatchB_req_raw);
        rpc.free_msg_buffer(unlatchB_resp_raw);
        DLOG("DN %u: Expect inPage %lu (from DN: %u) swap page finished!",
             daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id);
    }

    goto retry;
}

AllocPageMemoryReply allocPageMemory(DaemonContext& daemon_context,
                                     DaemonToMasterConnection& master_connection,
                                     AllocPageMemoryRequest& req) {
    DLOG_ASSERT(
        daemon_context.m_current_used_page_num + req.count <= daemon_context.m_max_data_page_num,
        "Can't allocate more page memory");
    AllocPageMemoryReply reply;
    for (size_t c = 0; c < req.count; ++c) {
        offset_t cxl_memory_offset = daemon_context.m_cxl_page_allocator->allocate(1);
        DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

        PageMetadata* page_metadata = new PageMetadata();
        page_metadata->cxl_memory_offset = cxl_memory_offset;

        reply.pageMetaVec.push_back(page_metadata);
        // daemon_context.m_current_used_page_num++;
        // daemon_context.m_page_table.insert(req.start_page_id + c, page_metadata);

        DLOG("new page %ld ---> %#lx", req.start_page_id + c, cxl_memory_offset);
    }
    reply.ret = true;
    return reply;
}

AllocPageReply allocPage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                         AllocPageRequest& req) {
    DLOG("alloc %lu new pages", req.count);

    // 向Master调用allocPage
    auto& rpc = daemon_context.get_erpc();

    using PageAllocRPC = RPC_TYPE_STRUCT(rpc_master::allocPage);

    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::RequestType));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::ResponseType));

    auto page_alloc_req = reinterpret_cast<PageAllocRPC::RequestType*>(req_raw.get_buf());
    page_alloc_req->mac_id = daemon_context.m_daemon_id;
    page_alloc_req->count = req.count;

    std::promise<void> pro;
    std::future<void> fu = pro.get_future();
    rpc.enqueue_request(daemon_context.m_master_connection.peer_session, PageAllocRPC::rpc_type,
                        req_raw, resp_raw, erpc_general_promise_flag_cb, static_cast<void*>(&pro));

    // 等待期间可能出现由于本地page不足而发生page swap
    this_cort::reset_resume_cond(
        [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
    this_cort::yield();

    auto resp = reinterpret_cast<PageAllocRPC::ResponseType*>(resp_raw.get_buf());

    page_id_t start_page_id = resp->start_page_id;

    AllocPageMemoryRequest local_req;
    local_req.mac_id = daemon_context.m_daemon_id;
    local_req.start_page_id = start_page_id;
    local_req.count = resp->start_count;
    AllocPageMemoryReply allocPageMemoryReply =
        allocPageMemory(daemon_context, daemon_context.m_master_connection, local_req);

    daemon_context.m_current_used_page_num += local_req.count;
    for (size_t c = 0; c < local_req.count; ++c) {
        PageMetadata *page_meta = allocPageMemoryReply.pageMetaVec[c];
        // page_meta->ref_client.insert(&client_connection);
        daemon_context.m_page_table.insert(local_req.start_page_id + c, page_meta);
        DLOG("allocPage: insert page %lu, offset = %ld ", local_req.start_page_id + c,
             allocPageMemoryReply.pageMetaVec[c]->cxl_memory_offset);
    }

    rpc.free_msg_buffer(req_raw);
    rpc.free_msg_buffer(resp_raw);

    AllocPageReply reply;
    reply.start_page_id = start_page_id;
    return reply;
}

FreePageReply freePage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                       FreePageRequest& req) {
    DLOG_FATAL("Not Support");
}

AllocReply alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                 AllocRequest& req) {
    DLOG_FATAL("Not Support");
}

FreeReply free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               FreeRequest& req) {
    DLOG_FATAL("Not Support");
}

GetPageRDMARefReply getPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   GetPageRDMARefRequest& req) {
    PageMetadata* page_meta;
    auto it = daemon_context.m_page_table.find(req.page_id);
    DLOG_ASSERT(it != daemon_context.m_page_table.end(), "Can't find page %lu", req.page_id);

    page_meta = it->second;

    uintptr_t local_addr =
        reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
        page_meta->cxl_memory_offset;
    ibv_mr* mr = daemon_context.get_mr(reinterpret_cast<void*>(local_addr));

    DLOG_ASSERT(mr->addr != nullptr, "The page %lu isn't registered to rdma memory", req.page_id);

    DaemonConnection* daemon_conn_temp;
    auto it_daemon_conn = daemon_context.m_connect_table.find(req.mac_id);
    daemon_conn_temp = it_daemon_conn->second;
    DaemonToDaemonConnection* daemon_conn =
        dynamic_cast<DaemonToDaemonConnection*>(daemon_conn_temp);

    daemon_connection.daemon_id = daemon_conn->daemon_id;
    daemon_connection.rack_id = daemon_conn->rack_id;
    daemon_connection.peer_session = daemon_conn->peer_session;
    daemon_connection.ip = daemon_conn->ip;
    daemon_connection.port = daemon_conn->port;
    daemon_connection.rdma_conn = daemon_conn->rdma_conn;
    page_meta->ref_daemon.insert(&daemon_connection);

    DLOG("get page %lu rdma ref [%#lx, %u], local [%#lx, %u],  peer_session = %d, daemon_id = %u", req.page_id,
         local_addr, mr->rkey, local_addr, mr->lkey, daemon_connection.peer_session, daemon_connection.daemon_id);

    GetPageRDMARefReply reply;
    reply.addr = local_addr;
    reply.rkey = mr->rkey;
    return reply;
}

DelPageRDMARefReply delPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   DelPageRDMARefRequest& req) {
    SharedMutex* ref_lock;
    auto it_lock = daemon_context.m_page_ref_lock.find(req.page_id);
    DLOG_ASSERT(it_lock != daemon_context.m_page_ref_lock.end(), "Can't find page %lu's ref lock",
                req.page_id);
    ref_lock = it_lock->second;

    // DLOG("DN %u: delPageRDMARef page %lu lock", daemon_context.m_daemon_id, req.page_id);
    // 给page ref加写锁
    if (!ref_lock->try_lock()) {
        this_cort::reset_resume_cond([&ref_lock]() { return ref_lock->try_lock_shared(); });
        this_cort::yield();
    }

    auto it = daemon_context.m_hot_stats.find(req.page_id);
    DLOG_ASSERT(it != daemon_context.m_hot_stats.end(), "Can't find page %lu's ref", req.page_id);

    // 清除该page的ref
    daemon_context.m_hot_stats.erase(req.page_id);
    DLOG("DN %u: Del page %ld rdma ref", daemon_context.m_daemon_id, req.page_id);

    ref_lock->unlock();
    // DLOG("DN %u: delPageRDMARef page %lu unlock", daemon_context.m_daemon_id, req.page_id);
    DelPageRDMARefReply reply;
    reply.isDel = true;
    return reply;
}

void allocSwapPageMemory(DaemonContext& daemon_context, AllocPageMemoryRequest& req) {
    DLOG_ASSERT(daemon_context.m_current_used_swap_page_num + req.count <
                    daemon_context.m_max_swap_page_num,
                "Can't allocate more page memory");

    for (size_t c = 0; c < req.count; ++c) {
        offset_t cxl_memory_offset = daemon_context.m_cxl_page_allocator->allocate(1);
        DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");
        daemon_context.m_current_used_swap_page_num++;

        PageMetadata* page_metadata = new PageMetadata();
        page_metadata->cxl_memory_offset = cxl_memory_offset;

        daemon_context.m_swap_page_table.insert(req.start_page_id + c, page_metadata);

        DLOG("new page %ld ---> %#lx", req.start_page_id + c, cxl_memory_offset);
    }
}

void delPageRefAndCacheBroadcast(DaemonContext& daemon_context, page_id_t page_id,
                                 PageMetadata* page_meta) {
    auto& rpc = daemon_context.get_erpc();
    // DLOG("DN %u: delPageRefBroadcast page %lu", daemon_context.m_daemon_id, page_id);
    // del page ref
    using DelPageRDMARefRPC = RPC_TYPE_STRUCT(rpc_daemon::delPageRDMARef);
    std::vector<std::future<void>> fu_ref_vec;
    std::vector<erpc::MsgBufferWrap> req_raw_ref_vec;
    std::vector<erpc::MsgBufferWrap> resp_raw_ref_vec;
    // del page cache
    std::vector<SpinFuture<msgq::MsgBuffer>> fu_cache_vec;

    for (auto daemon_conn : page_meta->ref_daemon) {
        // DLOG("DN %u: delPageRefBroadcast for i = %ld, peer_session = %d, daemon_id = %u",
        // daemon_context.m_daemon_id, i, daemon_conn->peer_session, daemon_conn->daemon_id); i++;

        auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(DelPageRDMARefRPC::RequestType));
        auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(DelPageRDMARefRPC::ResponseType));

        auto ref_req = reinterpret_cast<DelPageRDMARefRPC::RequestType*>(req_raw.get_buf());
        ref_req->mac_id = daemon_context.m_daemon_id;
        ref_req->page_id = page_id;  // 准备删除ref的page id

        std::promise<void> pro;
        std::future<void> fu = pro.get_future();
        rpc.enqueue_request(daemon_conn->peer_session, DelPageRDMARefRPC::rpc_type, req_raw,
                            resp_raw, erpc_general_promise_flag_cb, static_cast<void*>(&pro));

        fu_ref_vec.push_back(std::move(fu));
        req_raw_ref_vec.push_back(req_raw);
        resp_raw_ref_vec.push_back(resp_raw);
    }

    using RemovePageCacheRPC = RPC_TYPE_STRUCT(rpc_client::removePageCache);
    for (auto client_conn : page_meta->ref_client) {
        DLOG("DN %u: delPageCacheBroadcast client_id = %u",
        daemon_context.m_daemon_id, client_conn->client_id);

        auto wd_req_raw =
            client_conn->msgq_rpc->alloc_msg_buffer(sizeof(RemovePageCacheRPC::RequestType));
        auto wd_req = reinterpret_cast<RemovePageCacheRPC::RequestType*>(wd_req_raw.get_buf());
        wd_req->mac_id = daemon_context.m_daemon_id;
        wd_req->page_id = page_id;

        SpinPromise<msgq::MsgBuffer> pro;
        SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
        client_conn->msgq_rpc->enqueue_request(RemovePageCacheRPC::rpc_type, wd_req_raw,
                                               msgq_general_bool_flag_cb, static_cast<void*>(&pro));

        fu_cache_vec.push_back(std::move(fu));
    }

    for (size_t i = 0; i < fu_ref_vec.size(); i++) {
        auto fu = std::move(fu_ref_vec[i]);
        this_cort::reset_resume_cond(
            [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        auto resp =
            reinterpret_cast<DelPageRDMARefRPC::ResponseType*>(resp_raw_ref_vec[i].get_buf());

        rpc.free_msg_buffer(req_raw_ref_vec[i]);
        rpc.free_msg_buffer(resp_raw_ref_vec[i]);
    }

    size_t i = 0;
    for (auto client_conn : page_meta->ref_client) {
        auto fu = std::move(fu_cache_vec[i]);
        this_cort::reset_resume_cond(
            [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        msgq::MsgBuffer resp_raw = fu.get();
        client_conn->msgq_rpc->free_msg_buffer(resp_raw);
        i++;
    }
}

TryMigratePageReply tryMigratePage(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   TryMigratePageRequest& req) {
    // 获取预交换的page的本地元数据
    PageMetadata* page_meta;
    auto p_page_meta = daemon_context.m_page_table.find(req.page_id);
    DLOG_ASSERT(p_page_meta != daemon_context.m_page_table.end(), "Can't find page %lu",
                req.page_id);
    page_meta = p_page_meta->second;
    DLOG("DN: %u recv tryMigratePage for page %lu. swap page = %lu", daemon_context.m_daemon_id,
         req.page_id, req.swap_page_id);

    // 广播有当前page的ref的DN，删除其ref, 并通知当前rack下所有访问过该page的client删除相应的缓存
    DLOG("DN %u: delPageRefBroadcast page %lu", daemon_context.m_daemon_id, req.page_id);
    delPageRefAndCacheBroadcast(daemon_context, req.page_id, page_meta);

    // 使用RDMA单边读写将page上的内容进行交换
    DLOG("DN %u: rdma write. swapin_addr = %ld, swapin_key = %d", daemon_context.m_daemon_id,
         req.swapin_page_addr, req.swapin_page_rkey);
    uintptr_t local_addr =
        reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
        page_meta->cxl_memory_offset;
    ibv_mr* mr = daemon_context.get_mr(reinterpret_cast<void*>(local_addr));
    uint32_t lkey = mr->lkey;

    DaemonConnection* daemon_conn_temp;
    auto p_daemon_conn = daemon_context.m_connect_table.find(req.mac_id);
    daemon_conn_temp = p_daemon_conn->second;
    DaemonToDaemonConnection* daemon_conn =
        dynamic_cast<DaemonToDaemonConnection*>(daemon_conn_temp);

    rdma_rc::RDMABatch ba;
    daemon_conn->rdma_conn->prep_write(ba, local_addr, lkey, page_size, req.swapin_page_addr,
                                       req.swapin_page_rkey, false);

    DLOG("rdma write mid. swapout_page_addr = %lu", req.swapout_page_addr);
    bool isSwape, isFull;
    PageMetadata* local_page_meta;
    AllocPageMemoryReply allocPageMemoryReply;
    if (req.swapout_page_addr == 0 && req.swapout_page_rkey == 0) {
        isSwape = false;
    } else {
        isSwape = true;
        // 交换的情况，需要读对方的page到本地
        AllocPageMemoryRequest inner_req;
        inner_req.start_page_id = req.swap_page_id;  // 此时以要换进页的page id来申请分配一个页
        inner_req.count = 1;
        inner_req.mac_id = daemon_context.m_daemon_id;

        if (daemon_context.m_max_data_page_num > daemon_context.m_current_used_page_num) {
            // page还有剩余，则直接迁移到本地page上
            allocPageMemoryReply =
                allocPageMemory(daemon_context, daemon_context.m_master_connection, inner_req);
            auto p = daemon_context.m_page_table.find(req.swap_page_id);
            DLOG_ASSERT(p != daemon_context.m_page_table.end(), "Can't find page %lu",
                        req.swap_page_id);
            local_page_meta = p->second;
            isFull = false;
        } else {
            // 若page没有剩余，先迁移到swap区，之后再交换
            allocSwapPageMemory(daemon_context, inner_req);
            auto p = daemon_context.m_swap_page_table.find(req.swap_page_id);
            DLOG_ASSERT(p != daemon_context.m_swap_page_table.end(), "Can't find page %lu",
                        req.swap_page_id);
            local_page_meta = p->second;
            isFull = true;
        }

        uintptr_t swapin_addr =
            reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
            local_page_meta->cxl_memory_offset;
        mr = daemon_context.get_mr(reinterpret_cast<void*>(local_page_meta));
        lkey = mr->lkey;
        daemon_conn->rdma_conn->prep_read(ba, swapin_addr, lkey, page_size, req.swapout_page_addr,
                                          req.swapout_page_rkey, false);
        // 如果有读，也在后面和前面的写一块submit
    }
    auto fu = daemon_conn->rdma_conn->submit(ba);
    DLOG("DN %u: rdma write submit. local_addr = %ld, lkey = %u, req.swapin_page_addr = %ld,  req.swapin_page_rkey = %u", daemon_context.m_daemon_id, local_addr, lkey, req.swapin_page_addr,  req.swapin_page_rkey);
    this_cort::reset_resume_cond([&fu]() { return fu.try_get() == 0; });
    this_cort::yield();

    DLOG("DN %u: reply", daemon_context.m_daemon_id);
    TryMigratePageReply reply;
    reply.swaped = isSwape;
    // 回收迁移走的页面
    daemon_context.m_cxl_page_allocator->deallocate(page_meta->cxl_memory_offset);
    daemon_context.m_current_used_page_num--;
    // 清除即将迁移page位于当前DN上的元数据
    daemon_context.m_page_table.erase(req.page_id);

    if (isSwape && isFull) {
        // 若page没有剩余，迁移到了swap区，现在再迁移到page区域
        DLOG_ASSERT(daemon_context.m_max_data_page_num > daemon_context.m_current_used_page_num,
                    "Page is full, can't swap in.");
        daemon_context.m_current_used_page_num++;
        daemon_context.m_page_table.insert(req.swap_page_id,
                                           allocPageMemoryReply.pageMetaVec.front());

        // daemon_context.m_page_table.insert(req.swap_page_id, local_page_meta);
        // daemon_context.m_current_used_page_num++;
        // daemon_context.m_swap_page_table.erase(req.swap_page_id);
        // daemon_context.m_current_used_swap_page_num--;
    }
    DLOG("DN %u: finished migrate!", daemon_context.m_daemon_id);
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
    exit(-1);
}

}  // namespace rpc_daemon
