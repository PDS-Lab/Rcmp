#include "proto/rpc_daemon.hpp"

#include <atomic>
#include <boost/fiber/operations.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>

#include "common.hpp"
#include "config.hpp"
#include "eRPC/erpc.h"
#include "lock.hpp"
#include "log.hpp"
#include "promise.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_client.hpp"
#include "proto/rpc_master.hpp"
#include "proto/rpc_register.hpp"
#include "rdma_rc.hpp"
#include "stats.hpp"
#include "udp_client.hpp"
#include "utils.hpp"

using namespace std::chrono_literals;
namespace rpc_daemon {

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
                                 PageMetadata* page_meta, mac_id_t unless_daemon = -1);

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

/**
 * @brief 临时申请一个page物理地址空间，并不进行插入操作。（包括swap区的分配）
 *
 * @param daemon_context
 * @param start_page_id
 * @param count
 * @param pageMetaVec
 * @return void
 */
void allocPageMemoryTmp(DaemonContext& daemon_context, page_id_t start_page_id, size_t count,
                        std::vector<PageMetadata*>& pageMetaVec);

/*************************************************************/

void joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
              JoinRackRequest& req, ResponseHandle<JoinRackReply>& resp_handle) {
    DLOG_ASSERT(req.rack_id == daemon_context.m_options.rack_id,
                "Can't join different rack %d ---> %d", req.rack_id,
                daemon_context.m_options.rack_id);

    // 1. 通知master获取mac id
    auto fu = daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
        rpc_master::joinClient, {
                                    .rack_id = daemon_context.m_options.rack_id,
                                });

    auto& resp = fu.get();

    client_connection.client_id = resp.mac_id;

    daemon_context.m_conn_manager.AddConnection(client_connection.client_id, &client_connection);

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

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.client_mac_id = client_connection.client_id;
    reply.daemon_mac_id = daemon_context.m_daemon_id;
}

void crossRackConnect(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                      CrossRackConnectRequest& req,
                      ResponseHandle<CrossRackConnectReply>& resp_handle) {
    DLOG_ASSERT(req.conn_mac_id == daemon_context.m_daemon_id, "Can't connect this daemon");

    daemon_context.m_conn_manager.AddConnection(req.mac_id, &daemon_connection);

    daemon_connection.daemon_id = req.mac_id;
    daemon_connection.rack_id = req.rack_id;
    daemon_connection.ip = req.ip.get_string();
    daemon_connection.port = req.port;
    daemon_connection.erpc_conn.reset(
        new ErpcClient(daemon_context.GetErpc(), daemon_connection.ip, daemon_connection.port));

    DLOG("Connect with daemon [rack:%d --- id:%d], port = %d", daemon_connection.rack_id,
         daemon_connection.daemon_id, daemon_connection.port);

    auto local_addr = daemon_context.m_listen_conn.get_local_addr();

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    reply.rdma_ipv4 = local_addr.first;
    reply.rdma_port = local_addr.second;
}

void getPageCXLRefOrProxy(DaemonContext& daemon_context,
                          DaemonToClientConnection& client_connection,
                          GetPageCXLRefOrProxyRequest& req,
                          ResponseHandle<GetPageCXLRefOrProxyReply>& resp_handle) {
    PageMetadata* page_metadata;
    page_id_t page_id = GetPageID(req.gaddr);
    offset_t page_offset = GetPageOffset(req.gaddr);
retry:
    CortSharedMutex* ref_shmut =
        daemon_context.m_page_ref_lock
            .find_or_emplace(page_id, []() { return new CortSharedMutex(); })
            .first->second;

    // 给page ref加读锁
    std::shared_lock<CortSharedMutex> page_ref_lock(*ref_shmut);

    auto page_it = daemon_context.m_page_table.table.find(page_id);

    if (page_it != daemon_context.m_page_table.table.end()) {
        daemon_context.m_stats.page_hit++;

        page_metadata = page_it->second;
        // DLOG("insert ref_client for page %lu", page_id);
        page_metadata->ref_client.insert(&client_connection);

        resp_handle.Init();
        auto& reply = resp_handle.Get();
        reply.refs = true;
        reply.offset = page_metadata->cxl_memory_offset;

        return;
    }

    // 本地未命中

    daemon_context.m_stats.page_miss++;

    DaemonToDaemonConnection* dest_daemon_conn;

    auto page_hot_pair = daemon_context.m_hot_stats.find_or_emplace(page_id, [&]() {
        // 如果是第一次访问该page，走DirectIO流程（rem_page_md_cache不存在时，说明一定时第一次访问）
        RemotePageMetaCache* rem_page_md_cache =
            new RemotePageMetaCache(8, daemon_context.m_options.hot_decay_lambda);

        // 1. 获取mn上page的daemon，并锁定该page
        {
            auto latch_fu =
                daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
                    rpc_master::latchRemotePage, {
                                                     .mac_id = daemon_context.m_daemon_id,
                                                     .isWriteLock = false,
                                                     .page_id = page_id,
                                                     .page_id_swap = 0,
                                                 });

            auto& latch_resp = latch_fu.get();

            // 获取对端连接
            dest_daemon_conn = dynamic_cast<DaemonToDaemonConnection*>(
                daemon_context.m_conn_manager.GetConnection(latch_resp.dest_daemon_id));
        }

        // 3. 获取远端内存rdma ref
        {
            auto rref_fu = dest_daemon_conn->erpc_conn->call<CortPromise>(
                rpc_daemon::getPageRDMARef, {
                                                .mac_id = daemon_context.m_daemon_id,
                                                .page_id = page_id,
                                            });

            auto& rref_resp = rref_fu.get();
            rem_page_md_cache->remote_page_addr = rref_resp.addr;
            rem_page_md_cache->remote_page_rkey = rref_resp.rkey;
            rem_page_md_cache->remote_page_daemon_conn = dest_daemon_conn;
        }

        // 4. unlatch
        {
            auto unlatch_fu =
                daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
                    rpc_master::unLatchRemotePage, {
                                                       .mac_id = daemon_context.m_daemon_id,
                                                       .page_id = page_id,
                                                   });

            auto& resp = unlatch_fu.get();
        }

        return rem_page_md_cache;
    });

    auto page_hot_iter = page_hot_pair.first;
    RemotePageMetaCache* rem_page_md_cache = page_hot_iter->second;

    size_t current_hot = rem_page_md_cache->stats.add(getTimestamp());
    // 只有刚好等于水位线时，才进行迁移
    if (current_hot != daemon_context.m_options.hot_swap_watermark) {
        daemon_context.m_stats.page_dio++;

        // 启动DirectIO流程
        dest_daemon_conn = rem_page_md_cache->remote_page_daemon_conn;

        // printf("freq = %ld, rkey = %d, addr = %ld\n", current_hot,
        //    rem_page_md_cache->remote_page_rkey, rem_page_md_cache->remote_page_addr);

        // 5. 申请resp
        uintptr_t my_data_buf;
        uint32_t my_rkey;
        uint32_t my_size;
        // msgq::MsgBuffer wd_resp_raw;

        switch (req.type) {
            case GetPageCXLRefOrProxyRequest::WRITE: {
                // 5.1 如果是写操作,等待获取CN上的数据
                // using GetCurrentWriteDataRPC = RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData);
                // auto wd_req_raw = client_connection.msgq_rpc->alloc_msg_buffer(
                //     sizeof(GetCurrentWriteDataRPC::RequestType));
                // auto wd_req =
                //     reinterpret_cast<GetCurrentWriteDataRPC::RequestType*>(wd_req_raw.get_buf());
                // wd_req->mac_id = daemon_context.m_daemon_id;
                // wd_req->dio_write_buf = req.cn_write_buf;
                // wd_req->dio_write_size = req.cn_write_size;

                // SpinPromise<msgq::MsgBuffer> wd_pro;
                // SpinFuture<msgq::MsgBuffer> wd_fu = wd_pro.get_future();
                // client_connection.msgq_rpc->enqueue_request(GetCurrentWriteDataRPC::rpc_type,
                //                                             wd_req_raw,
                //                                             msgq_general_bool_flag_cb,
                //                                             static_cast<void*>(&wd_pro));

                // this_cort::ResetResumeCond(
                //     [&wd_fu]() { return wd_fu.wait_for(0s) != std::future_status::timeout; });
                // this_cort::yield();

                // msgq::MsgBuffer wd_resp_raw = wd_fu.get();
                // auto wd_resp =
                //     reinterpret_cast<GetCurrentWriteDataRPC::ResponseType*>(wd_resp_raw.get_buf());

                // ibv_mr* mr = daemon_context.GetMR(wd_resp->data);

                // my_data_buf = reinterpret_cast<uintptr_t>(wd_resp->data);
                // my_rkey = mr->rkey;
                // my_size = req.cn_write_size;

                // // 必须在msgq enqueue之后alloc resp，防止发送阻塞
                // reply_ptr = req.alloc_flex_resp(0);

                DLOG_FATAL("Not Support");
                break;
            }
            case GetPageCXLRefOrProxyRequest::READ: {
                // 5.2 如果是读操作，则动态申请读取resp buf
                resp_handle.Init(req.cn_read_size);
                auto& reply = resp_handle.Get();

                ibv_mr* mr = daemon_context.GetMR(reply.read_data);
                my_data_buf = reinterpret_cast<uintptr_t>(reply.read_data);
                my_rkey = mr->rkey;
                my_size = req.cn_read_size;
                break;
            }
            case GetPageCXLRefOrProxyRequest::WRITE_RAW: {
                // 5.3 如果是写操作,直接获取req上的写数据
                resp_handle.Init();

                ibv_mr* mr = daemon_context.GetMR(req.cn_write_raw_buf);
                my_data_buf = reinterpret_cast<uintptr_t>(req.cn_write_raw_buf);
                my_rkey = mr->rkey;
                my_size = req.cn_write_size;
                break;
            }
        }

        // 6. 调用dio读写远端内存
        {
            rdma_rc::RDMABatch ba;
            switch (req.type) {
                case GetPageCXLRefOrProxyRequest::READ:
                    dest_daemon_conn->rdma_conn->prep_read(
                        ba, my_data_buf, my_rkey, my_size,
                        (rem_page_md_cache->remote_page_addr + page_offset),
                        rem_page_md_cache->remote_page_rkey, false);
                    // DLOG("read size %u remote addr [%#lx, %u] to local addr [%#lx, %u]", my_size,
                    //      rem_page_md_cache->remote_page_addr,
                    //      rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE:
                    // dest_daemon_conn->rdma_conn->prep_write(
                    //     ba, my_data_buf, my_rkey, my_size,
                    //     (rem_page_md_cache->remote_page_addr + page_offset),
                    //     rem_page_md_cache->remote_page_rkey, false);
                    // client_connection.msgq_rpc->free_msg_buffer(wd_resp_raw);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE_RAW:
                    dest_daemon_conn->rdma_conn->prep_write(
                        ba, my_data_buf, my_rkey, my_size,
                        (rem_page_md_cache->remote_page_addr + page_offset),
                        rem_page_md_cache->remote_page_rkey, false);
                    // DLOG("write size %u remote addr [%#lx, %u] from local addr [%#lx, %u]",
                    // my_size,
                    //      rem_page_md_cache->remote_page_addr,
                    //      rem_page_md_cache->remote_page_rkey, my_data_buf, my_rkey);
                    break;
            }
            auto fu = dest_daemon_conn->rdma_conn->submit(ba);

            while (fu.try_get() != 0) {
                boost::this_fiber::yield();
            }
        }

        auto &reply = resp_handle.Get();
        reply.refs = false;
        return;
    }

    // page swap
    {
        // 给page ref取消读锁
        page_ref_lock.unlock();

        std::unique_lock<CortSharedMutex> page_ref_lock(*ref_shmut);

        // 双if判断
        if (daemon_context.m_hot_stats.find(page_id) != page_hot_iter) {
            goto retry;
        }

        daemon_context.m_stats.page_swap++;

        // 1 为page swap的区域准备内存，并确定是否需要换出页
        dest_daemon_conn = rem_page_md_cache->remote_page_daemon_conn;

        bool isSwap;
        page_id_t swap_page_id = invalid_page_id;
        uintptr_t swapin_addr, swapout_addr = 0;
        uint32_t swapin_key, swapout_key = 0;
        CortSharedMutex* ref_swapout_shmut;
        ibv_mr* swapout_mr;
        PageMetadata* swap_page_metadata;

        // 交换的情况，需要将自己的一个page交换到对方, 这个读写过程由对方完成

        // 首先为即将迁移到本地的page申请内存
        std::vector<PageMetadata*> pageMetaVec;
        allocPageMemoryTmp(daemon_context, page_id, 1, pageMetaVec);
        page_metadata = pageMetaVec.front();
        // DLOG("allocPageMemoryTmp: page_metadata->offset = %#lx",
        // page_metadata->cxl_memory_offset);

        // 本地不够，淘汰page
        if (daemon_context.m_max_data_page_num == daemon_context.m_current_used_page_num) {
            // 遍历page table中未被client引用的page作为swap page
            thread_local std::mt19937 eng(rand());
            daemon_context.m_page_table.random_foreach_all(
                eng, [&](std::pair<const page_id_t, PageMetadata*>& p) {
                    if (p.second->ref_client.empty() && p.second->status.exchange(1) == 0) {
                        swap_page_id = p.first;
                        return false;
                    }
                    return true;
                });

            // 所有的page都被client引用，则向client获取最久远的page作为swap page
            if (swap_page_id == invalid_page_id) {
                uint64_t oldest_time = UINT64_MAX;
                std::priority_queue<std::pair<uint64_t, page_id_t>,
                                    std::vector<std::pair<uint64_t, page_id_t>>,
                                    std::greater<std::pair<uint64_t, page_id_t>>>
                    oldest_heap;

                for (size_t i = 0; i < daemon_context.m_client_connect_table.size(); i++) {
                    DaemonToClientConnection* client_conn =
                        daemon_context.m_client_connect_table[i];
                    using GetPagePastAccessFreqRPC =
                        RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq);
                    auto wd_req_raw = client_conn->msgq_rpc->alloc_msg_buffer(
                        sizeof(GetPagePastAccessFreqRPC::RequestType));
                    auto wd_req = reinterpret_cast<GetPagePastAccessFreqRPC::RequestType*>(
                        wd_req_raw.get_buf());
                    wd_req->mac_id = daemon_context.m_daemon_id;

                    SpinPromise<msgq::MsgBuffer> pro;
                    SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
                    client_conn->msgq_rpc->enqueue_request(GetPagePastAccessFreqRPC::rpc_type,
                                                           wd_req_raw, msgq_general_bool_flag_cb,
                                                           static_cast<void*>(&pro));
                    this_cort::ResetResumeCond(
                        [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
                    this_cort::yield();

                    msgq::MsgBuffer wd_resp_raw = fu.get();
                    auto wd_resp = reinterpret_cast<GetPagePastAccessFreqRPC::ResponseType*>(
                        wd_resp_raw.get_buf());

                    page_id_t oldest_page_id = wd_resp->oldest_page_id;
                    uint64_t last_access_ts = wd_resp->last_access_ts;

                    client_conn->msgq_rpc->free_msg_buffer(wd_resp_raw);

                    oldest_heap.push({last_access_ts, oldest_page_id});
                }

                while (!oldest_heap.empty()) {
                    auto oldest_page_pair = oldest_heap.top();
                    oldest_heap.pop();
                    auto p_swap_page_meta =
                        daemon_context.m_page_table.find(oldest_page_pair.second);
                    if (p_swap_page_meta != daemon_context.m_page_table.end() &&
                        p_swap_page_meta->second->status.exchange(1) == 0) {
                        swap_page_id = oldest_page_pair.second;
                        break;
                    }
                }
            }
            if (swap_page_id == invalid_page_id) {
                daemon_context.m_page_table.random_foreach_all(
                    eng, [&](std::pair<const page_id_t, PageMetadata*>& random_page_pair) {
                        if (random_page_pair.second->status.exchange(1) == 0) {
                            swap_page_id = random_page_pair.first;
                            return false;
                        }
                        return true;
                    });
            }

            DLOG_ASSERT(swap_page_id != invalid_page_id);
            // DLOG("swap = %ld", swap_page_id);
            /* 1.2 注册换出页的地址，并获取rkey */
            auto swap_page_it = daemon_context.m_page_table.find(swap_page_id);
            DLOG_ASSERT(swap_page_it != daemon_context.m_page_table.end(),
                        "Can't find swap page %lu", swap_page_id);

            swap_page_metadata = swap_page_it->second;
            swapout_addr =
                reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
                swap_page_metadata->cxl_memory_offset;
            swapout_mr = daemon_context.GetMR(reinterpret_cast<void*>(swapout_addr));
            swapout_key = swapout_mr->rkey;

            // 给即将换出页的page_meta上写锁
            auto p_lock_swapout = daemon_context.m_page_ref_lock.find_or_emplace(
                swap_page_id, []() { return new SharedCortMutex(); });
            ref_swapout_shmut = p_lock_swapout.first->second;

            ref_swapout_shmut->lock();
        }

        swapin_addr =
            reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
            page_metadata->cxl_memory_offset;
        swapout_mr = daemon_context.GetMR(reinterpret_cast<void*>(swapin_addr));
        swapin_key = swapout_mr->rkey;

        // DLOG(
        //     "DN %u: Expect inPage %lu (from DN: %u) outPage %lu. swapin_addr = %ld, swapin_key ="
        //     "%d",
        //     daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id, swap_page_id,
        //     swapin_addr, swapin_key);

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
        if (swap_page_id != invalid_page_id) {
            // DLOG("swap delPageRefAndCacheBroadcast");
            delPageRefAndCacheBroadcast(daemon_context, swap_page_id, swap_page_metadata);
        }
        // 2.1.2 等待latch完成
        this_cort::ResetResumeCond(
            [&latch_fu]() { return latch_fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        rpc.free_msg_buffer(latch_req_raw);
        rpc.free_msg_buffer(latch_resp_raw);

        DLOG_ASSERT(page_hot_iter != daemon_context.m_hot_stats.end(), "Can't find page %lu's ref",
                    page_id);
        // 清除该迁入page的ref
        daemon_context.m_hot_stats.erase(page_hot_iter);

        /* 3. 向daemon发送page迁移（tryMigratePage），等待其完成迁移，返回RPC */
        using TryMigratePageRPC = RPC_TYPE_STRUCT(rpc_daemon::tryMigratePage);

        auto migrate_req_raw = rpc.alloc_msg_buffer_or_die(sizeof(TryMigratePageRPC::RequestType));
        auto migrate_resp_raw =
            rpc.alloc_msg_buffer_or_die(sizeof(TryMigratePageRPC::ResponseType));

        auto migrate_req =
            reinterpret_cast<TryMigratePageRPC::RequestType*>(migrate_req_raw.get_buf());
        migrate_req->mac_id = daemon_context.m_daemon_id;
        migrate_req->hot_score = 0x2342345;
        migrate_req->page_id = page_id;  // 期望迁移的page
        migrate_req->swap_page_id = swap_page_id;
        migrate_req->swapin_page_addr = swapin_addr;
        migrate_req->swapin_page_rkey = swapin_key;
        migrate_req->swapout_page_addr = swapout_addr;
        migrate_req->swapout_page_rkey = swapout_key;

        std::promise<void> migrate_pro;
        std::future<void> migrate_fu = migrate_pro.get_future();
        rpc.enqueue_request(dest_daemon_conn->peer_session, TryMigratePageRPC::rpc_type,
                            migrate_req_raw, migrate_resp_raw, erpc_general_promise_flag_cb,
                            static_cast<void*>(&migrate_pro));

        this_cort::ResetResumeCond(
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
            // 回收迁移走的页面
            daemon_context.m_cxl_page_allocator->deallocate(swap_page_metadata->cxl_memory_offset);
            daemon_context.m_current_used_page_num--;
            // 清除迁移走的page位于当前DN上的元数据
            daemon_context.m_page_table.erase(swap_page_id);
            // 换出页已迁移完毕，解锁
            ref_swapout_shmut->unlock();
        } else {
            // TODO: 拒绝swap
        }

        // 换近页已迁移完毕，解锁
        ref_shmut->unlock();

        /* 4. 向mn发送unLatchPageAndBalance，更改page dir，返回RPC*/
        // DLOG("DN %u: unLatchPageAndBalance!", daemon_context.m_daemon_id);
        using unLatchPageAndBalanceRPC = RPC_TYPE_STRUCT(rpc_master::unLatchPageAndSwap);
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

        this_cort::ResetResumeCond(
            [&unlatchB_fu]() { return unlatchB_fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        rpc.free_msg_buffer(unlatchB_req_raw);
        rpc.free_msg_buffer(unlatchB_resp_raw);

        // DLOG("DN %u: Expect inPage %lu (from DN: %u) swap page finished!",
        //      daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id);
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

        daemon_context.m_current_used_page_num++;
        daemon_context.m_page_table.insert(req.start_page_id + c, page_metadata);

        // DLOG("new page %ld ---> %#lx", req.start_page_id + c, cxl_memory_offset);
    }
    reply.ret = true;
    return reply;
}

void allocPageMemoryTmp(DaemonContext& daemon_context, page_id_t start_page_id, size_t count,
                        std::vector<PageMetadata*>& pageMetaVec) {
    DLOG_ASSERT(daemon_context.m_current_used_page_num + count <= daemon_context.m_total_page_num,
                "Can't allocate more page memory");
    AllocPageMemoryReply reply;
    for (size_t c = 0; c < count; ++c) {
        offset_t cxl_memory_offset = daemon_context.m_cxl_page_allocator->allocate(1);
        DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

        PageMetadata* page_metadata = new PageMetadata();
        page_metadata->cxl_memory_offset = cxl_memory_offset;

        pageMetaVec.push_back(page_metadata);

        // DLOG("new page %ld ---> %#lx", start_page_id + c, cxl_memory_offset);
    }
    return;
}

AllocPageReply allocPage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                         AllocPageRequest& req) {
    DLOG("alloc %lu new pages", req.count);

    // 向Master调用allocPage
    auto& rpc = daemon_context.GetErpc();

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
    this_cort::ResetResumeCond([&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
    this_cort::yield();

    auto resp = reinterpret_cast<PageAllocRPC::ResponseType*>(resp_raw.get_buf());

    page_id_t start_page_id = resp->start_page_id;

    std::vector<PageMetadata*> pageMetaVec;
    allocPageMemoryTmp(daemon_context, start_page_id, resp->start_count, pageMetaVec);

    daemon_context.m_current_used_page_num += resp->start_count;
    for (size_t c = 0; c < resp->start_count; ++c) {
        PageMetadata* page_meta = pageMetaVec[c];
        // page_meta->ref_client.insert(&client_connection);
        daemon_context.m_page_table.insert(start_page_id + c, page_meta);
        SharedCortMutex* ref_lock = new SharedCortMutex();
        daemon_context.m_page_ref_lock.insert(start_page_id + c, ref_lock);
        // DLOG("allocPage: insert page %lu, offset = %ld ", start_page_id + c,
        //      pageMetaVec[c]->cxl_memory_offset);
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
    ibv_mr* mr = daemon_context.GetMR(reinterpret_cast<void*>(local_addr));

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

    // DLOG("get page %lu rdma ref [%#lx, %u], local [%#lx, %u],  peer_session = %d, daemon_id =
    // %u",
    //      req.page_id, local_addr, mr->rkey, local_addr, mr->lkey, daemon_connection.peer_session,
    //      daemon_connection.daemon_id);

    GetPageRDMARefReply reply;
    reply.addr = local_addr;
    reply.rkey = mr->rkey;
    return reply;
}

DelPageRDMARefReply delPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   DelPageRDMARefRequest& req) {
    SharedCortMutex* ref_lock;
    auto it_lock = daemon_context.m_page_ref_lock.find(req.page_id);
    DLOG_ASSERT(it_lock != daemon_context.m_page_ref_lock.end(), "Can't find page %lu's ref lock",
                req.page_id);
    ref_lock = it_lock->second;

    // DLOG("DN %u: delPageRDMARef page %lu lock", daemon_context.m_daemon_id, req.page_id);
    // 给page ref加写锁
    ref_lock->lock();

    auto it = daemon_context.m_hot_stats.find(req.page_id);
    DLOG_ASSERT(it != daemon_context.m_hot_stats.end(), "Can't find page %lu's ref", req.page_id);

    // 清除该page的ref
    daemon_context.m_hot_stats.erase(req.page_id);
    // DLOG("DN %u: Del page %ld rdma ref", daemon_context.m_daemon_id, req.page_id);

    ref_lock->unlock();
    // DLOG("DN %u: delPageRDMARef page %lu unlock", daemon_context.m_daemon_id, req.page_id);
    DelPageRDMARefReply reply;
    reply.isDel = true;
    return reply;
}

/**
 * @brief 删除页ref和cache
 *
 * @param daemon_context
 * @param page_id
 * @param page_meta
 * @param unless_daemon 对于page
 * swap，迁入page已经在请求daemon端删除了ref，因此不需要再次发起`delPageRDMARef`请求
 */
void delPageRefAndCacheBroadcast(DaemonContext& daemon_context, page_id_t page_id,
                                 PageMetadata* page_meta, mac_id_t unless_daemon) {
    auto& rpc = daemon_context.GetErpc();
    // DLOG("DN %u: delPageRefBroadcast page %lu", daemon_context.m_daemon_id, page_id);
    // del page ref
    using DelPageRDMARefRPC = RPC_TYPE_STRUCT(rpc_daemon::delPageRDMARef);
    std::vector<std::promise<void>> del_ref_pro_vec(page_meta->ref_daemon.size());
    std::vector<erpc::MsgBufferWrap> del_ref_req_raw_vec;
    std::vector<erpc::MsgBufferWrap> del_ref_resp_raw_vec;
    // del page cache
    std::vector<SpinPromise<msgq::MsgBuffer>> remove_cache_pro_vec(page_meta->ref_client.size());

    size_t i = 0;
    for (auto daemon_conn : page_meta->ref_daemon) {
        if (daemon_conn->daemon_id == unless_daemon) {
            continue;
        }
        // DLOG("DN %u: delPageRefBroadcast for i = %ld, peer_session = %d, daemon_id = %u",
        //      daemon_context.m_daemon_id, i, daemon_conn->peer_session, daemon_conn->daemon_id);

        auto del_ref_req_raw = rpc.alloc_msg_buffer_or_die(sizeof(DelPageRDMARefRPC::RequestType));
        auto del_ref_resp_raw =
            rpc.alloc_msg_buffer_or_die(sizeof(DelPageRDMARefRPC::ResponseType));

        auto del_ref_req =
            reinterpret_cast<DelPageRDMARefRPC::RequestType*>(del_ref_req_raw.get_buf());
        del_ref_req->mac_id = daemon_context.m_daemon_id;
        del_ref_req->page_id = page_id;  // 准备删除ref的page id

        rpc.enqueue_request(daemon_conn->peer_session, DelPageRDMARefRPC::rpc_type, del_ref_req_raw,
                            del_ref_resp_raw, erpc_general_promise_flag_cb,
                            static_cast<void*>(&del_ref_pro_vec[i]));

        del_ref_req_raw_vec.push_back(del_ref_req_raw);
        del_ref_resp_raw_vec.push_back(del_ref_resp_raw);
        i++;
    }
    del_ref_pro_vec.resize(i);

    i = 0;
    using RemovePageCacheRPC = RPC_TYPE_STRUCT(rpc_client::removePageCache);
    for (auto client_conn : page_meta->ref_client) {
        // DLOG("DN %u: delPageCacheBroadcast client_id = %u", daemon_context.m_daemon_id,
        //      client_conn->client_id);

        auto remove_cache_req_raw =
            client_conn->msgq_rpc->alloc_msg_buffer(sizeof(RemovePageCacheRPC::RequestType));
        auto remove_cache_req =
            reinterpret_cast<RemovePageCacheRPC::RequestType*>(remove_cache_req_raw.get_buf());
        remove_cache_req->mac_id = daemon_context.m_daemon_id;
        remove_cache_req->page_id = page_id;

        client_conn->msgq_rpc->enqueue_request(RemovePageCacheRPC::rpc_type, remove_cache_req_raw,
                                               msgq_general_bool_flag_cb,
                                               static_cast<void*>(&remove_cache_pro_vec[i]));
        i++;
    }

    for (size_t i = 0; i < del_ref_pro_vec.size(); i++) {
        auto fu = del_ref_pro_vec[i].get_future();
        this_cort::ResetResumeCond(
            [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        rpc.free_msg_buffer(del_ref_req_raw_vec[i]);
        rpc.free_msg_buffer(del_ref_resp_raw_vec[i]);
    }
    // DLOG("Finish delPageRefBroadcast");
    i = 0;
    for (auto client_conn : page_meta->ref_client) {
        // DLOG("Get cache del client_conn get_future for i = %ld", i);
        auto fu = remove_cache_pro_vec[i].get_future();
        this_cort::ResetResumeCond(
            [&fu]() { return fu.wait_for(0s) != std::future_status::timeout; });
        this_cort::yield();

        msgq::MsgBuffer remove_cache_resp_raw = fu.get();
        client_conn->msgq_rpc->free_msg_buffer(remove_cache_resp_raw);
        i++;
    }
    // DLOG("Finish delPageCacheBroadcast");
}

TryMigratePageReply tryMigratePage(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   TryMigratePageRequest& req) {
    // 获取预交换的page的本地元数据
    SharedCortMutex* ref_lock;
    auto ref_lock_pair = daemon_context.m_page_ref_lock.find_or_emplace(
        req.page_id, []() { return new SharedCortMutex(); });
    DLOG_ASSERT(ref_lock_pair.first != daemon_context.m_page_ref_lock.end(),
                "Can't find page %lu's ref lock", req.page_id);
    ref_lock = ref_lock_pair.first->second;

    daemon_context.m_stats.page_swap++;

    ref_lock->lock();

    PageMetadata* page_meta;
    auto page_meta_it = daemon_context.m_page_table.find(req.page_id);
    DLOG_ASSERT(page_meta_it != daemon_context.m_page_table.end(), "Can't find page %lu",
                req.page_id);
    page_meta = page_meta_it->second;
    // DLOG("DN: %u recv tryMigratePage for page %lu. swap page = %lu", daemon_context.m_daemon_id,
    //      req.page_id, req.swap_page_id);

    // TODO: hot score 拒绝

    // 广播有当前page的ref的DN，删除其ref, 并通知当前rack下所有访问过该page的client删除相应的缓存
    // DLOG("DN %u: delPageRefBroadcast page %lu", daemon_context.m_daemon_id, req.page_id);
    delPageRefAndCacheBroadcast(daemon_context, req.page_id, page_meta,
                                daemon_connection.daemon_id);

    // 使用RDMA单边读写将page上的内容进行交换
    // DLOG("DN %u: rdma write. swapin_addr = %ld, swapin_key = %d", daemon_context.m_daemon_id,
    //      req.swapin_page_addr, req.swapin_page_rkey);
    uintptr_t local_addr =
        reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
        page_meta->cxl_memory_offset;
    ibv_mr* mr = daemon_context.GetMR(reinterpret_cast<void*>(local_addr));
    uint32_t lkey = mr->lkey;

    DaemonConnection* daemon_conn_temp;
    auto p_daemon_conn = daemon_context.m_connect_table.find(req.mac_id);
    daemon_conn_temp = p_daemon_conn->second;
    DaemonToDaemonConnection* daemon_conn =
        dynamic_cast<DaemonToDaemonConnection*>(daemon_conn_temp);

    rdma_rc::RDMABatch ba;
    daemon_conn->rdma_conn->prep_write(ba, local_addr, lkey, page_size, req.swapin_page_addr,
                                       req.swapin_page_rkey, false);

    // DLOG("rdma write mid. swapout_page_addr = %lu", req.swapout_page_addr);
    bool isSwap;
    PageMetadata* local_page_meta;
    std::vector<PageMetadata*> pageMetaVec;
    if (req.swapout_page_addr == 0 && req.swapout_page_rkey == 0) {
        isSwap = false;
    } else {
        isSwap = true;
        // 交换的情况，需要读对方的page到本地
        allocPageMemoryTmp(daemon_context, req.swap_page_id, 1, pageMetaVec);

        local_page_meta = pageMetaVec.front();

        uintptr_t swapin_addr =
            reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.page_data_start_addr) +
            local_page_meta->cxl_memory_offset;
        mr = daemon_context.GetMR(reinterpret_cast<void*>(swapin_addr));
        lkey = mr->lkey;
        daemon_conn->rdma_conn->prep_read(ba, swapin_addr, lkey, page_size, req.swapout_page_addr,
                                          req.swapout_page_rkey, false);
        // 如果有读，也在后面和前面的写一块submit
    }
    auto fu = daemon_conn->rdma_conn->submit(ba);
    // DLOG(
    //     "DN %u: rdma write submit. local_addr = %ld, lkey = %u, req.swapin_page_addr = %ld,  "
    //     "req.swapin_page_rkey = %u",
    //     daemon_context.m_daemon_id, local_addr, lkey, req.swapin_page_addr,
    //     req.swapin_page_rkey);
    this_cort::ResetResumeCond([&fu]() { return fu.try_get() == 0; });
    this_cort::yield();

    // DLOG("DN %u: reply", daemon_context.m_daemon_id);

    // 回收迁移走的页面
    daemon_context.m_cxl_page_allocator->deallocate(page_meta->cxl_memory_offset);
    daemon_context.m_current_used_page_num--;
    // 清除即将迁移page位于当前DN上的元数据
    daemon_context.m_page_table.erase(req.page_id);

    if (isSwap) {
        // 若page没有剩余，迁移到了swap区，现在再迁移到page区域
        DLOG_ASSERT(daemon_context.m_max_data_page_num > daemon_context.m_current_used_page_num,
                    "Page is full, can't swap in.");
        daemon_context.m_current_used_page_num++;
        daemon_context.m_page_table.insert(req.swap_page_id, pageMetaVec.front());

        // daemon_context.m_page_table.insert(req.swap_page_id, local_page_meta);
        // daemon_context.m_current_used_page_num++;
        // daemon_context.m_swap_page_table.erase(req.swap_page_id);
        // daemon_context.m_current_used_swap_page_num--;
    }
    ref_lock->unlock();

    TryMigratePageReply reply;
    reply.swaped = isSwap;
    // DLOG("DN %u: finished migrate!", daemon_context.m_daemon_id);
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
