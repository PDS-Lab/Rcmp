#include "proto/rpc_daemon.hpp"

#include "promise.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_client.hpp"
#include "proto/rpc_master.hpp"
#include "proto/rpc_register.hpp"
#include "udp_client.hpp"

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
    client_connection.msgq_conn = std::make_unique<MsgQClient>(
        msgq::MsgQueueRPC{daemon_context.m_msgq_manager.nexus.get(), q,
                          daemon_context.m_msgq_manager.nexus->GetPublicMsgQ(), &daemon_context});

    // 3. 通过UDP通知client创建msgq
    msgq::MsgUDPConnPacket pkt;
    pkt.recv_q_off = reinterpret_cast<uintptr_t>(client_connection.msgq_conn->rpc.m_send_queue) -
                     reinterpret_cast<uintptr_t>(daemon_context.m_cxl_format.msgq_zone_start_addr);
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
    daemon_connection.erpc_conn = std::make_unique<ErpcClient>(
        daemon_context.GetErpc(), daemon_connection.ip, daemon_connection.port);

    DLOG("Connect with daemon [rack:%d --- id:%d], port = %d", daemon_connection.rack_id,
         daemon_connection.daemon_id, daemon_connection.port);

    auto local_addr = daemon_context.m_listen_conn.get_local_addr();

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.daemon_mac_id = daemon_context.m_daemon_id;
    reply.rdma_port = local_addr.second;
}

void getPageCXLRefOrProxy(DaemonContext& daemon_context,
                          DaemonToClientConnection& client_connection,
                          GetPageCXLRefOrProxyRequest& req,
                          ResponseHandle<GetPageCXLRefOrProxyReply>& resp_handle) {
    PageMetadata* page_meta;
    page_id_t page_id = GetPageID(req.gaddr);
    offset_t page_offset = GetPageOffset(req.gaddr);

retry:
    page_meta = daemon_context.m_page_table.FindOrCreatePageMeta(page_id);
    std::shared_lock<CortSharedMutex> page_ref_lock(page_meta->page_ref_lock);

    /* 1. 本地获取访问page */
    PageVMMapMetadata* page_vm_meta = page_meta->vm_meta;
    if (page_vm_meta != nullptr) {
        daemon_context.m_stats.page_hit++;

        // DLOG("insert ref_client for page %lu", page_id);
        page_vm_meta->ref_client.insert(&client_connection);

        resp_handle.Init();
        auto& reply = resp_handle.Get();
        reply.refs = true;
        reply.offset = page_vm_meta->cxl_memory_offset;

        return;
    }

    /*
     * ---------------------------------------------
     *                   PAGE MISS
     * ---------------------------------------------
     */
    daemon_context.m_stats.page_miss++;

    DaemonToDaemonConnection* dest_daemon_conn;
    RemotePageRefMeta* remote_page_ref_meta =
        daemon_context.m_page_table.FindOrCreateRemotePageRefMeta(
            page_meta, [&](RemotePageRefMeta* remote_page_ref_meta) {
                // 如果是第一次访问该page，走DirectIO流程（remote_page_ref_meta不存在时，说明一定是第一次访问）

                /* 2. 获取mn上page的daemon，并锁定该page */
                {
                    auto latch_fu =
                        daemon_context.m_conn_manager.GetMasterConnection()
                            .erpc_conn->call<CortPromise>(rpc_master::latchRemotePage,
                                                          {
                                                              .mac_id = daemon_context.m_daemon_id,
                                                              .isWriteLock = false,
                                                              .page_id = page_id,
                                                              .page_id_swap = invalid_page_id,
                                                          });

                    auto& latch_resp = latch_fu.get();

                    // 获取对端连接
                    dest_daemon_conn = dynamic_cast<DaemonToDaemonConnection*>(
                        daemon_context.m_conn_manager.GetConnection(latch_resp.dest_daemon_id));
                }

                /* 3. 获取远端内存rdma ref */
                {
                    auto rref_fu = dest_daemon_conn->erpc_conn->call<CortPromise>(
                        rpc_daemon::getPageRDMARef, {
                                                        .mac_id = daemon_context.m_daemon_id,
                                                        .page_id = page_id,
                                                    });

                    auto& rref_resp = rref_fu.get();
                    remote_page_ref_meta->remote_page_addr = rref_resp.addr;
                    remote_page_ref_meta->remote_page_rkey = rref_resp.rkey;
                    remote_page_ref_meta->remote_page_daemon_conn = dest_daemon_conn;
                }

                /* 4. unlatch */
                {
                    auto unlatch_fu =
                        daemon_context.m_conn_manager.GetMasterConnection()
                            .erpc_conn->call<CortPromise>(rpc_master::unLatchRemotePage,
                                                          {
                                                              .mac_id = daemon_context.m_daemon_id,
                                                              .page_id = page_id,
                                                          });

                    auto& resp = unlatch_fu.get();
                }
            });

    size_t remote_page_current_hot = remote_page_ref_meta->Update();
    // 只有刚好等于水位线时，才进行迁移
    if (remote_page_current_hot != daemon_context.m_options.hot_swap_watermark)
    /*
     * ---------------------------------------------
     *                PAGE DIRECT IO
     * ---------------------------------------------
     */
    {
        daemon_context.m_stats.page_dio++;

        // 启动DirectIO流程
        dest_daemon_conn = remote_page_ref_meta->remote_page_daemon_conn;

        // printf("freq = %ld, rkey = %d, addr = %ld\n", current_hot,
        //    remote_page_ref_meta->remote_page_rkey, remote_page_ref_meta->remote_page_addr);

        /* 5. 申请resp */
        uintptr_t my_data_buf;
        uint32_t my_lkey;
        uint32_t my_size;

        switch (req.type) {
            case GetPageCXLRefOrProxyRequest::WRITE: {
                /* 5.1 如果是写操作,等待获取CN上的数据 */
                auto wd_fu = client_connection.msgq_conn->call<CortPromise>(
                    rpc_client::getCurrentWriteData,
                    {
                        .mac_id = daemon_context.m_daemon_id,
                        .dio_write_buf = req.u.write.cn_write_buf,
                        .dio_write_size = req.u.write.cn_write_size,
                    });

                auto& wd_resp = wd_fu.get();

                resp_handle.Init();

                ibv_mr* mr = daemon_context.GetMR(wd_resp.data);
                my_data_buf = reinterpret_cast<uintptr_t>(wd_resp.data);
                my_lkey = mr->lkey;
                my_size = req.u.write.cn_write_size;
                break;
            }
            case GetPageCXLRefOrProxyRequest::READ: {
                /* 5.2 如果是读操作，则动态申请读取resp buf */
                resp_handle.Init(req.u.read.cn_read_size);
                auto& reply = resp_handle.Get();

                ibv_mr* mr = daemon_context.GetMR(reply.read_data);
                my_data_buf = reinterpret_cast<uintptr_t>(reply.read_data);
                my_lkey = mr->lkey;
                my_size = req.u.read.cn_read_size;
                break;
            }
            case GetPageCXLRefOrProxyRequest::WRITE_RAW: {
                /* 5.3 如果是写操作,直接获取req上的写数据 */
                resp_handle.Init();

                ibv_mr* mr = daemon_context.GetMR(req.u.write_raw.cn_write_raw_buf);
                my_data_buf = reinterpret_cast<uintptr_t>(req.u.write_raw.cn_write_raw_buf);
                my_lkey = mr->lkey;
                my_size = req.u.write_raw.cn_write_raw_size;
                break;
            }
        }

        /* 6. 调用dio读写远端内存 */
        {
            rdma_rc::SgeWr sge_wr;
            switch (req.type) {
                case GetPageCXLRefOrProxyRequest::READ:
                    dest_daemon_conn->rdma_conn->prep_read(
                        &sge_wr, my_data_buf, my_lkey, my_size,
                        (remote_page_ref_meta->remote_page_addr + page_offset),
                        remote_page_ref_meta->remote_page_rkey, false);
                    // DLOG("read size %u remote addr [%#lx, %u] to local addr [%#lx, %u]", my_size,
                    //      remote_page_ref_meta->remote_page_addr,
                    //      remote_page_ref_meta->remote_page_rkey, my_data_buf, my_lkey);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE:
                    // dest_daemon_conn->rdma_conn->prep_write(
                    //     &sge_wr, my_data_buf, my_lkey, my_size,
                    //     (remote_page_ref_meta->remote_page_addr + page_offset),
                    //     remote_page_ref_meta->remote_page_rkey, false);
                    // client_connection.msgq_rpc->free_msg_buffer(wd_resp_raw);
                    break;
                case GetPageCXLRefOrProxyRequest::WRITE_RAW:
                    dest_daemon_conn->rdma_conn->prep_write(
                        &sge_wr, my_data_buf, my_lkey, my_size,
                        (remote_page_ref_meta->remote_page_addr + page_offset),
                        remote_page_ref_meta->remote_page_rkey, false);
                    // DLOG("write size %u remote addr [%#lx, %u] from local addr [%#lx, %u]",
                    // my_size,
                    //      remote_page_ref_meta->remote_page_addr + page_offset,
                    //      remote_page_ref_meta->remote_page_rkey, my_data_buf, my_lkey);
                    break;
            }
            auto fu = dest_daemon_conn->rdma_conn->submit(&sge_wr, 1);

            fu.get();
        }

        auto& reply = resp_handle.Get();
        reply.refs = false;
        return;
    }

    /*
     * ---------------------------------------------
     *                   PAGE SWAP
     * ---------------------------------------------
     */
    {
        // 给page ref取消读锁
        page_ref_lock.unlock();

        PageMetadata* swapin_page_meta = page_meta;
        std::unique_lock<CortSharedMutex> swapin_page_ref_lock(swapin_page_meta->page_ref_lock);

        // 判断remote ref是否失效（ABA）
        if (remote_page_ref_meta != swapin_page_meta->remote_ref_meta) {
            goto retry;
        }

        daemon_context.m_stats.page_swap++;

        /* 1. 为page swap的区域准备内存，并确定是否需要换出页 */
        dest_daemon_conn = remote_page_ref_meta->remote_page_daemon_conn;

        // 交换的情况，需要将自己的一个page交换到对方, 这个读写过程由对方完成
        bool is_swap = false;    // request return
        bool need_swap = false;  // swapout_page_id != invalid_page_id
        page_id_t swapout_page_id = invalid_page_id;
        uintptr_t swapin_addr, swapout_addr = 0;
        uint32_t swapin_key, swapout_key = 0;
        ibv_mr* swapin_mr;
        ibv_mr* swapout_mr;
        PageMetadata* swapout_page_meta;
        PageVMMapMetadata* reserve_page_vm_meta;
        std::unique_lock<CortSharedMutex> swapout_page_ref_lock;

        // 首先为即将迁移到本地的page申请内存
        while (!daemon_context.m_page_table.TestAllocPageMemory(1)) {
            // 当前swap区已满，等待完成
            boost::this_fiber::yield();
        }

        reserve_page_vm_meta = daemon_context.m_page_table.AllocPageMemory();
        // DLOG("reserve_page_vm_meta->offset = %#lx", reserve_page_vm_meta->cxl_memory_offset);

        // 本地不够，淘汰page
        if (daemon_context.m_page_table.NearlyFull()) {
            // 随机选择一个没有被本机柜内的client访问过的page进行交换
            // 例如：交换接收方的迁入页面，申请但未使用的页面
            daemon_context.m_page_table.RandomPickUnvisitVMPage(false, need_swap, swapout_page_id,
                                                                swapout_page_meta);

            // 所有的page都被client引用，则向client获取最久远的page作为swap page
            if (!need_swap) {
                uint64_t oldest_time = UINT64_MAX;
                MinHeap<std::pair<uint64_t, page_id_t>> oldest_heap;

                // 向所有client发起获取最旧page的请求
                {
                    std::vector<decltype(((MsgQClient*)0)
                                             ->call<CortPromise>(rpc_client::getPagePastAccessFreq,
                                                                 {}))>
                        fu_vec;

                    for (auto& client_conn : daemon_context.m_conn_manager.m_client_connect_table) {
                        auto fu = client_conn->msgq_conn->call<CortPromise>(
                            rpc_client::getPagePastAccessFreq,
                            {
                                .mac_id = daemon_context.m_daemon_id,
                            });

                        fu_vec.push_back(std::move(fu));
                    }

                    for (auto& fu : fu_vec) {
                        auto& wd_resp = fu.get();

                        if (wd_resp.oldest_page_id != invalid_page_id) {
                            oldest_heap.push({wd_resp.last_access_ts, wd_resp.oldest_page_id});
                        }
                    }
                }

                while (!oldest_heap.empty()) {
                    auto oldest_page_pair = oldest_heap.top();
                    oldest_heap.pop();
                    PageMetadata* swap_page_meta_tmp =
                        daemon_context.m_page_table.FindOrCreatePageMeta(oldest_page_pair.second);
                    if (swap_page_meta_tmp != nullptr && swap_page_meta_tmp->vm_meta != nullptr &&
                        swap_page_meta_tmp->vm_meta->TryPin()) {
                        swapout_page_id = oldest_page_pair.second;
                        swapout_page_meta = swap_page_meta_tmp;
                        need_swap = true;
                        break;
                    }
                }
            }

            // 如果client获取最久远的page正在被Pin，则随机选择一个未Pin的page
            if (!need_swap) {
                daemon_context.m_page_table.RandomPickUnvisitVMPage(
                    true, need_swap, swapout_page_id, swapout_page_meta);
            }

            DLOG_ASSERT(need_swap);
            DLOG_ASSERT(swapout_page_id != invalid_page_id);

            /* 1.2 注册换出页的地址，并获取rkey */
            swapout_addr =
                daemon_context.GetVirtualAddr(swapout_page_meta->vm_meta->cxl_memory_offset);
            swapout_mr = daemon_context.GetMR(reinterpret_cast<void*>(swapout_addr));
            swapout_key = swapout_mr->rkey;

            // TODO: swap page与page的死锁解决？

            // 给即将换出页的page_meta上写锁
            swapout_page_ref_lock =
                std::unique_lock<CortSharedMutex>(swapout_page_meta->page_ref_lock);
        }

        swapin_addr = daemon_context.GetVirtualAddr(reserve_page_vm_meta->cxl_memory_offset);
        swapin_mr = daemon_context.GetMR(reinterpret_cast<void*>(swapin_addr));
        swapin_key = swapin_mr->rkey;

        // DLOG(
        //     "DN %u: Expect inPage %lu (from DN: %u) outPage %lu. swapin_addr = %ld, swapin_key ="
        //     "%d",
        //     daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id, swapout_page_id,
        //     swapin_addr, swapin_key);

        /* 2. 向mn发送LatchPage(page_id)，获取mn上page的daemon，并锁定该page */
        {
            auto latch_fu =
                daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
                    rpc_master::latchRemotePage, {
                                                     .mac_id = daemon_context.m_daemon_id,
                                                     .isWriteLock = true,
                                                     .page_id = page_id,
                                                     .page_id_swap = swapout_page_id,
                                                 });

            /**
             * 2.1
             * 如果有需要换出的页，则广播有当前要swapout的page的ref的DN，删除其ref，并通知当前rack下所有访问过该page的client删除相应的缓存
             */
            if (need_swap) {
                // DLOG("swap delPageRefAndCacheBroadcast");
                delPageRefAndCacheBroadcast(daemon_context, swapout_page_id, swapout_page_meta);
            }

            /* 2.2 等待latch完成 */
            latch_fu.get();
        }

        DLOG_ASSERT(swapin_page_meta->remote_ref_meta != nullptr, "Can't find page %lu's ref",
                    page_id);
        // 清除该迁入page的ref
        daemon_context.m_page_table.EraseRemotePageRefMeta(swapin_page_meta);

        /* 3. 向daemon发送page迁移（tryMigratePage），等待其完成迁移，返回RPC */
        {
            auto migrate_fu = dest_daemon_conn->erpc_conn->call<CortPromise>(
                rpc_daemon::tryMigratePage, {
                                                .mac_id = daemon_context.m_daemon_id,
                                                .page_id = page_id,  // 期望迁移的page
                                                .swap_page_id = swapout_page_id,
                                                // TODO: hot score
                                                .hot_score = 0x2342345,
                                                .swapout_page_addr = swapout_addr,
                                                .swapin_page_addr = swapin_addr,
                                                .swapout_page_rkey = swapout_key,
                                                .swapin_page_rkey = swapin_key,
                                            });

            auto& migrate_resp = migrate_fu.get();

            is_swap = migrate_resp.swaped;
        }

        /* 4. 迁移完成，更新tlb */
        {
            // 尝试pin住page，防止立即被随机淘汰
            reserve_page_vm_meta->TryPin();
            daemon_context.m_page_table.ApplyPageMemory(swapin_page_meta, reserve_page_vm_meta);
            if (is_swap) {
                // 回收迁移走的页面
                daemon_context.m_page_table.CancelPageMemory(swapout_page_meta);
            } else {
                // TODO: 远端拒绝swap
            }

            // 换出页已迁移完毕，解锁
            if (need_swap) {
                swapout_page_ref_lock.unlock();
            }

            // 换近页已迁移完毕，解锁
            swapin_page_ref_lock.unlock();
            reserve_page_vm_meta->UnPin();
        }

        /* 5. 向mn发送unLatchPageAndSwap，更改page dir，返回RPC */
        {
            auto unlatch_fu =
                daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
                    rpc_master::unLatchPageAndSwap,
                    {
                        .mac_id = daemon_context.m_daemon_id,
                        .page_id = page_id,  // 换入的page(原本在远端)
                        .new_daemon_id = daemon_context.m_daemon_id,      // 自己的daemon id
                        .new_rack_id = daemon_context.m_options.rack_id,  // 自己的rack id
                        .page_id_swap = swapout_page_id,  // 换出的page(原本在本地)
                        .new_daemon_id_swap = dest_daemon_conn->daemon_id,  // 对方的daemon id
                        .new_rack_id_swap = dest_daemon_conn->rack_id,      // 对方的rack id
                    });

            unlatch_fu.get();
        }

        // DLOG("DN %u: Expect inPage %lu (from DN: %u) swap page finished!",
        //      daemon_context.m_daemon_id, page_id, dest_daemon_conn->daemon_id);
    }

    goto retry;
}

void allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     AllocPageMemoryRequest& req,
                     ResponseHandle<AllocPageMemoryReply>& resp_handle) {
    DLOG_ASSERT(daemon_context.m_page_table.TestAllocPageMemory(req.count),
                "Can't allocate more page memory");

    for (size_t c = 0; c < req.count; ++c) {
        PageVMMapMetadata* page_vm_meta = daemon_context.m_page_table.AllocPageMemory();
        PageMetadata* page_meta =
            daemon_context.m_page_table.FindOrCreatePageMeta(req.start_page_id + c);
        daemon_context.m_page_table.ApplyPageMemory(page_meta, page_vm_meta);
    }

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.ret = true;
}

void allocPage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               AllocPageRequest& req, ResponseHandle<AllocPageReply>& resp_handle) {
    DLOG("alloc %lu new pages", req.count);

    // 向Master调用allocPage
    auto fu = daemon_context.m_conn_manager.GetMasterConnection().erpc_conn->call<CortPromise>(
        rpc_master::allocPage, {
                                   .mac_id = daemon_context.m_daemon_id,
                                   .count = req.count,
                               });

    // 等待期间可能出现由于本地page不足而发生page swap
    auto& resp = fu.get();

    page_id_t start_page_id = resp.start_page_id;

    for (size_t c = 0; c < resp.start_count; ++c) {
        PageVMMapMetadata* page_vm_meta = daemon_context.m_page_table.AllocPageMemory();
        PageMetadata* page_meta =
            daemon_context.m_page_table.FindOrCreatePageMeta(start_page_id + c);
        daemon_context.m_page_table.ApplyPageMemory(page_meta, page_vm_meta);
    }

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.start_page_id = start_page_id;
}

void freePage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
              FreePageRequest& req, ResponseHandle<FreePageReply>& resp_handle) {
    DLOG_FATAL("Not Support");
}

void alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
           AllocRequest& req, ResponseHandle<AllocReply>& resp_handle) {
    DLOG_FATAL("Not Support");
}

void free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
          FreeRequest& req, ResponseHandle<FreeReply>& resp_handle) {
    DLOG_FATAL("Not Support");
}

void getPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    GetPageRDMARefRequest& req, ResponseHandle<GetPageRDMARefReply>& resp_handle) {
    PageMetadata* page_meta = daemon_context.m_page_table.FindOrCreatePageMeta(req.page_id);
    DLOG_ASSERT(page_meta->vm_meta != nullptr, "Can't find page %lu", req.page_id);

    uintptr_t local_addr = daemon_context.GetVirtualAddr(page_meta->vm_meta->cxl_memory_offset);
    ibv_mr* mr = daemon_context.GetMR(reinterpret_cast<void*>(local_addr));

    DLOG_ASSERT(mr->addr != nullptr, "The page %lu isn't registered to rdma memory", req.page_id);

    page_meta->vm_meta->ref_daemon.insert(&daemon_connection);

    // DLOG("get page %lu rdma ref [%#lx, %u], local [%#lx, %u],  peer_session = %d, daemon_id =
    // %u",
    //      req.page_id, local_addr, mr->rkey, local_addr, mr->lkey, daemon_connection.peer_session,
    //      daemon_connection.daemon_id);

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.addr = local_addr;
    reply.rkey = mr->rkey;
}

void delPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    DelPageRDMARefRequest& req, ResponseHandle<DelPageRDMARefReply>& resp_handle) {
    PageMetadata* page_meta = daemon_context.m_page_table.FindOrCreatePageMeta(req.page_id);

    std::unique_lock<CortSharedMutex> ref_lock(page_meta->page_ref_lock);
    RemotePageRefMeta* remote_page_ref_meta = page_meta->remote_ref_meta;

    DLOG_ASSERT(remote_page_ref_meta != nullptr, "Can't find page %lu's ref", req.page_id);

    // 清除该page的ref
    daemon_context.m_page_table.EraseRemotePageRefMeta(page_meta);

    // DLOG("DN %u: Del page %ld rdma ref", daemon_context.m_daemon_id, req.page_id);

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.ret = true;
}

void tryMigratePage(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    TryMigratePageRequest& req, ResponseHandle<TryMigratePageReply>& resp_handle) {
    daemon_context.m_stats.page_swap++;

    PageMetadata* page_meta = daemon_context.m_page_table.FindOrCreatePageMeta(req.page_id);

    std::unique_lock<CortSharedMutex> ref_lock(page_meta->page_ref_lock);

    DLOG_ASSERT(page_meta->vm_meta != nullptr, "Can't find page %lu", req.page_id);
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
    uintptr_t local_addr = daemon_context.GetVirtualAddr(page_meta->vm_meta->cxl_memory_offset);
    ibv_mr* mr = daemon_context.GetMR(reinterpret_cast<void*>(local_addr));
    uint32_t lkey = mr->lkey;

    DaemonToDaemonConnection* daemon_conn = dynamic_cast<DaemonToDaemonConnection*>(
        daemon_context.m_conn_manager.GetConnection(req.mac_id));

    rdma_rc::SgeWr sge_wrs[2];
    int sge_wrs_cnt = 1;
    daemon_conn->rdma_conn->prep_write(&sge_wrs[0], local_addr, lkey, page_size,
                                       req.swapin_page_addr, req.swapin_page_rkey, false);

    // DLOG("rdma write mid. swapout_page_addr = %lu", req.swapout_page_addr);
    bool is_swap;
    PageVMMapMetadata* local_page_vm_meta;
    if (req.swapout_page_addr == 0 && req.swapout_page_rkey == 0) {
        is_swap = false;
    } else {
        is_swap = true;
        // 交换的情况，需要读对方的page到本地
        while (!daemon_context.m_page_table.TestAllocPageMemory(1)) {
            // 当前swap区已满，等待完成
            boost::this_fiber::yield();
        }

        local_page_vm_meta = daemon_context.m_page_table.AllocPageMemory();

        uintptr_t swapin_addr =
            daemon_context.GetVirtualAddr(local_page_vm_meta->cxl_memory_offset);
        mr = daemon_context.GetMR(reinterpret_cast<void*>(swapin_addr));
        lkey = mr->lkey;
        daemon_conn->rdma_conn->prep_read(&sge_wrs[1], swapin_addr, lkey, page_size,
                                          req.swapout_page_addr, req.swapout_page_rkey, false);
        sge_wrs_cnt++;
    }

    auto fu = daemon_conn->rdma_conn->submit(sge_wrs, sge_wrs_cnt);

    fu.get();

    // DLOG(
    //     "DN %u: rdma write submit. local_addr = %ld, lkey = %u, req.swapin_page_addr = %ld,  "
    //     "req.swapin_page_rkey = %u",
    //     daemon_context.m_daemon_id, local_addr, lkey, req.swapin_page_addr,
    //     req.swapin_page_rkey);

    // DLOG("DN %u: reply", daemon_context.m_daemon_id);

    // 回收迁移走的页面
    daemon_context.m_page_table.CancelPageMemory(page_meta);

    if (is_swap) {
        // 若page没有剩余，迁移到了swap区，现在转移到page区域
        PageMetadata* swap_page_meta =
            daemon_context.m_page_table.FindOrCreatePageMeta(req.swap_page_id);
        daemon_context.m_page_table.ApplyPageMemory(swap_page_meta, local_page_vm_meta);
    }

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.swaped = is_swap;
    // DLOG("DN %u: finished migrate!", daemon_context.m_daemon_id);
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
    // DLOG("DN %u: delPageRefBroadcast page %lu", daemon_context.m_daemon_id, page_id);

    std::vector<decltype(((ErpcClient*)0)->call<CortPromise>(rpc_daemon::delPageRDMARef, {}))>
        del_ref_fu_vec;
    std::vector<decltype(((MsgQClient*)0)->call<CortPromise>(rpc_client::removePageCache, {}))>
        remove_cache_fu_vec;

    for (auto daemon_conn : page_meta->vm_meta->ref_daemon) {
        if (daemon_conn->daemon_id == unless_daemon) {
            continue;
        }
        // DLOG("DN %u: delPageRefBroadcast for i = %ld, peer_session = %d, daemon_id = %u",
        //      daemon_context.m_daemon_id, i, daemon_conn->peer_session, daemon_conn->daemon_id);

        auto fu = daemon_conn->erpc_conn->call<CortPromise>(
            rpc_daemon::delPageRDMARef, {
                                            .mac_id = daemon_context.m_daemon_id,
                                            .page_id = page_id,  // 准备删除ref的page id
                                        });

        del_ref_fu_vec.push_back(std::move(fu));
    }

    for (auto client_conn : page_meta->vm_meta->ref_client) {
        // DLOG("DN %u: delPageCacheBroadcast client_id = %u", daemon_context.m_daemon_id,
        //      client_conn->client_id);

        auto fu = client_conn->msgq_conn->call<CortPromise>(
            rpc_client::removePageCache, {
                                             .mac_id = daemon_context.m_daemon_id,
                                             .page_id = page_id,
                                         });

        remove_cache_fu_vec.push_back(std::move(fu));
    }

    for (auto& fu : del_ref_fu_vec) {
        fu.get();
    }
    // DLOG("Finish delPageRefBroadcast");

    for (auto& fu : remove_cache_fu_vec) {
        fu.get();
    }
    // DLOG("Finish delPageCacheBroadcast");
}

/************************ for test ***************************/

void __testdataSend1(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                     __TestDataSend1Request& req,
                     ResponseHandle<__TestDataSend1Reply>& resp_handle) {
    __TestDataSend1Reply reply;
    reply.size = req.size;
    assert(req.size == 64);

    memcpy(reply.data, req.data, reply.size * sizeof(int));

    resp_handle.Init();
}
void __testdataSend2(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                     __TestDataSend2Request& req,
                     ResponseHandle<__TestDataSend2Reply>& resp_handle) {
    __TestDataSend2Reply reply;
    reply.size = req.size;
    assert(req.size == 72);

    memcpy(reply.data, req.data, reply.size * sizeof(int));
    resp_handle.Init();
}

void __notifyPerf(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                  __notifyPerfRequest& req, ResponseHandle<__notifyPerfReply>& resp_handle) {
    __DEBUG_START_PERF();
    resp_handle.Init();
}

void __stopPerf(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                __stopPerfRequest& req, ResponseHandle<__stopPerfReply>& resp_handle) {
    exit(-1);
    resp_handle.Init();
}

}  // namespace rpc_daemon
