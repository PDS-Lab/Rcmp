#include "proto/rpc_master.hpp"

#include <chrono>

#include "common.hpp"
#include "cort_sched.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_daemon.hpp"

using namespace std::literals;

namespace rpc_master {

JoinDaemonReply joinDaemon(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection, JoinDaemonRequest& req) {
    RackMacTable* rack_table;
    auto it = master_context.m_cluster_manager.cluster_rack_table.find(req.rack_id);
    DLOG_ASSERT(it == master_context.m_cluster_manager.cluster_rack_table.end(),
                "Reconnect rack %u daemon", req.rack_id);

    rack_table = new RackMacTable();
    rack_table->with_cxl = req.with_cxl;
    rack_table->max_free_page_num = req.free_page_num;
    rack_table->current_allocated_page_num = 0;

    mac_id_t mac_id = master_context.m_cluster_manager.mac_id_allocator->gen();

    rack_table->daemon_connect = &daemon_connection;
    rack_table->daemon_connect->rack_id = req.rack_id;
    rack_table->daemon_connect->daemon_id = mac_id;
    rack_table->daemon_connect->ip = req.ip.get_string();
    rack_table->daemon_connect->port = req.port;
    rack_table->daemon_connect->peer_session = master_context.get_erpc().create_session(
        rack_table->daemon_connect->ip + ":" + std::to_string(rack_table->daemon_connect->port), 0);

    size_t rack_count = master_context.m_cluster_manager.cluster_rack_table.size();
    JoinDaemonReply* reply_ptr =
        req.alloc_flex_resp(sizeof(JoinDaemonReply::RackInfo) * rack_count);
    reply_ptr->other_rack_count = rack_count;
    master_context.m_cluster_manager.cluster_rack_table.foreach_all(
        [&, i = 0](std::pair<const rack_id_t, RackMacTable*>& p) mutable {
            DLOG_ASSERT(i < rack_count);

            MasterToDaemonConnection* conn = p.second->daemon_connect;
            auto& info = reply_ptr->other_rack_infos[i];
            auto peer_addr = conn->rdma_conn->get_peer_addr();
            info.rack_id = conn->rack_id;
            info.daemon_id = conn->daemon_id;
            info.daemon_ipv4 = conn->ip;
            info.daemon_erpc_port = conn->port;
            info.daemon_rdma_ipv4 = peer_addr.first;
            info.daemon_rdma_port = peer_addr.second;
            i++;
            return true;
        });

    master_context.m_cluster_manager.cluster_rack_table.insert(req.rack_id, rack_table);
    master_context.m_cluster_manager.connect_table.insert(daemon_connection.daemon_id,
                                                          &daemon_connection);

    master_context.m_page_id_allocator->addCapacity(rack_table->max_free_page_num);

    DLOG("Connect with daemon [rack:%d --- id:%d]", daemon_connection.rack_id,
         daemon_connection.daemon_id);

    auto local_addr = master_context.m_listen_conn.get_local_addr();

    reply_ptr->daemon_mac_id = mac_id;
    reply_ptr->master_mac_id = master_context.m_master_id;
    reply_ptr->rdma_ipv4 = local_addr.first;
    reply_ptr->rdma_port = local_addr.second;
    return {};
}

JoinClientReply joinClient(MasterContext& master_context,
                           MasterToClientConnection& client_connection, JoinClientRequest& req) {
    RackMacTable* rack_table;
    auto it = master_context.m_cluster_manager.cluster_rack_table.find(req.rack_id);
    DLOG_ASSERT(it != master_context.m_cluster_manager.cluster_rack_table.end(),
                "Don't find rack %u", req.rack_id);

    rack_table = it->second;

    mac_id_t mac_id = master_context.m_cluster_manager.mac_id_allocator->gen();

    client_connection.rack_id = req.rack_id;
    client_connection.client_id = mac_id;

    rack_table->client_connect_table.push_back(&client_connection);

    DLOG("Connect with client [rack:%d --- id:%d]", client_connection.rack_id,
         client_connection.client_id);

    JoinClientReply reply;
    reply.mac_id = mac_id;
    return reply;
}

AllocPageReply allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                         AllocPageRequest& req) {
    RackMacTable* rack_table;

    auto it = master_context.m_cluster_manager.cluster_rack_table.find(daemon_connection.rack_id);
    DLOG_ASSERT(it != master_context.m_cluster_manager.cluster_rack_table.end(),
                "Can't find this deamon");

    rack_table = it->second;

    page_id_t new_page_id = master_context.m_page_id_allocator->multiGen(req.count);
    DLOG_ASSERT(new_page_id != invalid_page_id, "no unusable page");

    size_t current_rack_alloc_page_num =
        std::min(req.count, rack_table->max_free_page_num - rack_table->current_allocated_page_num);
    size_t other_rack_alloc_page_num = req.count - current_rack_alloc_page_num;

    // 已分配page的序号
    size_t c = 0;
    // 采取就近原则分配page到daemon所在rack
    while (c < current_rack_alloc_page_num) {
        PageRackMetadata* page_meta = new PageRackMetadata();
        page_meta->rack_id = daemon_connection.rack_id;
        page_meta->daemon_id = daemon_connection.daemon_id;
        // DLOG("alloc page: %lu ---> rack %d", new_page_id + c,
        // rack_table->daemon_connect->rack_id);
        master_context.m_page_directory.insert(new_page_id + c, page_meta);
        c++;
    }

    rack_table->current_allocated_page_num += current_rack_alloc_page_num;

    if (other_rack_alloc_page_num > 0) {
        // 如果current daemon没有多余page，则向其他rack daemon注册allocPageMemory
        using PageAllocRPC = RPC_TYPE_STRUCT(rpc_daemon::allocPageMemory);

        struct CTX {
            std::promise<void> pro;
            std::future<void> fu;
            erpc::MsgBufferWrap req_raw;
            erpc::MsgBufferWrap resp_raw;
            MasterToDaemonConnection* daemon_connect;
            page_id_t alloc_start_page_id;
            size_t alloc_cnt;

            CTX(erpc::MsgBufferWrap req_raw, erpc::MsgBufferWrap resp_raw)
                : req_raw(req_raw), resp_raw(resp_raw) {}
        };
        // 暂存消息上下文
        std::vector<CTX*> ctxv;

        // 遍历rack表，找出有空闲page的rack分配
        master_context.m_cluster_manager.cluster_rack_table.foreach_all(
            [&](std::pair<const rack_id_t, RackMacTable*>& p) {
                size_t rack_alloc_page_num =
                    std::min(other_rack_alloc_page_num,
                             p.second->max_free_page_num - p.second->current_allocated_page_num);

                if (rack_alloc_page_num == 0) {
                    return true;
                }

                auto& rpc = master_context.get_erpc();

                auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::RequestType));
                auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(PageAllocRPC::ResponseType));

                auto req = reinterpret_cast<PageAllocRPC::RequestType*>(req_raw.get_buf());

                req->mac_id = master_context.m_master_id;
                req->start_page_id = new_page_id + c;
                req->count = rack_alloc_page_num;

                CTX* ctx = new CTX(req_raw, resp_raw);
                ctx->fu = ctx->pro.get_future();
                ctx->alloc_start_page_id = new_page_id + c;
                ctx->daemon_connect = p.second->daemon_connect;
                ctx->alloc_cnt = rack_alloc_page_num;
                rpc.enqueue_request(p.second->daemon_connect->peer_session, PageAllocRPC::rpc_type,
                                    req_raw, resp_raw, erpc_general_promise_flag_cb,
                                    static_cast<void*>(&ctx->pro));

                ctxv.push_back(ctx);

                c += rack_alloc_page_num;

                // 减去在该rack分配的page数
                other_rack_alloc_page_num -= rack_alloc_page_num;
                // 如果到0，则可以停止遍历
                return other_rack_alloc_page_num != 0;
            });

        // join all ctx
        for (auto& ctx : ctxv) {
            // 逐个切出协程
            this_cort::reset_resume_cond(
                [&ctx]() { return ctx->fu.wait_for(0s) != std::future_status::timeout; });
            this_cort::yield();

            for (size_t i = 0; i < ctx->alloc_cnt; ++i) {
                PageRackMetadata* page_meta = new PageRackMetadata();
                page_meta->rack_id = ctx->daemon_connect->rack_id;
                page_meta->daemon_id = ctx->daemon_connect->daemon_id;
                // DLOG("alloc page: %lu ---> rack %d", ctx->alloc_start_page_id + i,
                //      ctx->daemon_connect->rack_id);
                master_context.m_page_directory.insert(ctx->alloc_start_page_id + i, page_meta);
            }

            auto& rpc = master_context.get_erpc();

            rpc.free_msg_buffer(ctx->req_raw);
            rpc.free_msg_buffer(ctx->resp_raw);

            delete ctx;
        }
    }

    AllocPageReply reply;
    reply.start_page_id = new_page_id;
    reply.start_count = current_rack_alloc_page_num;
    return reply;
}

FreePageReply freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       FreePageRequest& req) {
    RackMacTable* rack_table;
    PageRackMetadata* page_meta;

    auto it = master_context.m_page_directory.find(req.start_page_id);
    DLOG_ASSERT(it != master_context.m_page_directory.end(), "Can't free this page id %lu",
                req.start_page_id);

    page_meta = it->second;

    // TODO: 支持free任意rack上的page
    // 需要删除指定rack上的page meta、cache等元数据
    DLOG_FATAL("Not Support");

    auto rack_table_it =
        master_context.m_cluster_manager.cluster_rack_table.find(page_meta->rack_id);
    DLOG_ASSERT(rack_table_it != master_context.m_cluster_manager.cluster_rack_table.end(),
                "Can't find this deamon");

    rack_table = rack_table_it->second;

    master_context.m_page_directory.erase(req.start_page_id);
    master_context.m_page_id_allocator->recycle(req.start_page_id);
    rack_table->current_allocated_page_num--;
    delete page_meta;

    FreePageReply reply;
    reply.ret = true;
    return reply;
}

LatchRemotePageReply latchRemotePage(MasterContext& master_context,
                                     MasterToDaemonConnection& daemon_connection,
                                     LatchRemotePageRequest& req) {
    PageRackMetadata* page_meta;
    auto it = master_context.m_page_directory.find(req.page_id);
    DLOG_ASSERT(it != master_context.m_page_directory.end(), "Can't find this page %lu",
                req.page_id);

    page_meta = it->second;

    if (!req.isWriteLock) {
        if (!page_meta->latch.try_lock_shared()) {
            this_cort::reset_resume_cond(
                [&page_meta]() { return page_meta->latch.try_lock_shared(); });
            this_cort::yield();
        }
    } else {  // 写锁时，才需要判断是否有swap(有swap时，按page id的大小顺序进行上锁，避免死锁)
        if (req.page_id_swap != invalid_page_id && req.page_id_swap < req.page_id) {
            PageRackMetadata* page_meta_swap;
            auto it_swap = master_context.m_page_directory.find(req.page_id_swap);
            DLOG_ASSERT(it_swap != master_context.m_page_directory.end(),
                        "Can't find this page %lu", req.page_id_swap);
            // TODO：死锁问题
            page_meta_swap = it_swap->second;
            if (!page_meta_swap->latch.try_lock()) {
                this_cort::reset_resume_cond(
                    [&page_meta_swap]() { return page_meta_swap->latch.try_lock(); });
                this_cort::yield();
            }
        }

        if (!page_meta->latch.try_lock()) {
            this_cort::reset_resume_cond([&page_meta]() { return page_meta->latch.try_lock(); });
            this_cort::yield();
        }

        if (req.page_id_swap != invalid_page_id && req.page_id_swap > req.page_id) {
            PageRackMetadata* page_meta_swap;
            auto it_swap = master_context.m_page_directory.find(req.page_id_swap);
            DLOG_ASSERT(it_swap != master_context.m_page_directory.end(),
                        "Can't find this page %lu", req.page_id_swap);
            page_meta_swap = it_swap->second;
            if (!page_meta_swap->latch.try_lock()) {
                this_cort::reset_resume_cond(
                    [&page_meta_swap]() { return page_meta_swap->latch.try_lock(); });
                this_cort::yield();
            }
        }
    }

    LatchRemotePageReply reply;
    reply.dest_rack_id = page_meta->rack_id;
    reply.dest_daemon_id = page_meta->daemon_id;
    return reply;
}

UnLatchRemotePageReply unLatchRemotePage(MasterContext& master_context,
                                         MasterToDaemonConnection& daemon_connection,
                                         UnLatchRemotePageRequest& req) {
    PageRackMetadata* page_meta;

    auto it = master_context.m_page_directory.find(req.page_id);
    DLOG_ASSERT(it != master_context.m_page_directory.end(), "Can't find this page %lu",
                req.page_id);

    page_meta = it->second;

    page_meta->latch.unlock_shared();

    UnLatchRemotePageReply reply;
    return reply;
}

UnLatchPageAndBalanceReply unLatchPageAndBalance(MasterContext& master_context,
                                                 MasterToDaemonConnection& daemon_connection,
                                                 UnLatchPageAndBalanceRequest& req) {
    // DLOG("Into unLatchPageAndBalance");
    PageRackMetadata* page_meta;
    auto p = master_context.m_page_directory.find(req.page_id);
    DLOG_ASSERT(p != master_context.m_page_directory.end(), "Can't find this page %lu",
                req.page_id);
    page_meta = p->second;

    page_meta->rack_id = req.new_rack_id;
    page_meta->daemon_id = req.new_daemon_id;

    page_meta->latch.unlock();
    // DLOG("Swap page %lu to rack: %u, DN:%u. This operation is initiated by DN %u", req.page_id,
    //      req.new_rack_id, req.new_daemon_id, daemon_connection.daemon_id);

    if (req.page_id_swap != invalid_page_id) {
        auto p = master_context.m_page_directory.find(req.page_id_swap);
        DLOG_ASSERT(p != master_context.m_page_directory.end(), "Can't find this page %lu",
                    req.page_id_swap);
        page_meta = p->second;

        page_meta->rack_id = req.new_rack_id_swap;
        page_meta->daemon_id = req.new_daemon_id_swap;

        page_meta->latch.unlock();
        // DLOG("Swap page %lu to rack: %u, DN:%u. This operation is initiated by DN %u",
        //      req.page_id_swap, req.new_rack_id_swap, req.new_daemon_id_swap,
        //      daemon_connection.daemon_id);
    }

    master_context.m_stats.page_swap++;

    UnLatchPageAndBalanceReply reply;
    return reply;
}

}  // namespace rpc_master