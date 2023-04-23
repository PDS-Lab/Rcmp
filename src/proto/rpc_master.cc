#include "proto/rpc_master.hpp"

#include "common.hpp"
#include "cort_sched.hpp"
#include "log.hpp"

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

    master_context.m_cluster_manager.cluster_rack_table.insert(req.rack_id, rack_table);
    master_context.m_cluster_manager.connect_table.insert(daemon_connection.daemon_id,
                                                          &daemon_connection);

    master_context.m_page_id_allocator->addCapacity(rack_table->max_free_page_num);

    DLOG("Connect with daemon [rack:%d --- id:%d]", daemon_connection.rack_id,
         daemon_connection.daemon_id);

    auto local_addr = master_context.m_listen_conn.get_local_addr();

    JoinDaemonReply reply;
    reply.daemon_mac_id = mac_id;
    reply.master_mac_id = master_context.m_master_id;
    reply.rdma_ipv4 = local_addr.first;
    reply.rdma_port = local_addr.second;
    return reply;
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

    // 采取就近原则分配page到daemon所在rack
    for (size_t c = 0; c < current_rack_alloc_page_num; ++c) {
        PageRackMetadata* page_meta = new PageRackMetadata();
        page_meta->rack_id = daemon_connection.rack_id;
        page_meta->daemon_id = daemon_connection.daemon_id;
        DLOG("alloc page: %lu ---> rack %d", new_page_id + c, rack_table->daemon_connect->rack_id);
        master_context.m_page_directory.insert(new_page_id + c, page_meta);
    }
    rack_table->current_allocated_page_num += current_rack_alloc_page_num;

    if (other_rack_alloc_page_num > 0) {
        // TODO: 如果daemon没有多余page，则向其他rack daemon注册allocPageMemory
        // TODO: 启动migrate机制
        DLOG_FATAL("Not Support");
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

    if (!page_meta->latch.try_lock()) {
        this_cort::reset_resume_cond([&page_meta]() { return page_meta->latch.try_lock(); });
        this_cort::yield();
    }

    MasterToDaemonConnection* dest_daemon = dynamic_cast<MasterToDaemonConnection*>(
        master_context.get_connection(page_meta->daemon_id));

    auto peer_addr = dest_daemon->rdma_conn->get_peer_addr();

    LatchRemotePageReply reply;
    reply.dest_rack_id = page_meta->rack_id;
    reply.dest_daemon_id = page_meta->daemon_id;
    reply.dest_daemon_ipv4 = dest_daemon->ip;
    reply.dest_daemon_erpc_port = dest_daemon->port;
    reply.dest_daemon_rdma_ipv4 = peer_addr.first;
    reply.dest_daemon_rdma_port = peer_addr.second;
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

    page_meta->latch.unlock();

    UnLatchRemotePageReply reply;
    return reply;
}

}  // namespace rpc_master