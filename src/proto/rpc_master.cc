#include "proto/rpc_master.hpp"

#include "common.hpp"
#include "log.hpp"

namespace rpc_master {

JoinDaemonReply joinDaemon(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection, JoinDaemonRequest& req) {
    RackMacTable* rack_table;
    bool ret = master_context.m_cluster_manager.cluster_rack_table.find(req.rack_id, &rack_table);
    DLOG_ASSERT(!ret, "Reconnect rack %u daemon", req.rack_id);

    rack_table = new RackMacTable();
    rack_table->with_cxl = req.with_cxl;
    rack_table->max_free_page_num = req.free_page_num;
    rack_table->current_allocated_page_num = 0;

    mac_id_t mac_id = master_context.m_cluster_manager.mac_id_allocator->gen();

    rack_table->daemon_connect = &daemon_connection;
    rack_table->daemon_connect->rack_id = req.rack_id;
    rack_table->daemon_connect->daemon_id = mac_id;

    master_context.m_cluster_manager.cluster_rack_table.insert(req.rack_id, rack_table);
    master_context.m_cluster_manager.connect_table.insert(daemon_connection.daemon_id,
                                                          &daemon_connection);

    master_context.m_page_id_allocator->addCapacity(rack_table->max_free_page_num);

    DLOG("Connect with daemon [rack:%d --- id:%d]", daemon_connection.rack_id,
         daemon_connection.daemon_id);

    auto local_addr = master_context.listen_conn.get_local_addr();

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
    bool ret = master_context.m_cluster_manager.cluster_rack_table.find(req.rack_id, &rack_table);
    DLOG_ASSERT(ret, "Don't find rack %u", req.rack_id);

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
    PageRackMetadata page_rack_metadata;

    bool ret = master_context.m_cluster_manager.cluster_rack_table.find(daemon_connection.rack_id,
                                                                        &rack_table);
    DLOG_ASSERT(ret, "Can't find this deamon");

    page_id_t new_page_id = master_context.m_page_id_allocator->gen();
    DLOG_ASSERT(new_page_id != invalid_page_id, "no unusable page");

    if (rack_table->current_allocated_page_num < rack_table->max_free_page_num) {
        // 采取就近原则分配page到daemon所在rack
        page_rack_metadata.rack_id = daemon_connection.rack_id;
        rack_table->current_allocated_page_num++;
    } else {
        // TODO: 如果daemon没有多余page，则向其他rack daemon注册，调用allocPageMemory(req.slab_size)
        // TODO: 启动migrate机制
        DLOG_FATAL("Not Support");
    }

    DLOG("alloc page: %lu ---> rack %d", new_page_id, rack_table->daemon_connect->rack_id);

    master_context.m_page_directory.insert(new_page_id, new PageRackMetadata(page_rack_metadata));

    AllocPageReply reply;
    reply.page_id = new_page_id;
    return reply;
}

FreePageReply freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       FreePageRequest& req) {
    RackMacTable* rack_table;
    PageRackMetadata* page_rack_metadata;

    bool ret = master_context.m_cluster_manager.cluster_rack_table.find(daemon_connection.rack_id,
                                                                        &rack_table);
    DLOG_ASSERT(ret, "Can't find this deamon");

    ret = master_context.m_page_directory.find(req.page_id, &page_rack_metadata);
    DLOG_ASSERT(ret, "Can't free this page id %lu", req.page_id);

    DLOG_ASSERT(page_rack_metadata->rack_id == daemon_connection.rack_id,
                "You must free this page id %lu by daemon holds on", req.page_id);

    master_context.m_page_directory.erase(req.page_id);
    master_context.m_page_id_allocator->recycle(req.page_id);
    delete page_rack_metadata;

    FreePageReply reply;
    reply.ret = true;
    return reply;
}

}  // namespace rpc_master