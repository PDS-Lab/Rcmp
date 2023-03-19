#include "proto/rpc_master.hpp"

#include "common.hpp"
#include "log.hpp"

namespace rpc_master {

mac_id_t joinCluster(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                 rack_id_t rack_id, bool with_cxl, size_t free_page_num) {
    RackMacTable* rack_table;
    bool ret = master_context.cluster_manager.cluster_connect_table.find(rack_id, &rack_table);
    DLOG_ASSERT(!ret, "Reconnect rack %u daemon", rack_id);

    rack_table = new RackMacTable();
    rack_table->with_cxl = with_cxl;
    rack_table->max_free_page_num = free_page_num;
    rack_table->current_allocated_page_num = 0;
    
    mac_id_t mac_id = master_context.cluster_manager.mac_id_allocator->gen();

    rack_table->daemon_connect = &daemon_connection;
    rack_table->daemon_connect->rack_id = rack_id;
    rack_table->daemon_connect->daemon_id = mac_id;

    master_context.cluster_manager.cluster_connect_table.insert(rack_id, rack_table);

    return mac_id;
}

mac_id_t joinCluster(MasterContext& master_context, MasterClientConnection& client_connection,
                 bool with_cxl) {}

AllocPageReply allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                         size_t slab_size) {
    AllocPageReply reply;

    RackMacTable* rack_table;
    PageRackMetadata page_rack_metadata;

    bool ret = master_context.cluster_manager.cluster_connect_table.find(daemon_connection.rack_id,
                                                                         &rack_table);
    DLOG_ASSERT(ret, "Can't find this deamon");

    page_id_t new_page_id = master_context.page_id_allocator->gen();
    DLOG_ASSERT(new_page_id != invalid_page_id, "no unusable page");

    reply.page_id = new_page_id;

    if (rack_table->current_allocated_page_num < rack_table->max_free_page_num) {
        // 采取就近原则分配page到daemon所在rack
        page_rack_metadata.rack_id = daemon_connection.rack_id;
        rack_table->current_allocated_page_num++;
        reply.need_self_alloc_page_memory = true;
    } else {
        // TODO: 如果daemon没有多余page，则向其他rack daemon注册，调用allocPageMemory(slab_size)
        DLOG_FATAL("Not Support");
        reply.need_self_alloc_page_memory = false;
    }

    master_context.page_directory.insert(new_page_id, new PageRackMetadata(page_rack_metadata));
    return reply;
}

void freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
              page_id_t page_id) {
    RackMacTable* rack_table;
    PageRackMetadata* page_rack_metadata;

    bool ret = master_context.cluster_manager.cluster_connect_table.find(daemon_connection.rack_id,
                                                                         &rack_table);
    DLOG_ASSERT(ret, "Can't find this deamon");

    ret = master_context.page_directory.find(page_id, &page_rack_metadata);
    DLOG_ASSERT(ret, "Can't free this page id %lu", page_id);

    DLOG_ASSERT(page_rack_metadata->rack_id == daemon_connection.rack_id,
                "You must free this page id %lu by daemon holds on", page_id);

    master_context.page_directory.erase(page_id);
    master_context.page_id_allocator->recycle(page_id);
    delete page_rack_metadata;
}
}  // namespace rpc_master