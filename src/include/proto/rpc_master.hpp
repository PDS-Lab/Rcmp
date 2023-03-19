#pragma once

#include "common.hpp"
#include "master_impl.hpp"

namespace rpc_master {

/**
 * @brief 将daemon加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param daemon_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param rack_id
 * @param with_cxl
 * @param free_page_num
 * @return mac_id_t
 */
mac_id_t joinCluster(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                     rack_id_t rack_id, uint16_t daemon_port, bool with_cxl, size_t free_page_num);

/**
 * @brief 将client加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param client_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param with_cxl
 * @return mac_id_t
 */
mac_id_t joinCluster(MasterContext& master_context, MasterClientConnection& client_connection,
                     bool with_cxl);

struct AllocPageReply {
    page_id_t page_id;
    bool need_self_alloc_page_memory;
};
/**
 * @brief
 * 申请一个page。该操作会希望在daemon端调用`allocPageMemory()`进行分配CXL物理地址。如果该daemon已满，本操作会随机向其他daemon发送该函数进行分配，此时原daemon不应将page
 * id加入到Page Table中。
 *
 * @param master_context
 * @param daemon_connection
 * @param slab_size
 * @return AllocPageReply 如果page被派到daemon上，则`need_self_alloc_page_memory`为true
 */
AllocPageReply allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                         size_t slab_size);

/**
 * @brief 释放page。该操作需要保证daemon本身持有这个页时才能释放这个page。
 *
 * @param master_context
 * @param daemon_connection
 * @param page_id
 */
void freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
              page_id_t page_id);


struct GetRackDaemonByPageIDReply {
    char daemon_ipv4[16];
    uint16_t daemon_port;
    rack_id_t rack_id;
};
/**
 * @brief 根据page id获取对应rack的daemon的IPv4地址。该调用应在client的`远程直接访问`情况下使用。
 * 
 * @param master_context 
 * @param client_connection 
 * @param page_id 
 * @return GetRackDaemonByPageIDReply 
 */
GetRackDaemonByPageIDReply getRackDaemonByPageID(MasterContext& master_context, MasterClientConnection& client_connection, page_id_t page_id);

}  // namespace rpc_master