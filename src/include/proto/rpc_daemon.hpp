#pragma once

#include "common.hpp"
#include "log.hpp"
#include "rchms.hpp"
#include "rrpc.hpp"
#include "daemon_impl.hpp"

namespace rpc_daemon {

/**
 * @brief 将client加入到机柜中。在建立连接时调用。
 * 
 * @param daemon_context 
 * @param client_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @return true 加入成功
 * @return false 加入失败
 */
bool joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection);

/**
 * @brief 获取page的引用。如果本地Page Table没有该page id，则会触发远程调用。如果是直接内存访问，则返回-1，让client自己调用master的
 * 
 * @param daemon_context 
 * @param client_connection 
 * @param page_id 
 * @return offset_t 
 */
offset_t getPageRef(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                    page_id_t page_id);

bool allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     page_id_t page_id, size_t slab_size);

rchms::GAddr alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection, size_t n);
bool free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection, rchms::GAddr gaddr,
          size_t n);

}  // namespace rpc_daemon