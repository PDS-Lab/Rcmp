#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "rchms.hpp"
#include "rpc_base.hpp"

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

struct GetPageRefReply {
    bool local_get;
    union {
        offset_t offset;
        struct {
            char dest_daemon_ipv4[16];
            uint16_t dest_daemon_port;
        };
    };
};
/**
 * @brief 获取page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。如果是直接内存访问，则返回-1，让client自己调用master的
 *
 * @param daemon_context
 * @param client_connection
 * @param page_id
 * @return GetPageRefReply
 */
GetPageRefReply getPageRef(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, page_id_t page_id);

/**
 * @brief 申请一个page物理地址空间
 *
 * @param daemon_context
 * @param master_connection
 * @param page_id
 * @param slab_size
 * @return true
 * @return false
 */
bool allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     page_id_t page_id, size_t slab_size);

/**
 * @brief 申请一个内存地址。如果本地缺少有效的page，则向master发送`allocPage()`请求获取新page
 *
 * @param daemon_context
 * @param client_connection
 * @param n
 * @return rchms::GAddr
 */
rchms::GAddr alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                   size_t n);

/**
 * @brief 释放一个内存地址。
 *
 * @param daemon_context
 * @param client_connection
 * @param gaddr
 * @param n
 * @return true
 * @return false
 */
bool free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
          rchms::GAddr gaddr, size_t n);

}  // namespace rpc_daemon
