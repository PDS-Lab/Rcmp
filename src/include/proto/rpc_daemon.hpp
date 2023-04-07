#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "rchms.hpp"
#include "rpc_base.hpp"

namespace rpc_daemon {

struct JoinRackRequest : public RequestMsg {
    char client_ipv4[16];
    uint16_t client_port;
    rack_id_t rack_id;
};
struct JoinRackReply : public ResponseMsg {
    mac_id_t client_mac_id;
    mac_id_t daemon_mac_id;
};
/**
 * @brief 将client加入到机柜中。在建立连接时调用。
 *
 * @param daemon_context
 * @param client_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @return true 加入成功
 * @return false 加入失败
 */
JoinRackReply joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                       JoinRackRequest& req);

struct GetPageRefRequest : public RequestMsg {
    page_id_t page_id;
};
struct GetPageRefReply : public ResponseMsg {
    offset_t offset;
    // union {
    // struct {
    //     char dest_daemon_ipv4[16];
    //     uint16_t dest_daemon_port;
    // };
    // };
};
/**
 * @brief 获取page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。
 *
 * // 如果是直接内存访问，则返回-1，让client自己调用master的
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return GetPageRefReply
 */
GetPageRefReply getPageRef(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, GetPageRefRequest& req);

struct AllocPageMemoryRequest : public RequestMsg {
    page_id_t page_id;
    size_t slab_size;
};
struct AllocPageMemoryReply : public ResponseMsg {
    bool ret;
};
/**
 * @brief 申请一个page物理地址空间
 *
 * @param daemon_context
 * @param master_connection
 * @param req
 * @return AllocPageMemoryReply
 */
AllocPageMemoryReply allocPageMemory(DaemonContext& daemon_context,
                                     DaemonToMasterConnection& master_connection,
                                     AllocPageMemoryRequest& req);

struct AllocRequest : public RequestMsg {
    size_t size;
};
struct AllocReply : public ResponseMsg {
    rchms::GAddr gaddr;
};
/**
 * @brief 申请一个内存地址。如果本地缺少有效的page，则向master发送`allocPage()`请求获取新page
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return AllocReply
 */
AllocReply alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                 AllocRequest& req);

struct FreeRequest : public RequestMsg {
    rchms::GAddr gaddr;
    size_t n;
};
struct FreeReply : public ResponseMsg {
    bool ret;
};
/**
 * @brief 释放一个内存地址。
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return FreeReply
 */
FreeReply free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               FreeRequest& req);

/************************* for test ***************************/

struct __TestDataSend1Request : public RequestMsg {
    size_t size;
    int data[64];
};
struct __TestDataSend1Reply : public ResponseMsg {
    size_t size;
    int data[64];
};

struct __TestDataSend2Request : public RequestMsg {
    size_t size;
    int data[72];
};
struct __TestDataSend2Reply : public ResponseMsg {
    size_t size;
    int data[72];
};

/**
 * @brief 发送数据测试1
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return __TestDataSend1Reply
 */
__TestDataSend1Reply __testdataSend1(DaemonContext& daemon_context,
                                     DaemonToClientConnection& client_connection,
                                     __TestDataSend1Request& req);

/**
 * @brief 发送数据测试2
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return __TestDataSend2Reply
 */
__TestDataSend2Reply __testdataSend2(DaemonContext& daemon_context,
                                     DaemonToClientConnection& client_connection,
                                     __TestDataSend2Request& req);

}  // namespace rpc_daemon
