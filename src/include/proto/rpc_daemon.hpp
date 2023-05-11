#pragma once

#include <cstdint>

#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "rchms.hpp"
#include "rpc_base.hpp"
#include "utils.hpp"

namespace rpc_daemon {

struct JoinRackRequest : public RequestMsg {
    IPv4String client_ipv4;
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

struct CrossRackConnectRequest : public RequestMsg {
    IPv4String ip;
    uint16_t port;
    rack_id_t rack_id;
    mac_id_t conn_mac_id;
};
struct CrossRackConnectReply : public ResponseMsg {
    mac_id_t daemon_mac_id;
    IPv4String rdma_ipv4;
    uint16_t rdma_port;
};
CrossRackConnectReply crossRackConnect(DaemonContext& daemon_context,
                                       DaemonToDaemonConnection& daemon_connection,
                                       CrossRackConnectRequest& req);

struct GetPageCXLRefOrProxyReply;
struct GetPageCXLRefOrProxyRequest : public RequestMsg,
                                     detail::RawResponseReturn<GetPageCXLRefOrProxyReply> {
    enum {
        READ,
        WRITE,
        WRITE_RAW,
    } type;
    rchms::GAddr gaddr;
    union {
        struct {  // type == READ
            size_t cn_read_size;
        };
        struct {  // type == WRITE
            size_t cn_write_size;
            void* cn_write_buf;
        };
        struct {  // type == WRITE_RAW
            size_t cn_write_raw_size;
            uint8_t cn_write_raw_buf[0];
        };
    };
};
struct GetPageCXLRefOrProxyReply : public ResponseMsg {
    bool refs;
    union {
        struct {  // refs == true
            offset_t offset;
        };
        struct {  // refs == false
            uint8_t read_data[0];
        };
    };
};
/**
 * @brief 获取page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return GetPageRefOrProxyReply
 */
GetPageCXLRefOrProxyReply getPageCXLRefOrProxy(DaemonContext& daemon_context,
                                               DaemonToClientConnection& client_connection,
                                               GetPageCXLRefOrProxyRequest& req);

struct AllocPageMemoryRequest : public RequestMsg {
    page_id_t start_page_id;
    size_t count;
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
struct AllocPageRequest : public RequestMsg {
    size_t count;
};
struct AllocPageReply : public ResponseMsg {
    page_id_t start_page_id;  // 分配的起始页id
    size_t start_count;       // 实际在请求方rack分配的个数
};
/**
 * @brief
 * 申请一个page
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @return AllocPageReply
 */
AllocPageReply allocPage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                         AllocPageRequest& req);

struct FreePageRequest : public RequestMsg {
    page_id_t start_page_id;
    size_t count;
};
struct FreePageReply : public ResponseMsg {
    bool ret;
};
/**
 * @brief 释放page。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 */
FreePageReply freePage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                       FreePageRequest& req);


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

struct GetPageRDMARefRequest : public RequestMsg {
    page_id_t page_id;
};
struct GetPageRDMARefReply : public ResponseMsg {
    uintptr_t addr;
    uint32_t rkey;
};
/**
 * @brief 获取page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @return GetPageRDMARefReply
 */
GetPageRDMARefReply getPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   GetPageRDMARefRequest& req);

struct DelPageRDMARefRequest : public RequestMsg {
    page_id_t page_id;
};
struct DelPageRDMARefReply : public ResponseMsg {
    bool isDel;
};
/**
 * @brief 删除page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @return DelPageRDMARefReply
 */
DelPageRDMARefReply delPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   DelPageRDMARefRequest& req);

struct TryMigratePageRequest : public RequestMsg {
    page_id_t page_id;
    page_id_t swap_page_id;
    uint64_t hot_score;
    uintptr_t swapout_page_addr;  // 当swapout_page_addr == 0且swapout_page_rkey == 0时代表不换出页
    uintptr_t swapin_page_addr;
    uint32_t swapout_page_rkey;
    uint32_t swapin_page_rkey;
};
struct TryMigratePageReply : public ResponseMsg {
    bool swaped;
};
/**
 * @brief 
 * 
 * @param daemon_context 
 * @param daemon_connection 
 * @param req 
 * @return TryMigratePageReply 
 */
TryMigratePageReply tryMigratePage(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   TryMigratePageRequest& req);

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

struct __notifyPerfRequest : public RequestMsg {};
struct __notifyPerfReply : public ResponseMsg {};
__notifyPerfReply __notifyPerf(DaemonContext& daemon_context,
                               DaemonToClientConnection& client_connection,
                               __notifyPerfRequest& req);

struct __stopPerfRequest : public RequestMsg {};
struct __stopPerfReply : public ResponseMsg {};
__stopPerfReply __stopPerf(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, __stopPerfRequest& req);

}  // namespace rpc_daemon
