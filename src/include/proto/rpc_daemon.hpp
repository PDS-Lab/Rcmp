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
    } type;
    rchms::GAddr gaddr;
    union {
        struct {  // type == READ
            size_t cn_read_size;
        };
        struct {  // type == WRITE
            void* cn_write_buf;
            size_t cn_write_size;
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
    page_id_t page_id;
    // size_t slab_size;
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
/**
 * @brief 在swap区申请一个page物理地址空间，只在需要迁移页面且本地的page区已满时调用
 *
 * @param daemon_context
 * @param req
 * @return void
 */
void allocSwapPageMemory(DaemonContext& daemon_context, AllocPageMemoryRequest& req);

/**
 * @brief 广播有当前page的ref的DN，删除其ref
 *
 * @param daemon_context
 * @param page_id
 * @param page_meta
 * @return void
 */
void delPageRefBroadcast(DaemonContext& daemon_context, page_id_t page_id, PageMetadata* page_meta);

/**
 * @brief 通知当前rack下所有访问过该page的client删除相应的缓存
 *
 * @param daemon_context
 * @param page_id
 * @param page_meta
 * @return void
 */
void delPageCacheBroadcast(DaemonContext& daemon_context, page_id_t page_id, PageMetadata* page_meta);

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
 * @brief 获取page的引用。如果本地Page Table没有该page
 * id，则会触发远程调用。
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @return GetPageRDMARefReply
 */
DelPageRDMARefReply delPageRDMARef(DaemonContext& daemon_context,
                                   DaemonToDaemonConnection& daemon_connection,
                                   DelPageRDMARefRequest& req);

struct TryMigratePageRequest : public RequestMsg {
    page_id_t page_id;
    page_id_t swap_page_id;
    uint64_t score;
    uintptr_t swapout_page_addr;  // 当swapout_page_addr == 0且swapout_page_rkey == 0时代表不换出页
    uintptr_t swapin_page_addr;
    uint32_t swapout_page_rkey;
    uint32_t swapin_page_rkey;
    // std::unique_ptr<SingleAllocator> slab_allocator;

    // TryMigratePageRequest(){}
    // TryMigratePageRequest(SingleAllocator &s_allocator) : slab_allocator(s_allocator) {}
};
struct TryMigratePageReply : public ResponseMsg {
    bool swaped;
    // SingleAllocator slab_allocator;
    // std::unique_ptr<SingleAllocator> slab_allocator;
    // TryMigratePageReply() {}
    // TryMigratePageReply(SingleAllocator &s_allocator) : slab_allocator(s_allocator) {}
};
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
