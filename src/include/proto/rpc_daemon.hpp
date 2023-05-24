#pragma once

#include <cstdint>

#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc_adaptor.hpp"
#include "rchms.hpp"
#include "utils.hpp"

namespace rpc_daemon {

struct JoinRackRequest {
    mac_id_t mac_id;  // unused
    IPv4String client_ipv4;
    uint16_t client_port;
    rack_id_t rack_id;
};
struct JoinRackReply {
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
void joinRack(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
              JoinRackRequest& req, ResponseHandle<JoinRackReply>& resp_handle);

struct CrossRackConnectRequest {
    mac_id_t mac_id;
    IPv4String ip;
    uint16_t port;
    rack_id_t rack_id;
    mac_id_t conn_mac_id;
};
struct CrossRackConnectReply {
    mac_id_t daemon_mac_id;
    uint16_t rdma_port;
};
void crossRackConnect(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                      CrossRackConnectRequest& req,
                      ResponseHandle<CrossRackConnectReply>& resp_handle);

struct GetPageCXLRefOrProxyRequest {
    mac_id_t mac_id;
    enum {
        READ,
        WRITE,
        WRITE_RAW,
    } type;
    rchms::GAddr gaddr;
    union {
        struct {  // type == WRITE
            size_t cn_write_size;
            void* cn_write_buf;
        } write;
        struct {  // type == READ
            size_t cn_read_size;
        } read;
        struct {  // type == WRITE_RAW
            size_t cn_write_raw_size;
            uint8_t cn_write_raw_buf[0];
        } write_raw;
    } u;
};
struct GetPageCXLRefOrProxyReply {
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
void getPageCXLRefOrProxy(DaemonContext& daemon_context,
                          DaemonToClientConnection& client_connection,
                          GetPageCXLRefOrProxyRequest& req,
                          ResponseHandle<GetPageCXLRefOrProxyReply>& resp_handle);

struct AllocPageMemoryRequest {
    mac_id_t mac_id;
    page_id_t start_page_id;
    size_t count;
};
struct AllocPageMemoryReply {
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
void allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     AllocPageMemoryRequest& req,
                     ResponseHandle<AllocPageMemoryReply>& resp_handle);

struct AllocRequest {
    mac_id_t mac_id;
    size_t size;
};
struct AllocReply {
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
void alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
           AllocRequest& req, ResponseHandle<AllocReply>& resp_handle);
struct AllocPageRequest {
    mac_id_t mac_id;
    size_t count;
};
struct AllocPageReply {
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
void allocPage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               AllocPageRequest& req, ResponseHandle<AllocPageReply>& resp_handle);

struct FreePageRequest {
    mac_id_t mac_id;
    page_id_t start_page_id;
    size_t count;
};
struct FreePageReply {
    bool ret;
};
/**
 * @brief 释放page。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 */
void freePage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
              FreePageRequest& req, ResponseHandle<FreePageReply>& resp_handle);

struct FreeRequest {
    mac_id_t mac_id;
    rchms::GAddr gaddr;
    size_t n;
};
struct FreeReply {
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
void free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
          FreeRequest& req, ResponseHandle<FreeReply>& resp_handle);

struct GetPageRDMARefRequest {
    mac_id_t mac_id;
    page_id_t page_id;
};
struct GetPageRDMARefReply {
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
void getPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    GetPageRDMARefRequest& req, ResponseHandle<GetPageRDMARefReply>& resp_handle);

struct DelPageRDMARefRequest {
    mac_id_t mac_id;
    page_id_t page_id;
};
struct DelPageRDMARefReply {
    bool ret;
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
void delPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    DelPageRDMARefRequest& req, ResponseHandle<DelPageRDMARefReply>& resp_handle);

struct TryMigratePageRequest {
    mac_id_t mac_id;
    page_id_t page_id;
    page_id_t swap_page_id;
    uint64_t hot_score;
    uintptr_t swapout_page_addr;  // 当swapout_page_addr == 0且swapout_page_rkey == 0时代表不换出页
    uintptr_t swapin_page_addr;
    uint32_t swapout_page_rkey;
    uint32_t swapin_page_rkey;
};
struct TryMigratePageReply {
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
void tryMigratePage(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    TryMigratePageRequest& req, ResponseHandle<TryMigratePageReply>& resp_handle);

/************************* for test ***************************/

struct __TestDataSend1Request {
    mac_id_t mac_id;
    size_t size;
    int data[64];
};
struct __TestDataSend1Reply {
    size_t size;
    int data[64];
};

struct __TestDataSend2Request {
    mac_id_t mac_id;
    size_t size;
    int data[72];
};
struct __TestDataSend2Reply {
    size_t size;
    int data[72];
};

void __testdataSend1(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                     __TestDataSend1Request& req,
                     ResponseHandle<__TestDataSend1Reply>& resp_handle);

void __testdataSend2(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                     __TestDataSend2Request& req,
                     ResponseHandle<__TestDataSend2Reply>& resp_handle);

struct __notifyPerfRequest {
    mac_id_t mac_id;
};
struct __notifyPerfReply {};
void __notifyPerf(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                  __notifyPerfRequest& req, ResponseHandle<__notifyPerfReply>& resp_handle);

struct __stopPerfRequest {
    mac_id_t mac_id;
};
struct __stopPerfReply {};
void __stopPerf(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                __stopPerfRequest& req, ResponseHandle<__stopPerfReply>& resp_handle);

}  // namespace rpc_daemon
