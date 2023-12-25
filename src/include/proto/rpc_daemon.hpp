#pragma once

#include <cstdint>

#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc_adaptor.hpp"
#include "rcmp.hpp"
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
    float half_life_us;
};
/**
 * @brief Adds client to the rack. Called when the connection is established.
 *
 * @param daemon_context
 * @param client_connection It needs to be dereferenced to the object requested from the heap, after
 * which its lifecycle will be maintained by the MasterContext.
 * @param req
 * @param resp_handle
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
        CAS,
    } type;
    rcmp::GAddr gaddr;
    uint32_t hint_version;
    uint64_t hint;
    union {
        struct {  // type == WRITE
            size_t cn_write_size;
            const void* cn_write_buf;
        } write;
        struct {  // type == READ
            size_t cn_read_size;
        } read;
        struct {  // type == WRITE_RAW
            size_t cn_write_raw_size;
            uint8_t cn_write_raw_buf[0];
        } write_raw;
        struct {  // type == CAS
            size_t expected;
            size_t desired;
        } cas;
    } u;
};
struct GetPageCXLRefOrProxyReply {
    bool refs;
    uint32_t hint_version;
    uint64_t hint;
    union {
        struct {  // refs == true
            offset_t offset;
        };
        struct {      // refs == false
            struct {  // cas
                uint64_t old_val;
            };
            struct {  // read
                uint8_t read_data[0];
            };
        };
    };
};
/**
 * @brief Get a reference to the page. If the local Page Table does not have that page id, a remote
 * io is triggered.
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @param resp_handle
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
 * @brief Allocate a page physical address space
 *
 * @param daemon_context
 * @param master_connection
 * @param req
 * @param resp_handle
 */
void allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     AllocPageMemoryRequest& req,
                     ResponseHandle<AllocPageMemoryReply>& resp_handle);

struct AllocRequest {
    mac_id_t mac_id;
    size_t size;
};
struct AllocReply {
    rcmp::GAddr gaddr;
};
void alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
           AllocRequest& req, ResponseHandle<AllocReply>& resp_handle);
struct AllocPageRequest {
    mac_id_t mac_id;
    size_t count;
};
struct AllocPageReply {
    page_id_t start_page_id;  // Allocated start page id
    size_t start_count;       // Number actually allocated in the requesting rack
};
/**
 * @brief Allocate for a page
 *
 * @param daemon_context
 * @param client_connection
 * @param req
 * @param resp_handle
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
 * @brief Free a page
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void freePage(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
              FreePageRequest& req, ResponseHandle<FreePageReply>& resp_handle);

struct FreeRequest {
    mac_id_t mac_id;
    rcmp::GAddr gaddr;
    size_t n;
};
struct FreeReply {
    bool ret;
};
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
 * @brief Get a reference to the page. If the local Page Table does not have that page id, a remote
 * io is triggered.
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void getPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    GetPageRDMARefRequest& req, ResponseHandle<GetPageRDMARefReply>& resp_handle);

struct DelPageRDMARefRequest {
    mac_id_t mac_id;
    page_id_t page_id;  // Preparing to delete the page id of the ref
};
struct DelPageRDMARefReply {
    bool ret;
};
/**
 * @brief Removes a reference to a page.
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void delPageRDMARef(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                    DelPageRDMARefRequest& req, ResponseHandle<DelPageRDMARefReply>& resp_handle);

struct MigratePageRequest {
    mac_id_t mac_id;
    page_id_t page_id;
    page_id_t swap_page_id;
    uintptr_t swapout_page_addr;  // When `swapout_page_addr == 0` and `swapout_page_rkey == 0`, it
                                  // means no swapout.
    uintptr_t swapin_page_addr;
    uint32_t swapout_page_rkey;
    uint32_t swapin_page_rkey;
};
struct MigratePageReply {
    bool swapped;
};
/**
 * @brief
 *
 * @param daemon_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void migratePage(DaemonContext& daemon_context, DaemonToDaemonConnection& daemon_connection,
                 MigratePageRequest& req, ResponseHandle<MigratePageReply>& resp_handle);

struct TryDelPageRequest {
    mac_id_t mac_id;
    page_id_t page_id;
};
struct TryDelPageReply {
    bool ret;
};
void tryDelPage(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                TryDelPageRequest& req, ResponseHandle<TryDelPageReply>& resp_handle);

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
