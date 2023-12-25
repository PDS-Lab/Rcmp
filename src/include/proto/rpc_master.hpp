#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "proto/rpc_adaptor.hpp"
#include "utils.hpp"

namespace rpc_master {

struct JoinDaemonRequest {
    mac_id_t mac_id;  // unused
    IPv4String ip;
    uint16_t port;
    rack_id_t rack_id;
    bool with_cxl;
    size_t free_page_num;
};
struct JoinDaemonReply {
    mac_id_t daemon_mac_id;
    mac_id_t master_mac_id;
    uint16_t rdma_port;

    struct RackInfo {
        rack_id_t rack_id;
        mac_id_t daemon_id;
        IPv4String daemon_ipv4;
        uint16_t daemon_erpc_port;
        uint16_t daemon_rdma_port;
    };

    size_t other_rack_count;
    RackInfo other_rack_infos[0];
};
/**
 * @brief Adds the daemon to the cluster. Called when a connection is established.
 *
 * @param master_context
 * @param daemon_connection It needs to be dereferenced to the object requested from the heap, after
 * which its lifecycle will be maintained by the MasterContext.
 * @param req
 * @param resp_handle
 */
void joinDaemon(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                JoinDaemonRequest& req, ResponseHandle<JoinDaemonReply>& resp_handle);

struct JoinClientRequest {
    mac_id_t mac_id;  // unused
    rack_id_t rack_id;
};
struct JoinClientReply {
    mac_id_t mac_id;
};
/**
 * @brief Adds the client to the cluster. Called when a connection is established.
 *
 * @param master_context
 * @param client_connection It needs to be dereferenced to the object requested from the heap, after
 * which its lifecycle will be maintained by the MasterContext.
 * @param req
 * @param resp_handle
 */
void joinClient(MasterContext& master_context, MasterToClientConnection& client_connection,
                JoinClientRequest& req, ResponseHandle<JoinClientReply>& resp_handle);

struct AllocPageRequest {
    mac_id_t mac_id;
    size_t count;
};
struct AllocPageReply {
    page_id_t current_start_page_id;  // Allocated start page id
    size_t current_page_count;        // Number actually allocated in the requesting rack
    page_id_t other_start_page_id;
    size_t other_page_count;
};
/**
 * @brief
 * Allocate a page. this operation will expect a call to `allocPageMemory()` on the daemon side to
 * allocate the CXL physical address. If the daemon is full, this operation will randomly send this
 * function to other daemons for allocation.
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
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
void freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
              FreePageRequest& req, ResponseHandle<FreePageReply>& resp_handle);

struct GetRackDaemonByPageIDRequest {
    page_id_t page_id;
};
struct GetRackDaemonByPageIDReply {
    IPv4String dest_daemon_ipv4;
    uint16_t dest_daemon_port;
    rack_id_t rack_id;
};
/**
 * @brief Get the IPv4 address of the daemon corresponding to rack based on the page id. This call
 * should be used in the remote direct io case of the daemon.
 *
 * @param master_context
 * @param client_connection
 * @param req
 * @param resp_handle
 */
void getRackDaemonByPageID(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection,
                           GetRackDaemonByPageIDRequest& req,
                           ResponseHandle<GetRackDaemonByPageIDReply>& resp_handle);

struct LatchRemotePageRequest {
    mac_id_t mac_id;
    bool exclusive;
    page_id_t page_id;
};
struct LatchRemotePageReply {
    rack_id_t dest_rack_id;
    mac_id_t dest_daemon_id;
};
/**
 * @brief Get and latch the remote page from being swapped.
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void latchRemotePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                     LatchRemotePageRequest& req,
                     ResponseHandle<LatchRemotePageReply>& resp_handle);

struct UnLatchRemotePageRequest {
    mac_id_t mac_id;
    bool exclusive;
    page_id_t page_id;
};
struct UnLatchRemotePageReply {
    bool ret;
};
/**
 * @brief Unlatch remote page
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void unLatchRemotePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       UnLatchRemotePageRequest& req,
                       ResponseHandle<UnLatchRemotePageReply>& resp_handle);

struct tryMigratePageRequest {
    mac_id_t mac_id;
    bool exclusive;
    page_id_t page_id;
    float page_heat;
    page_id_t page_id_swap;
};
struct tryMigratePageReply {
    bool ret;
};
/**
 * @brief Try migrate page, if success, all pages will locked. You need call `MigratePageDone` when
 * migrating is done.
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void tryMigratePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                    tryMigratePageRequest& req, ResponseHandle<tryMigratePageReply>& resp_handle);

struct MigratePageDoneRequest {
    mac_id_t mac_id;
    page_id_t page_id;            // Swapin page (originally at the far end)
    mac_id_t new_daemon_id;       // Your own daemon id
    rack_id_t new_rack_id;        // Your own rack id
    page_id_t page_id_swap;       // Swapped out page (originally local), if invalid, no swap
    mac_id_t new_daemon_id_swap;  // The peer's daemon id
    rack_id_t new_rack_id_swap;   // The peer's rack id
};
struct MigratePageDoneReply {
    bool ret;
};
/**
 * @brief Unlatch the remote page and transfer the page to this daemon
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @param resp_handle
 */
void MigratePageDone(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                     MigratePageDoneRequest& req,
                     ResponseHandle<MigratePageDoneReply>& resp_handle);

}  // namespace rpc_master