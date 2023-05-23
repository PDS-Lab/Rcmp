#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "proto/rpc_adaptor.hpp"
#include "utils.hpp"

namespace rpc_master {

struct JoinDaemonRequest {
    IPv4String ip;
    uint16_t port;
    rack_id_t rack_id;
    bool with_cxl;
    size_t free_page_num;
};
struct JoinDaemonReply {
    mac_id_t daemon_mac_id;
    mac_id_t master_mac_id;
    IPv4String rdma_ipv4;
    uint16_t rdma_port;

    struct RackInfo {
        rack_id_t rack_id;
        mac_id_t daemon_id;
        IPv4String daemon_ipv4;
        uint16_t daemon_erpc_port;
        IPv4String daemon_rdma_ipv4;
        uint16_t daemon_rdma_port;
    };

    size_t other_rack_count;
    RackInfo other_rack_infos[0];
};
/**
 * @brief 将daemon加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param daemon_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param req
 * @return JoinDaemonReply
 */
void joinDaemon(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                JoinDaemonRequest& req, ResponseHandle<JoinDaemonReply>& resp_handle);

struct JoinClientRequest {
    rack_id_t rack_id;
};
struct JoinClientReply {
    mac_id_t mac_id;
};
/**
 * @brief 将client加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param client_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param req
 * @return JoinClientReply
 */
void joinClient(MasterContext& master_context, MasterToClientConnection& client_connection,
                JoinClientRequest& req, ResponseHandle<JoinClientReply>& resp_handle);

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
 * 申请一个page。该操作会希望在daemon端调用`allocPageMemory()`进行分配CXL物理地址。
 * 如果该daemon已满，本操作会随机向其他daemon发送该函数进行分配。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return AllocPageReply
 */
void allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
               AllocPageRequest& req, ResponseHandle<AllocPageReply>& resp_handle);

struct FreePageRequest {
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
 * @brief 根据page id获取对应rack的daemon的IPv4地址。该调用应在daemon的`远程直接访问`情况下使用。
 *
 * @param master_context
 * @param client_connection
 * @param req
 * @return GetRackDaemonByPageIDReply
 */
void getRackDaemonByPageID(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection,
                           GetRackDaemonByPageIDRequest& req,
                           ResponseHandle<GetRackDaemonByPageIDReply>& resp_handle);

struct LatchRemotePageRequest {
    mac_id_t mac_id;
    bool isWriteLock;
    page_id_t page_id;
    page_id_t page_id_swap;
};
struct LatchRemotePageReply {
    rack_id_t dest_rack_id;
    mac_id_t dest_daemon_id;
};
/**
 * @brief 获取并锁定远端page不被swap
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return LatchRemotePageReply 返回目标daemon的rdma ip与port，以供建立连接
 */
void latchRemotePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                     LatchRemotePageRequest& req,
                     ResponseHandle<LatchRemotePageReply>& resp_handle);

struct UnLatchRemotePageRequest {
    mac_id_t mac_id;
    page_id_t page_id;
};
struct UnLatchRemotePageReply {};
/**
 * @brief 解锁远端page
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return UnLatchRemotePageReply 返回目标daemon的rdma ip与port，以供建立连接
 */
void unLatchRemotePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       UnLatchRemotePageRequest& req,
                       ResponseHandle<UnLatchRemotePageReply>& resp_handle);

struct UnLatchPageAndSwapRequest {
    mac_id_t mac_id;
    page_id_t page_id;
    mac_id_t new_daemon_id;
    rack_id_t new_rack_id;
    page_id_t page_id_swap;         // invalid 不换出
    mac_id_t new_daemon_id_swap;
    rack_id_t new_rack_id_swap;
};
struct UnLatchPageAndSwapReply {};
/**
 * @brief 解锁远端page，并将该page转移至该daemon手中
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return UnLatchPageAndSwapReply 返回目标daemon的rdma ip与port，以供建立连接
 */
void unLatchPageAndSwap(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                        UnLatchPageAndSwapRequest& req,
                        ResponseHandle<UnLatchPageAndSwapReply>& resp_handle);

}  // namespace rpc_master