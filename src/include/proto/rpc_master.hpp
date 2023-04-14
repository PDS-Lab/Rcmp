#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "rpc_base.hpp"
#include "utils.hpp"

namespace rpc_master {

struct JoinDaemonRequest : public RequestMsg {
    rack_id_t rack_id;
    bool with_cxl;
    size_t free_page_num;
};
struct JoinDaemonReply : public ResponseMsg {
    mac_id_t daemon_mac_id;
    mac_id_t master_mac_id;
    IPv4String rdma_ipv4;
    uint16_t rdma_port;
};
/**
 * @brief 将daemon加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param daemon_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param req
 * @return JoinDaemonReply
 */
JoinDaemonReply joinDaemon(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection, JoinDaemonRequest& req);

struct JoinClientRequest : public RequestMsg {
    rack_id_t rack_id;
};
struct JoinClientReply : public ResponseMsg {
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
JoinClientReply joinClient(MasterContext& master_context,
                           MasterToClientConnection& client_connection, JoinClientRequest& req);

struct AllocPageRequest : public RequestMsg {
    size_t slab_size;
};
struct AllocPageReply : public ResponseMsg {
    page_id_t page_id;
};
/**
 * @brief
 * 申请一个page。该操作会希望在daemon端调用`allocPageMemory()`进行分配CXL物理地址。如果该daemon已满，本操作会随机向其他daemon发送该函数进行分配，此时原daemon不应将page
 * id加入到Page Table中。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return AllocPageReply 如果page被派到daemon上，则`need_self_alloc_page_memory`为true
 */
AllocPageReply allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                         AllocPageRequest& req);

struct FreePageRequest : public RequestMsg {
    page_id_t page_id;
};
struct FreePageReply : public ResponseMsg {
    bool ret;
};
/**
 * @brief 释放page。该操作需要保证daemon本身持有这个页时才能释放这个page。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 */
FreePageReply freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       FreePageRequest& req);

struct GetRackDaemonByPageIDRequest : public RequestMsg {
    page_id_t page_id;
};
struct GetRackDaemonByPageIDReply : public ResponseMsg {
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
GetRackDaemonByPageIDReply getRackDaemonByPageID(MasterContext& master_context,
                                                 MasterToDaemonConnection& daemon_connection,
                                                 GetRackDaemonByPageIDRequest& req);

struct LatchRemotePageRequest : public RequestMsg {
    page_id_t page_id;
};
struct LatchRemotePageReply : public ResponseMsg {
    rack_id_t dest_rack_id;
    mac_id_t dest_daemon_id;
    IPv4String dest_daemon_ipv4;
    uint16_t dest_daemon_erpc_port;
    IPv4String dest_daemon_rdma_ipv4;
    uint16_t dest_daemon_rdma_port;
};
/**
 * @brief 获取并锁定远端page不被swap
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return LatchRemotePageReply 返回目标daemon的rdma ip与port，以供建立连接
 */
LatchRemotePageReply latchRemotePage(MasterContext& master_context,
                                     MasterToDaemonConnection& daemon_connection,
                                     LatchRemotePageRequest& req);

struct UnLatchRemotePageRequest : public RequestMsg {
    page_id_t page_id;
};
struct UnLatchRemotePageReply : public ResponseMsg {};
/**
 * @brief 解锁远端page
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return UnLatchRemotePageReply 返回目标daemon的rdma ip与port，以供建立连接
 */
UnLatchRemotePageReply unLatchRemotePage(MasterContext& master_context,
                                         MasterToDaemonConnection& daemon_connection,
                                         UnLatchRemotePageRequest& req);

struct UnLatchPageAndBalanceRequest : public RequestMsg {
    page_id_t page_id;
    rack_id_t from_rack_id;
};
struct UnLatchPageAndBalanceReply : public ResponseMsg {};
/**
 * @brief 解锁远端page，并将该page转移至该daemon手中
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return UnLatchPageAndBalanceReply 返回目标daemon的rdma ip与port，以供建立连接
 */
UnLatchPageAndBalanceReply unLatchPageAndBalance(MasterContext& master_context,
                                                 MasterToDaemonConnection& daemon_connection,
                                                 UnLatchPageAndBalanceRequest& req);

}  // namespace rpc_master