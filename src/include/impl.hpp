#pragma once

#include <list>
#include <memory>
#include <unordered_set>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"

/************************  Master   **********************/

struct PageRackMetadata {
    uint32_t rack_id;
};

struct MasterConnection {
    std::string ip;
    uint16_t port;

    virtual ~MasterConnection() = default;
};

struct MasterToClientConnection : public MasterConnection {
    rack_id_t rack_id;
    mac_id_t client_id;
};

struct MasterToDaemonConnection : public MasterConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct RackMacTable {
    bool with_cxl;
    MasterToDaemonConnection *daemon_connect;
    size_t max_free_page_num;
    size_t current_allocated_page_num;
    std::vector<MasterToClientConnection *> client_connect_table;
};

struct ClusterManager {
    ConcurrentHashMap<rack_id_t, RackMacTable *> cluster_rack_table;
    ConcurrentHashMap<mac_id_t, MasterConnection *> connect_table;
    std::unique_ptr<IDGenerator> mac_id_allocator;
};

struct MasterContext {
    rchms::MasterOptions options;

    mac_id_t master_id;  // 节点id，由master分配

    ClusterManager cluster_manager;

    std::unique_ptr<IDGenerator> page_id_allocator;
    ConcurrentHashMap<page_id_t, PageRackMetadata *> page_directory;

    struct {
        volatile bool running;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } erpc_ctx;

    MasterContext() = default;
    MasterContext(MasterContext &) = delete;
    MasterContext(MasterContext &&) = delete;
    MasterContext &operator=(MasterContext &) = delete;
    MasterContext &operator=(MasterContext &&) = delete;

    static MasterContext &getInstance();

    MasterConnection *get_connection(mac_id_t mac_id);
    erpc::IBRpcWrap get_erpc();
};

/************************  Daemon   **********************/

struct DaemonConnection {
    std::string ip;
    uint16_t port;

    virtual ~DaemonConnection() = default;
};

struct DaemonToMasterConnection : public DaemonConnection {
    mac_id_t master_id;
};

struct DaemonToClientConnection : public DaemonConnection {
    MsgQueue send_msg_queue;  // 发给client的msg queue
    MsgQueue recv_msg_queue;  // 接收client消息的msg queue

    mac_id_t client_id;
};

struct DaemonToDaemonConnection : public DaemonConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct PageMetadata {
    offset_t cxl_memory_offset;
    Allocator slab_allocator;
    std::unordered_set<DaemonToClientConnection *> ref_client;

    PageMetadata(size_t slab_size);
};

struct DaemonContext {
    rchms::DaemonOptions options;

    mac_id_t daemon_id;  // 节点id，由master分配

    int cxl_devdax_fd;
    void *cxl_memory_addr;

    size_t total_page_num;     // 所有page的个数
    size_t max_swap_page_num;  // swap区的page个数
    size_t max_data_page_num;  // 所有可用数据页个数

    size_t current_used_page_num;       // 当前使用的数据页个数
    size_t current_used_swap_page_num;  // 当前正在swap的页个数

    DaemonToMasterConnection master_connection;
    std::unique_ptr<Allocator> cxl_msg_queue_allocator;
    std::vector<DaemonToClientConnection *> client_connect_table;
    std::vector<DaemonToDaemonConnection *> other_daemon_connect_table;
    ConcurrentHashMap<mac_id_t, DaemonConnection *> connect_table;
    std::unique_ptr<Allocator> cxl_page_allocator;
    ConcurrentHashMap<page_id_t, PageMetadata *> page_table;

    std::array<std::list<page_id_t>, page_size / min_slab_size> can_alloc_slab_class_lists;

    struct {
        volatile bool running;
        int master_session;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } erpc_ctx;

    DaemonContext() = default;
    DaemonContext(DaemonContext &) = delete;
    DaemonContext(DaemonContext &&) = delete;
    DaemonContext &operator=(DaemonContext &) = delete;
    DaemonContext &operator=(DaemonContext &&) = delete;

    static DaemonContext &getInstance();
    DaemonConnection *get_connection(mac_id_t mac_id);
    erpc::IBRpcWrap get_erpc();
};

/************************  Client   **********************/

struct ClientConnection {
    std::string ip;
    uint16_t port;

    virtual ~ClientConnection() = default;
};

struct ClientToMasterConnection : public ClientConnection {
    mac_id_t master_id;
};

struct ClientToDaemonConnection : public ClientConnection {
    MsgQueue send_msg_queue;  // 发给daemon的msg queue
    MsgQueue recv_msg_queue;  // 接收daemon消息的msg queue

    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct ClientContext {
    rchms::ClientOptions options;

    std::string ip;
    uint16_t port;

    mac_id_t client_id;

    int cxl_devdax_fd;
    void *cxl_memory_addr;

    ClientToMasterConnection master_connection;
    ClientToDaemonConnection local_rack_daemon_connection;
    ConcurrentHashMap<mac_id_t, ClientToDaemonConnection *> other_rack_daemon_connection;
    ConcurrentHashMap<page_id_t, offset_t> page_table_cache;

    ClientConnection *get_connection(mac_id_t mac_id);
};

struct rchms::PoolContext::PoolContextImpl : public ClientContext {};
