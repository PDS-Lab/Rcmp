#pragma once

#include <list>
#include <memory>
#include <thread>
#include <unordered_set>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "cxl.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"
#include "udp_server.hpp"

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
    rchms::MasterOptions m_options;

    mac_id_t m_master_id;  // 节点id，由master分配

    ClusterManager m_cluster_manager;

    ConcurrentHashMap<page_id_t, PageRackMetadata *> m_page_directory;
    std::unique_ptr<IDGenerator> m_page_id_allocator;

    struct {
        volatile bool running;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

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
    msgq::MsgQueueRPC *msgq_rpc;

    mac_id_t client_id;
};

struct DaemonToDaemonConnection : public DaemonConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct PageMetadata {
    offset_t cxl_memory_offset;
    SingleAllocator slab_allocator;
    std::unordered_set<DaemonToClientConnection *> ref_client;

    PageMetadata(size_t slab_size);
};

struct DaemonContext {
    rchms::DaemonOptions m_options;

    mac_id_t m_daemon_id;  // 节点id，由master分配

    int m_cxl_devdax_fd;
    void *m_cxl_memory_addr;

    size_t m_total_page_num;     // 所有page的个数
    size_t m_max_swap_page_num;  // swap区的page个数
    size_t m_max_data_page_num;  // 所有可用数据页个数

    size_t m_current_used_page_num;       // 当前使用的数据页个数
    size_t m_current_used_swap_page_num;  // 当前正在swap的页个数

    CXLMemFormat cxl_format;
    msgq::MsgQueueManager msgq_manager;
    DaemonToMasterConnection m_master_connection;
    std::vector<DaemonToClientConnection *> m_client_connect_table;
    std::vector<DaemonToDaemonConnection *> m_other_daemon_connect_table;
    std::unique_ptr<SingleAllocator> m_cxl_page_allocator;
    ConcurrentHashMap<mac_id_t, DaemonConnection *> m_connect_table;
    ConcurrentHashMap<page_id_t, PageMetadata *> m_page_table;

    std::array<std::list<page_id_t>, page_size / min_slab_size> m_can_alloc_slab_class_lists;

    struct {
        volatile bool running;
        int master_session;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

    DaemonContext() = default;
    DaemonContext(DaemonContext &) = delete;
    DaemonContext(DaemonContext &&) = delete;
    DaemonContext &operator=(DaemonContext &) = delete;
    DaemonContext &operator=(DaemonContext &&) = delete;

    static DaemonContext &getInstance();

    void initCXLPool();
    void initRPCNexus();
    void connectWithMaster();

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
    msgq::MsgQueueRPC *msgq_rpc;

    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct ClientContext {
    rchms::ClientOptions m_options;

    mac_id_t m_client_id;

    int m_cxl_devdax_fd;
    void *m_cxl_memory_addr;

    CXLMemFormat format;
    std::unique_ptr<UDPServer<msgq::MsgUDPConnPacket>> udp_conn_recver;
    std::unique_ptr<msgq::MsgQueueRPC> msgq_rpc;
    std::unique_ptr<msgq::MsgQueueNexus> msgq_nexus;
    ClientToMasterConnection m_master_connection;
    ClientToDaemonConnection m_local_rack_daemon_connection;
    ConcurrentHashMap<mac_id_t, ClientToDaemonConnection *> m_other_rack_daemon_connection;
    ConcurrentHashMap<page_id_t, offset_t> m_page_table_cache;

    ClientConnection *get_connection(mac_id_t mac_id);
};

struct rchms::PoolContext::PoolContextImpl : public ClientContext {};
