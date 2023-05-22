#pragma once

#include <infiniband/verbs.h>

#include <list>
#include <memory>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "config.hpp"
#include "cxl.hpp"
#include "eRPC/erpc.h"
#include "lock.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"
#include "proto/rpc_adaptor.hpp"
#include "rchms.hpp"
#include "rdma_rc.hpp"
#include "stats.hpp"
#include "udp_server.hpp"
#include "utils.hpp"

/************************  Master   **********************/

struct PageRackMetadata {
    uint32_t rack_id;
    mac_id_t daemon_id;
    CortSharedMutex latch;
};

struct MasterConnection {
    std::unique_ptr<ErpcClient> erpc_conn;

    virtual ~MasterConnection() = default;
};

struct MasterToClientConnection : public MasterConnection {
    rack_id_t rack_id;
    mac_id_t client_id;
};

struct MasterToDaemonConnection : public MasterConnection {
    std::string ip;
    uint16_t port;  // erpc port
    rack_id_t rack_id;
    mac_id_t daemon_id;

    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn;
};

struct RackMacTable {
    size_t GetCurrentAllocatedPageNum() const { return current_allocated_page_num; }
    size_t GetMaxFreePageNum() const { return max_free_page_num; }

    bool with_cxl;
    MasterToDaemonConnection *daemon_connect;
    size_t max_free_page_num;
    size_t current_allocated_page_num;
    std::vector<MasterToClientConnection *> client_connect_table;
};

struct ClusterManager {
    ConcurrentHashMap<rack_id_t, RackMacTable *, CortSharedMutex> cluster_rack_table;
    ConcurrentHashMap<mac_id_t, MasterConnection *, CortSharedMutex> connect_table;
    std::unique_ptr<IDGenerator> mac_id_allocator;
};

struct PageDirectory {
    PageRackMetadata *FindPage(page_id_t page_id) {
        return table[page_id];
    }

    void AddPage(RackMacTable *rack_table, page_id_t page_id) {
        PageRackMetadata *page_meta = new PageRackMetadata();
        page_meta->rack_id = rack_table->daemon_connect->rack_id;
        page_meta->daemon_id = rack_table->daemon_connect->daemon_id;
        table.insert(page_id, page_meta);
        rack_table->current_allocated_page_num++;
    }

    void RemovePage(RackMacTable *rack_table, page_id_t page_id) {
        auto it = table.find(page_id);
        PageRackMetadata* page_meta = it->second;
        table.erase(it);
        delete page_meta;
        rack_table->current_allocated_page_num--;
    }

    ConcurrentHashMap<page_id_t, PageRackMetadata *, CortSharedMutex> table;
};

struct MasterContext : public NOCOPYABLE {
    rchms::MasterOptions m_options;

    mac_id_t m_master_id;  // 节点id，由master分配

    ClusterManager m_cluster_manager;

    PageDirectory m_page_directory;
    std::unique_ptr<IDGenerator> m_page_id_allocator;

    struct {
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

    rdma_rc::RDMAConnection m_listen_conn;

    struct {
        uint64_t page_swap = 0;
    } m_stats;

    MasterContext();

    static MasterContext &getInstance();

    void initCluster();
    void initRDMARC();
    void initRPCNexus();
    void initCoroutinePool();

    mac_id_t GetMacID() const { return m_master_id; }
    MasterConnection *GetConnection(mac_id_t mac_id);
    erpc::IBRpcWrap &GetErpc() { return m_erpc_ctx.rpc_set[0]; }
};

/************************  Daemon   **********************/

struct DaemonConnection {
    std::string ip;
    uint16_t port;  // erpc port

    virtual ~DaemonConnection() = default;
};

struct DaemonToMasterConnection : public DaemonConnection {
    mac_id_t master_id;

    std::unique_ptr<ErpcClient> erpc_conn;
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn;
};

struct DaemonToClientConnection : public DaemonConnection {
    msgq::MsgQueueRPC *msgq_rpc;

    mac_id_t client_id;
};

struct DaemonToDaemonConnection : public DaemonConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;

    std::unique_ptr<ErpcClient> erpc_conn;
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn;
};

struct PageMetadata {
    std::atomic<uint8_t> status{0};
    offset_t cxl_memory_offset;  // 相对于format.page_data_start_addr
    std::set<DaemonToClientConnection *> ref_client;
    std::set<DaemonToDaemonConnection *> ref_daemon;
};

struct RemotePageMetaCache {
    FreqStats stats;
    uintptr_t remote_page_addr;
    uint32_t remote_page_rkey;
    DaemonToDaemonConnection *remote_page_daemon_conn;

    RemotePageMetaCache(size_t max_recent_record, float hot_decay_lambda);
};

struct MsgQueueManager {
    const static size_t RING_ELEM_SIZE = sizeof(msgq::MsgQueue);

    void *start_addr;
    uint32_t ring_cnt;
    std::unique_ptr<SingleAllocator<RING_ELEM_SIZE>> msgq_allocator;
    std::unique_ptr<msgq::MsgQueueNexus> nexus;
    std::unique_ptr<msgq::MsgQueueRPC> rpc;

    msgq::MsgQueue *allocQueue();
    void freeQueue(msgq::MsgQueue *msgq);
};

struct PageTableManager {
    size_t total_page_num;     // 所有page的个数
    size_t max_swap_page_num;  // swap区的page个数
    size_t max_data_page_num;  // 所有可用数据页个数

    std::atomic<size_t> current_used_page_num;  // 当前使用的数据页个数

    ConcurrentHashMap<page_id_t, PageMetadata *, CortSharedMutex> table;
    std::unique_ptr<SingleAllocator<page_size>> page_allocator;
};

struct ConnectionManager {
    DaemonToMasterConnection &GetMasterConnection() { return m_master_connection; }

    DaemonConnection *GetConnection(mac_id_t mac_id) {
        if (mac_id == master_id) {
            return &GetMasterConnection();
        }
        auto it = m_connect_table.find(mac_id);
        DLOG_ASSERT(it != m_connect_table.end(), "Can't find mac %d", mac_id);
        return it->second;
    }

    void AddConnection(mac_id_t mac_id, DaemonToClientConnection *conn) {
        m_connect_table.insert({mac_id, conn});
        m_client_connect_table.insert(conn);
    }

    void AddConnection(mac_id_t mac_id, DaemonToDaemonConnection *conn) {
        m_connect_table.insert({mac_id, conn});
        m_other_daemon_connect_table.insert(conn);
    }

    DaemonToMasterConnection m_master_connection;
    std::set<DaemonToClientConnection *> m_client_connect_table;
    std::set<DaemonToDaemonConnection *> m_other_daemon_connect_table;
    std::unordered_map<mac_id_t, DaemonConnection *> m_connect_table;
};

struct DaemonContext : public NOCOPYABLE {
    rchms::DaemonOptions m_options;

    mac_id_t m_daemon_id;  // 节点id，由master分配

    int m_cxl_devdax_fd;
    void *m_cxl_memory_addr;

    CXLMemFormat m_cxl_format;
    MsgQueueManager m_msgq_manager;
    ConnectionManager m_conn_manager;
    ConcurrentHashMap<page_id_t, RemotePageMetaCache *, CortSharedMutex> m_hot_stats;
    ConcurrentHashMap<page_id_t, CortSharedMutex *, CortSharedMutex> m_page_ref_lock;
    PageTableManager m_page_table;

    rdma_rc::RDMAConnection m_listen_conn;
    std::vector<ibv_mr *> m_rdma_page_mr_table;  // 为cxl注册的mr，初始化长度后不可更改
    std::unordered_map<void *, ibv_mr *> m_rdma_mr_table;

    struct {
        volatile bool running;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

    struct {
        uint64_t page_hit = 0;
        uint64_t page_miss = 0;
        uint64_t page_dio = 0;
        uint64_t page_swap = 0;
    } m_stats;

    static DaemonContext &getInstance();

    mac_id_t GetMacID() const { return m_daemon_id; }

    erpc::IBRpcWrap &GetErpc() { return m_erpc_ctx.rpc_set[0]; }

    DaemonConnection *GetConnection(mac_id_t mac_id) {
        return m_conn_manager.GetConnection(mac_id);
    }

    ibv_mr *GetMR(void *p);

    void InitCXLPool();
    void InitRPCNexus();
    void InitRDMARC();
    void ConnectWithMaster();
    void RegisterCXLMR();
};

/************************  Client   **********************/

struct ClientConnection {
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

struct LocalPageCache {
    FreqStats stats;
    offset_t offset;

    LocalPageCache(size_t max_recent_record)
        : stats(max_recent_record, 0, hot_stat_freq_timeout_interval) {}
};

struct ClientContext : public NOCOPYABLE {
    rchms::ClientOptions m_options;

    mac_id_t m_client_id;

    int m_cxl_devdax_fd;
    void *m_cxl_memory_addr;

    CXLMemFormat m_cxl_format;
    std::unique_ptr<UDPServer<msgq::MsgUDPConnPacket>> m_udp_conn_recver;
    std::unique_ptr<msgq::MsgQueueRPC> m_msgq_rpc;
    std::unique_ptr<msgq::MsgQueueNexus> m_msgq_nexus;
    ClientToDaemonConnection m_local_rack_daemon_connection;
    std::unordered_set<page_id_t> m_page;
    ConcurrentHashMap<page_id_t, LocalPageCache *> m_page_table_cache;
    ConcurrentHashMap<page_id_t, SharedMutex *> m_ptl_cache_lock;

    volatile bool m_msgq_stop;
    std::thread m_msgq_worker;

    char *m_batch_buffer = new char[write_batch_buffer_size + write_batch_buffer_overflow_size];
    size_t m_batch_cur = 0;
    std::vector<std::tuple<rchms::GAddr, size_t, offset_t>> m_batch_list;
    std::thread batch_flush_worker;

    struct {
        uint64_t local_hit = 0;
        uint64_t local_miss = 0;
    } m_stats;

    ClientContext();

    mac_id_t GetMacID() const;
    ClientConnection *GetConnection(mac_id_t mac_id);
};

struct rchms::PoolContext::PoolContextImpl : public ClientContext {};
