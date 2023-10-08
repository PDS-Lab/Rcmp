#pragma once

#include "cxl.hpp"
#include "fiber_pool.hpp"
#include "msg_queue.hpp"
#include "page_table.hpp"
#include "proto/rpc_adaptor.hpp"
#include "rdma_rc.hpp"
#include "udp_server.hpp"

struct SysStatistics {
    uint64_t local_hit = 0;
    uint64_t local_miss = 0;
    uint64_t local_cache_search_time = 0;
    uint64_t local_cache_update_time = 0;
    uint64_t local_cache_fault_time = 0;

    uint64_t page_hit = 0;
    uint64_t page_miss = 0;
    uint64_t page_dio = 0;
    uint64_t page_swap = 0;

    uint64_t rpc_opn = 0;
    uint64_t rpc_exec_time = 0;

    uint64_t read_io = 0;
    uint64_t cxl_read_byte = 0;
    uint64_t cxl_read_time = 0;
    uint64_t write_io = 0;
    uint64_t cxl_write_byte = 0;
    uint64_t cxl_write_time = 0;

    uint64_t read_time = 0;
    uint64_t write_time = 0;
};

/************************  Master   **********************/

struct MasterConnection {
    virtual ~MasterConnection() = default;

    virtual msgq::MsgQueueRPC *GetMsgQ() = delete;
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

    std::unique_ptr<ErpcClient> erpc_conn;
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn = nullptr;
};

struct ClusterManager {
    ConcurrentHashMap<rack_id_t, RackMacTable *, CortSharedMutex> cluster_rack_table;
    ConcurrentHashMap<mac_id_t, MasterConnection *, CortSharedMutex> connect_table;
    std::unique_ptr<IDGenerator> mac_id_allocator;
};

struct MasterContext : public NOCOPYABLE {
    rchms::MasterOptions m_options;

    mac_id_t m_master_id;  // 节点id，由master分配

    ClusterManager m_cluster_manager;
    PageDirectory m_page_directory;

    FiberPool m_fiber_pool_;

    struct {
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

    rdma_rc::RDMAConnection m_listen_conn;

    SysStatistics m_stats;

    static MasterContext &getInstance() {
        static MasterContext master_ctx;
        return master_ctx;
    }

    mac_id_t GetMacID() const { return m_master_id; }

    MasterConnection *GetConnection(mac_id_t mac_id) {
        DLOG_ASSERT(mac_id != m_master_id, "Can't find self connection");
        auto it = m_cluster_manager.connect_table.find(mac_id);
        DLOG_ASSERT(it != m_cluster_manager.connect_table.end(), "Can't find mac %d", mac_id);
        return it->second;
    }

    erpc::IBRpcWrap &GetErpc() { return m_erpc_ctx.rpc_set[0]; }

    FiberPool &GetFiberPool() { return m_fiber_pool_; }

    void InitCluster();
    void InitRDMARC();
    void InitRPCNexus();
    void InitFiberPool();
};

/************************  Daemon   **********************/

struct DaemonConnection {
    std::string ip;
    uint16_t port;  // erpc port

    virtual ~DaemonConnection() = default;

    virtual msgq::MsgQueueRPC *GetMsgQ() = 0;
};

struct DaemonToMasterConnection : public DaemonConnection {
    mac_id_t master_id;

    std::unique_ptr<ErpcClient> erpc_conn;
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return nullptr; }
};

struct DaemonToClientConnection : public DaemonConnection {
    std::unique_ptr<MsgQClient> msgq_conn;

    mac_id_t client_id;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return &msgq_conn->rpc; }
};

struct DaemonToDaemonConnection : public DaemonConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;

    std::unique_ptr<ErpcClient> erpc_conn;
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn = nullptr;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return nullptr; }
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
    PageTableManager m_page_table;

    rdma_rc::RDMAConnection m_listen_conn;
    std::vector<ibv_mr *> m_rdma_page_mr_table;  // 为cxl注册的mr，初始化长度后不可更改
    std::unordered_map<void *, ibv_mr *> m_rdma_mr_table;

    FiberPool m_fiber_pool;

    struct {
        volatile bool running;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } m_erpc_ctx;

    SysStatistics m_stats;

    static DaemonContext &getInstance() {
        static DaemonContext daemon_ctx;
        return daemon_ctx;
    }

    mac_id_t GetMacID() const { return m_daemon_id; }

    erpc::IBRpcWrap &GetErpc() { return m_erpc_ctx.rpc_set[0]; }

    DaemonConnection *GetConnection(mac_id_t mac_id) {
        return m_conn_manager.GetConnection(mac_id);
    }

    FiberPool &GetFiberPool() { return m_fiber_pool; }

    ibv_mr *GetMR(void *p) {
        if (p >= m_cxl_format.start_addr && p < m_cxl_format.end_addr) {
            return m_rdma_page_mr_table[(reinterpret_cast<uintptr_t>(p) -
                                         reinterpret_cast<const uintptr_t>(
                                             m_cxl_format.start_addr)) /
                                        mem_region_aligned_size];
        } else {
            return m_rdma_mr_table[reinterpret_cast<void *>(
                align_floor(reinterpret_cast<uintptr_t>(p), mem_region_aligned_size))];
        }
    }

    uintptr_t GetVirtualAddr(offset_t offset) const {
        return reinterpret_cast<uintptr_t>(m_cxl_format.page_data_start_addr) + offset;
    }

    void InitCXLPool();
    void InitRPCNexus();
    void InitRDMARC();
    void InitFiberPool();
    void ConnectWithMaster();
    void RegisterCXLMR();
    void RDMARCPoll();
};

/************************  Client   **********************/

struct ClientConnection {
    virtual ~ClientConnection() = default;

    virtual msgq::MsgQueueRPC *GetMsgQ() = 0;
};

struct ClientToMasterConnection : public ClientConnection {
    mac_id_t master_id;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return nullptr; }
};

struct ClientToDaemonConnection : public ClientConnection {
    std::unique_ptr<MsgQClient> msgq_conn;

    rack_id_t rack_id;
    mac_id_t daemon_id;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return &msgq_conn->rpc; }
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

    PageThreadCacheManager m_tcache_mgr;

    FiberPool m_fiber_pool_;

    volatile bool m_msgq_stop;
    std::thread m_msgq_worker;

    // char *m_batch_buffer = new char[write_batch_buffer_size + write_batch_buffer_overflow_size];
    // size_t m_batch_cur = 0;
    // std::vector<std::tuple<rchms::GAddr, size_t, offset_t>> m_batch_list;
    // std::thread batch_flush_worker;

    SysStatistics m_stats;

    mac_id_t GetMacID() const { return m_client_id; }

    ClientConnection *GetConnection(mac_id_t mac_id) {
        DLOG_ASSERT(mac_id != m_client_id, "Can't find self connection");
        if (mac_id == m_local_rack_daemon_connection.daemon_id) {
            return &m_local_rack_daemon_connection;
        }
        DLOG_FATAL("Can't find mac %d", mac_id);
    }

    FiberPool &GetFiberPool() { return m_fiber_pool_; }

    uintptr_t GetVirtualAddr(offset_t offset) const {
        return reinterpret_cast<uintptr_t>(m_cxl_format.page_data_start_addr) + offset;
    }

    void InitCXLPool();
    void InitRPCNexus();
    void InitFiberPool();
    void ConnectWithDaemon();
    void InitMsgQPooller();
};

struct rchms::PoolContext::PoolContextImpl : public ClientContext {};
