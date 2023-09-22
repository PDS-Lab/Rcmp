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
#include "fiber_pool.hpp"
#include "lock.hpp"
#include "lockmap.hpp"
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
        auto it = table.find(page_id);
        if (it == table.end()) {
            return nullptr;
        }
        return it->second;
    }

    PageRackMetadata *AddPage(RackMacTable *rack_table, page_id_t page_id) {
        PageRackMetadata *page_meta = new PageRackMetadata();
        page_meta->rack_id = rack_table->daemon_connect->rack_id;
        page_meta->daemon_id = rack_table->daemon_connect->daemon_id;
        table.insert(page_id, page_meta);
        rack_table->current_allocated_page_num++;

        DLOG("Add page %lu --> rack %u", page_id, page_meta->rack_id);

        return page_meta;
    }

    void RemovePage(RackMacTable *rack_table, page_id_t page_id) {
        auto it = table.find(page_id);
        PageRackMetadata *page_meta = it->second;
        table.erase(it);
        delete page_meta;
        rack_table->current_allocated_page_num--;
    }

    ConcurrentHashMap<page_id_t, PageRackMetadata *, CortSharedMutex> table;
    std::unique_ptr<IDGenerator> page_id_allocator;
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

    struct {
        uint64_t page_swap = 0;
    } m_stats;

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
    std::unique_ptr<rdma_rc::RDMAConnection> rdma_conn;

    virtual msgq::MsgQueueRPC *GetMsgQ() override { return nullptr; }
};

struct PageMetadata {
    bool TryPin() { return !status.test_and_set(); }

    void UnPin() { status.clear(); }

    std::atomic_flag status{false};
    offset_t cxl_memory_offset;  // 相对于format.page_data_start_addr
    std::set<DaemonToClientConnection *> ref_client;
    std::set<DaemonToDaemonConnection *> ref_daemon;
};

struct RemotePageMetaCache {
    uint64_t Update() { return stats.add(getTimestamp()); }

    FreqStats stats;
    uintptr_t remote_page_addr;
    uint32_t remote_page_rkey;
    DaemonToDaemonConnection *remote_page_daemon_conn;

    RemotePageMetaCache(size_t max_recent_record, float hot_decay_lambda)
        : stats(max_recent_record, hot_decay_lambda, hot_stat_freq_timeout_interval) {}
};

struct MsgQueueManager {
    const static size_t RING_ELEM_SIZE = sizeof(msgq::MsgQueue);

    void *start_addr;
    uint32_t ring_cnt;
    std::unique_ptr<SingleAllocator<RING_ELEM_SIZE>> msgq_allocator;
    std::unique_ptr<msgq::MsgQueueNexus> nexus;
    std::unique_ptr<msgq::MsgQueueRPC> rpc;

    msgq::MsgQueue *allocQueue() {
        uintptr_t ring_off = msgq_allocator->allocate(1);
        DLOG_ASSERT(ring_off != -1, "Can't alloc msg queue");
        msgq::MsgQueue *r =
            reinterpret_cast<msgq::MsgQueue *>(reinterpret_cast<uintptr_t>(start_addr) + ring_off);
        new (r) msgq::MsgQueue();
        ring_cnt++;
        return r;
    }

    void freeQueue(msgq::MsgQueue *msgq) { DLOG_FATAL("Not Support"); }
};

struct PageTableManager {
    PageMetadata *AllocPageMemory() {
        DLOG_ASSERT(TestAllocPageMemory(), "Can't allocate more page memory");

        offset_t cxl_memory_offset = page_allocator->allocate(1);
        DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

        PageMetadata *page_metadata = new PageMetadata();
        page_metadata->cxl_memory_offset = cxl_memory_offset;

        // DLOG("new page %ld ---> %#lx", start_page_id + c, cxl_memory_offset);

        return page_metadata;
    }

    void ApplyPageMemory(page_id_t page_id, PageMetadata *page_meta) {
        current_used_page_num++;
        table.insert(page_id, page_meta);
    }

    void CancelPageMemory(page_id_t page_id, PageMetadata *page_meta) {
        page_allocator->deallocate(page_meta->cxl_memory_offset, 1);
        current_used_page_num--;
        table.erase(page_id);
        delete page_meta;
    }

    PageMetadata *FindPageMeta(page_id_t page_id) {
        auto it = table.find(page_id);
        if (it == table.end()) {
            return nullptr;
        }
        return it->second;
    }

    void RandomPickUnvisitPage(bool force, bool &ret, page_id_t &page_id,
                               PageMetadata *&page_meta) {
        thread_local std::mt19937 eng(rand());

        table.random_foreach_all(eng, [&](std::pair<const page_id_t, PageMetadata *> &p) {
            if ((force || p.second->ref_client.empty()) && p.second->TryPin()) {
                page_id = p.first;
                page_meta = p.second;
                ret = true;
                return false;
            }
            return true;
        });
    }

    bool NearlyFull() const { return current_used_page_num == max_data_page_num; }

    bool TestAllocPageMemory(size_t count = 1) const {
        return current_used_page_num + count <= total_page_num;
    }

    size_t GetCurrentUsedPageNum() const { return current_used_page_num; }

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
    LockResourceManager<page_id_t, CortSharedMutex> m_page_ref_lock;
    PageTableManager m_page_table;

    rdma_rc::RDMAConnection m_listen_conn;
    std::vector<ibv_mr *> m_rdma_page_mr_table;  // 为cxl注册的mr，初始化长度后不可更改
    std::unordered_map<void *, ibv_mr *> m_rdma_mr_table;

    FiberPool m_fiber_pool_;

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

    static DaemonContext &getInstance() {
        static DaemonContext daemon_ctx;
        return daemon_ctx;
    }

    mac_id_t GetMacID() const { return m_daemon_id; }

    erpc::IBRpcWrap &GetErpc() { return m_erpc_ctx.rpc_set[0]; }

    DaemonConnection *GetConnection(mac_id_t mac_id) {
        return m_conn_manager.GetConnection(mac_id);
    }

    FiberPool &GetFiberPool() { return m_fiber_pool_; }

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

struct LocalPageCache {
    uint64_t Update() { return stats.add(getTimestamp()); }

    FreqStats stats;
    offset_t offset;

    LocalPageCache(size_t max_recent_record)
        : stats(max_recent_record, 0, hot_stat_freq_timeout_interval) {}
};

struct PageTableCache {
    LocalPageCache *FindCache(page_id_t page_id) {
        auto it = table.find(page_id);
        if (it == table.end()) {
            return nullptr;
        }
        return it->second;
    }

    LocalPageCache *AddCache(page_id_t page_id, offset_t offset) {
        LocalPageCache *page_cache = new LocalPageCache(8);
        page_cache->offset = offset;

        // DLOG("add local page %lu cache", page_id);
        table.insert(page_id, page_cache);

        return page_cache;
    }

    void RemoveCache(page_id_t page_id) {
        auto it = table.find(page_id);
        if (it == table.end()) {
            return;
        }

        LocalPageCache *page_cache = it->second;

        table.erase(it);
        delete page_cache;

        // DLOG("remove local page %lu cache", page_id);
    }

    ~PageTableCache() {
        table.foreach_all([](const std::pair<page_id_t, LocalPageCache *> &p) {
            delete p.second;
            return true;
        });
    }

    ConcurrentHashMap<page_id_t, LocalPageCache *> table;
};

struct PageThreadLocalCache;
struct PageThreadCacheManager {
    void insert(PageThreadLocalCache *tcache) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        tcache_list_.push_back(tcache);
    }

    void erase(PageThreadLocalCache *tcache) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        auto it = std::find(tcache_list_.begin(), tcache_list_.end(), tcache);
        tcache_list_.erase(it);
    }

    template <typename F, typename... Args>
    void foreach_all(F &&fn, Args &&...args) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        for (auto &tcache : tcache_list_) {
            fn(*tcache, std::move(args)...);
        }
    }

    std::shared_mutex mutex_;
    std::list<PageThreadLocalCache *> tcache_list_;
};

class PageThreadLocalCache {
   public:
    static PageThreadLocalCache &getInstance(PageThreadCacheManager &mgr) {
        static thread_local PageThreadLocalCache instance(mgr);
        return instance;
    }

    PageTableCache page_table_cache;
    LockResourceManager<page_id_t, SharedMutex> ptl_cache_lock;

   private:
    PageThreadCacheManager &mgr;
    PageThreadLocalCache(PageThreadCacheManager &mgr) : mgr(mgr) { mgr.insert(this); }
    ~PageThreadLocalCache() { mgr.erase(this); }
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

    struct {
        uint64_t local_hit = 0;
        uint64_t local_miss = 0;
    } m_stats;

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
