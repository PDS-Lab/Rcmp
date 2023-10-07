#pragma once

#include <list>
#include <set>
#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "lock.hpp"
#include "lockmap.hpp"
#include "stats.hpp"


struct MasterToDaemonConnection;
struct MasterToClientConnection;

struct PageRackMetadata {
    uint32_t rack_id;
    mac_id_t daemon_id;
    CortSharedMutex latch;
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

struct PageDirectory {
    PageRackMetadata *FindPage(page_id_t page_id);
    PageRackMetadata *AddPage(RackMacTable *rack_table, page_id_t page_id);
    void RemovePage(RackMacTable *rack_table, page_id_t page_id);

    ConcurrentHashMap<page_id_t, PageRackMetadata *, CortSharedMutex> table;
    std::unique_ptr<IDGenerator> page_id_allocator;
};

struct DaemonToClientConnection;
struct DaemonToDaemonConnection;

struct PageVMMapMetadata {
    bool TryPin() { return !pinned.test_and_set(); }
    void UnPin() { pinned.clear(); }

    std::atomic_flag pinned{false};
    offset_t cxl_memory_offset;  // 相对于format.page_data_start_addr
    std::set<DaemonToClientConnection *> ref_client;
    std::set<DaemonToDaemonConnection *> ref_daemon;
};

struct RemotePageRefMeta {
    uint64_t Update() { return stats.add(getUsTimestamp()); }

    FreqStats stats;
    uintptr_t remote_page_addr;
    uint32_t remote_page_rkey;
    DaemonToDaemonConnection *remote_page_daemon_conn;

    RemotePageRefMeta(size_t max_recent_record, float hot_decay_lambda)
        : stats(max_recent_record, hot_decay_lambda, hot_stat_freq_timeout_interval) {}
};

struct PageMetadata {
    CortSharedMutex page_ref_lock;
    CortMutex remote_ref_lock;
    PageVMMapMetadata *vm_meta = nullptr;
    RemotePageRefMeta *remote_ref_meta = nullptr;
};

struct PageTableManager {
    template <typename F, typename... Args>
    PageMetadata *FindOrCreatePageMeta(page_id_t page_id, F &&fn, Args &&...args) {
        auto p = table.find_or_emplace(page_id, [&]() {
            PageMetadata *page_meta = new PageMetadata();
            fn(page_meta, std::move(args)...);
            return page_meta;
        });
        return p.first->second;
    }

    PageMetadata *FindOrCreatePageMeta(page_id_t page_id) {
        auto p = table.find_or_emplace(page_id, [&]() { return new PageMetadata(); });
        return p.first->second;
    }

    template <typename F, typename... Args>
    RemotePageRefMeta *FindOrCreateRemotePageRefMeta(PageMetadata *page_meta, F &&fn,
                                                     Args &&...args) {
        if (page_meta->remote_ref_meta == nullptr) {
            std::unique_lock<CortMutex> page_remote_ref_lock(page_meta->remote_ref_lock);
            if (page_meta->remote_ref_meta == nullptr) {
                RemotePageRefMeta *remote_ref_meta =
                    new RemotePageRefMeta(max_recent_record, hot_decay_lambda);
                fn(remote_ref_meta, std::move(args)...);
                page_meta->remote_ref_meta = remote_ref_meta;
            }
        }
        return page_meta->remote_ref_meta;
    }

    RemotePageRefMeta *FindOrCreateRemotePageRefMeta(PageMetadata *page_meta) {
        if (page_meta->remote_ref_meta == nullptr) {
            std::unique_lock<CortMutex> page_remote_ref_lock(page_meta->remote_ref_lock);
            if (page_meta->remote_ref_meta == nullptr) {
                page_meta->remote_ref_meta =
                    new RemotePageRefMeta(max_recent_record, hot_decay_lambda);
            }
        }
        return page_meta->remote_ref_meta;
    }

    void EraseRemotePageRefMeta(PageMetadata *page_meta);
    PageVMMapMetadata *AllocPageMemory();
    void ApplyPageMemory(PageMetadata *page_meta, PageVMMapMetadata *page_vm_meta);
    void CancelPageMemory(PageMetadata *page_meta);
    void RandomPickUnvisitVMPage(bool force, bool &ret, page_id_t &page_id,
                                 PageMetadata *&page_meta);

    // TODO: release page meta resource when vm_meta and remote_ref_meta are nullptr

    bool NearlyFull() const { return current_used_page_num == max_data_page_num; }

    bool TestAllocPageMemory(size_t count = 1) const {
        return current_used_page_num + count <= total_page_num;
    }

    size_t GetCurrentUsedPageNum() const { return current_used_page_num; }

    size_t max_recent_record;
    float hot_decay_lambda;

    size_t total_page_num;     // 所有page的个数
    size_t max_swap_page_num;  // swap区的page个数
    size_t max_data_page_num;  // 所有可用数据页个数

    std::atomic<size_t> current_used_page_num;  // 当前使用的数据页个数

    ConcurrentHashMap<page_id_t, PageMetadata *, CortSharedMutex> table;
    std::unique_ptr<SingleAllocator<page_size>> page_allocator;
};

struct LocalPageCache {
    uint64_t Update() { return stats.add(getUsTimestamp()); }

    FreqStats stats;
    offset_t offset;

    LocalPageCache(size_t max_recent_record)
        : stats(max_recent_record, 0, hot_stat_freq_timeout_interval) {}
};

struct PageCacheTable {
    ~PageCacheTable();

    LocalPageCache *FindCache(page_id_t page_id);
    LocalPageCache *AddCache(page_id_t page_id, offset_t offset);
    void RemoveCache(page_id_t page_id);

    std::unordered_map<page_id_t, LocalPageCache *> table;
};

struct PageThreadLocalCache;

struct PageThreadCacheManager {
    void insert(PageThreadLocalCache *tcache);
    void erase(PageThreadLocalCache *tcache);

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

    PageCacheTable page_cache_table;
    LockResourceManager<page_id_t, SharedMutex> ptl_cache_lock;

   private:
    PageThreadCacheManager &mgr;
    PageThreadLocalCache(PageThreadCacheManager &mgr) : mgr(mgr) { mgr.insert(this); }
    ~PageThreadLocalCache() { mgr.erase(this); }
};