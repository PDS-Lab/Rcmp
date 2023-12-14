#pragma once

#include <list>
#include <mutex>
#include <set>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "hashtable.hpp"
#include "lock.hpp"
#include "robin_hood.h"
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
    offset_t cxl_memory_offset;  // Relative to `format.page_data_start_addr`
    std::set<DaemonToClientConnection *> ref_client;
    std::set<DaemonToDaemonConnection *> ref_daemon;
};

struct RemotePageRefMeta {
    int version;
    FreqStats stats;
    uintptr_t remote_page_addr;
    uint32_t remote_page_rkey;
    DaemonToDaemonConnection *remote_page_daemon_conn;

    RemotePageRefMeta(uint64_t half_life_us) : stats(half_life_us), version(rand()) {}

    FreqStats::Heatness UpdateWrite() { return stats.add_wr(rdtsc() / 1e3); }
    FreqStats::Heatness UpdateRead() { return stats.add_rd(rdtsc() / 1e3); }
    void ClearHeat() { stats.clear(); }
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
                RemotePageRefMeta *remote_ref_meta = new RemotePageRefMeta(heat_half_life_us);
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
                page_meta->remote_ref_meta = new RemotePageRefMeta(heat_half_life_us);
            }
        }
        return page_meta->remote_ref_meta;
    }

    void EraseRemotePageRefMeta(PageMetadata *page_meta);
    PageVMMapMetadata *AllocPageMemory();
    void FreePageMemory(PageVMMapMetadata *page_vm_meta);
    void ApplyPageMemory(PageMetadata *page_meta, PageVMMapMetadata *page_vm_meta);
    void CancelPageMemory(PageMetadata *page_meta);
    bool PickUnvisitVMPage(page_id_t &page_id, PageMetadata *&page_meta);
    std::vector<std::pair<page_id_t, PageMetadata *>> RandomPickVMPage(size_t n);

    // TODO: release page meta resource when vm_meta and remote_ref_meta are nullptr

    bool NearlyFull() const { return current_used_page_num == max_data_page_num; }

    bool TestAllocPageMemory(size_t count = 1) const {
        return current_used_page_num + count <= total_page_num;
    }

    size_t GetCurrentUsedPageNum() const { return current_used_page_num; }

    uint64_t heat_half_life_us;

    size_t total_page_num;     // Number of all pages
    size_t max_swap_page_num;  // Number of pages in swap area
    size_t max_data_page_num;  // Number of all available data pages

    std::atomic<size_t> current_used_page_num;  // Number of data pages currently in use

    RandomAccessMap<page_id_t, PageMetadata *, CortSharedMutex> table;
    std::queue<std::pair<page_id_t, PageMetadata *>> unvisited_pages;  // lock unsafe
    std::unique_ptr<SingleAllocator<page_size>> page_allocator;
};

struct LocalPageCache {
    FreqStats::Heatness UpdateWrite() { return stats.add_wr(rdtsc() / 1000); }
    FreqStats::Heatness UpdateRead() { return stats.add_rd(rdtsc() / 1000); }
    FreqStats::Heatness WriteHeat() { return stats.m_wr_heat.heat(rdtsc() / 1000); }
    FreqStats::Heatness ReadHeat() { return stats.m_rd_heat.heat(rdtsc() / 1000); }

    FreqStats stats;
    offset_t offset;

    LocalPageCache(uint64_t half_life_us) : stats(half_life_us) {}
};

struct LocalPageCacheMeta {
    Mutex ref_lock;
    LocalPageCache *cache = nullptr;
};

struct PageCacheTable {
    ~PageCacheTable();

    LocalPageCacheMeta *FindOrCreateCacheMeta(page_id_t page_id);
    LocalPageCache *FindCache(page_id_t page_id);
    LocalPageCache *FindCache(LocalPageCacheMeta *cache_meta) const;
    LocalPageCache *AddCache(LocalPageCacheMeta *cache_meta, offset_t offset);
    void RemoveCache(LocalPageCacheMeta *cache_meta);

    SharedMutex table_lock;

    robin_hood::unordered_flat_map<page_id_t, LocalPageCacheMeta *, std::hash<page_id_t>> table;
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

   private:
    PageThreadCacheManager &mgr;
    PageThreadLocalCache(PageThreadCacheManager &mgr) : mgr(mgr) { mgr.insert(this); }
    ~PageThreadLocalCache() { mgr.erase(this); }
};