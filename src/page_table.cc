#include "page_table.hpp"

#include "impl.hpp"

PageRackMetadata *PageDirectory::FindPage(page_id_t page_id) {
    auto it = table.find(page_id);
    if (it == table.end()) {
        return nullptr;
    }
    return it->second;
}

PageRackMetadata *PageDirectory::AddPage(RackMacTable *rack_table, page_id_t page_id) {
    PageRackMetadata *page_meta = new PageRackMetadata();
    page_meta->rack_id = rack_table->daemon_connect->rack_id;
    page_meta->daemon_id = rack_table->daemon_connect->daemon_id;
    table.insert(page_id, page_meta);
    rack_table->current_allocated_page_num++;

    DLOG("Add page %lu --> rack %u", page_id, page_meta->rack_id);

    return page_meta;
}

void PageDirectory::RemovePage(RackMacTable *rack_table, page_id_t page_id) {
    auto it = table.find(page_id);
    PageRackMetadata *page_meta = it->second;
    table.erase(it);
    delete page_meta;
    rack_table->current_allocated_page_num--;
}

void PageTableManager::EraseRemotePageRefMeta(PageMetadata *page_meta) {
    std::unique_lock<CortMutex> page_remote_ref_lock(page_meta->remote_ref_lock);
    delete page_meta->remote_ref_meta;
    page_meta->remote_ref_meta = nullptr;
}

PageVMMapMetadata *PageTableManager::AllocPageMemory() {
    DLOG_ASSERT(TestAllocPageMemory(), "Can't allocate more page memory");

    offset_t cxl_memory_offset = page_allocator->allocate(1);
    DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

    PageVMMapMetadata *page_vm_meta = new PageVMMapMetadata();
    page_vm_meta->cxl_memory_offset = cxl_memory_offset;

    return page_vm_meta;
}

void PageTableManager::ApplyPageMemory(PageMetadata *page_meta, PageVMMapMetadata *page_vm_meta) {
    DLOG_ASSERT(page_meta->vm_meta == nullptr, "Can't cover existed page vm meta");
    page_meta->vm_meta = page_vm_meta;
    current_used_page_num++;
}

void PageTableManager::CancelPageMemory(PageMetadata *page_meta) {
    auto tmp = page_meta->vm_meta;
    page_meta->vm_meta = nullptr;
    page_allocator->deallocate(tmp->cxl_memory_offset, 1);
    current_used_page_num--;
    delete tmp;
}

void PageTableManager::RandomPickUnvisitVMPage(bool force, bool &ret, page_id_t &page_id,
                                               PageMetadata *&page_meta) {
    thread_local std::mt19937 eng(rand());

    table.random_foreach_all(eng, [&](std::pair<const page_id_t, PageMetadata *> &p) {
        if (p.second->vm_meta != nullptr && (force || p.second->vm_meta->ref_client.empty()) &&
            p.second->vm_meta->TryPin()) {
            page_id = p.first;
            page_meta = p.second;
            ret = true;
            return false;
        }
        return true;
    });
}

PageCacheTable::~PageCacheTable() {
    for (auto &p : table) {
        delete p.second;
    }
}

LocalPageCache *PageCacheTable::FindCache(page_id_t page_id) {
    auto it = table.find(page_id);
    if (it == table.end()) {
        return nullptr;
    }
    return it->second;
}

LocalPageCache *PageCacheTable::AddCache(page_id_t page_id, offset_t offset) {
    LocalPageCache *page_cache = new LocalPageCache(8);
    page_cache->offset = offset;

    // DLOG("add local page %lu cache", page_id);
    table.emplace(page_id, page_cache);

    return page_cache;
}

void PageCacheTable::RemoveCache(page_id_t page_id) {
    auto it = table.find(page_id);
    if (it == table.end()) {
        return;
    }

    LocalPageCache *page_cache = it->second;

    table.erase(it);
    delete page_cache;

    // DLOG("remove local page %lu cache", page_id);
}

void PageThreadCacheManager::insert(PageThreadLocalCache *tcache) {
    std::unique_lock<std::shared_mutex> lck(mutex_);
    tcache_list_.push_back(tcache);
}

void PageThreadCacheManager::erase(PageThreadLocalCache *tcache) {
    std::unique_lock<std::shared_mutex> lck(mutex_);
    auto it = std::find(tcache_list_.begin(), tcache_list_.end(), tcache);
    tcache_list_.erase(it);
}