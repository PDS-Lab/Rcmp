#include "proto/rpc_client.hpp"

namespace rpc_client {

GetCurrentWriteDataReply getCurrentWriteData(ClientContext& client_context,
                                             ClientToDaemonConnection& daemon_connection,
                                             GetCurrentWriteDataRequest& req) {
    auto reply_ptr = req.alloc_flex_resp(req.dio_write_size);

    memcpy(reply_ptr->data, req.dio_write_buf, req.dio_write_size);

    return {};
}

GetPagePastAccessFreqReply getPagePastAccessFreq(ClientContext& client_context,
                                                 ClientToDaemonConnection& daemon_connection,
                                                 GetPagePastAccessFreqRequest& req) {
    SharedMutex* cache_lock;
    LocalPageCache* pageCache;
    page_id_t oldest_page = 0;
    uint64_t last_time = UINT64_MAX;
    uint64_t last_time_tmp = 0;

    // m_page中存的是该CN访问过的所有Page的id
    // client_context.m_ptl_cache_lock.foreach_all([&](std::pair<const page_id_t, SharedMutex*>& p)
    // {
    // size_t i = 0;
    client_context.m_page_table_cache.foreach_all(
        [&](std::pair<const page_id_t, LocalPageCache*>& p) {
            // auto pageCache = p.second;
            auto page_id = p.first;

            auto p_lock = client_context.m_ptl_cache_lock.find(page_id);
            cache_lock = p_lock->second;
            cache_lock->lock_shared();

            auto p_cache = client_context.m_page_table_cache.find(page_id);
            if (p_cache != client_context.m_page_table_cache.end()) {
                auto pageCache = p_cache->second;

                last_time_tmp = pageCache->stats.last();
                if (last_time > last_time_tmp) {
                    last_time = last_time_tmp;  // 越小，越旧
                    oldest_page = page_id;
                }
                DLOG("CN: %u getPagePastAccessFreq: find page %lu's cache.",
                     client_context.m_client_id, page_id);
            }

            cache_lock->unlock_shared();

            // i++;
            return true;
        });
    DLOG("CN: %u getPagePastAccessFreq: finished.", client_context.m_client_id);
    GetPagePastAccessFreqReply reply;
    reply.last_access_ts = last_time;
    reply.oldest_page_id = oldest_page;
    return reply;
}

RemovePageCacheReply removePageCache(ClientContext& client_context,
                                     ClientToDaemonConnection& daemon_connection,
                                     RemovePageCacheRequest& req) {
    SharedMutex* cache_lock;
    auto p_lock = client_context.m_ptl_cache_lock.find(req.page_id);
    DLOG("removePageCache page %lu's cache lock.", req.page_id);
    if (p_lock == client_context.m_ptl_cache_lock.end()) {
        RemovePageCacheReply reply;
        return reply;
    }

    cache_lock = p_lock->second;
    // 上写锁
    cache_lock->lock();

    LocalPageCache* pageCache;
    auto p_cache = client_context.m_page_table_cache.find(req.page_id);
    DLOG_ASSERT(p_cache != client_context.m_page_table_cache.end(), "Can't find page %lu's cache.",
                req.page_id);
    pageCache = p_cache->second;

    // 清除该page的cache
    client_context.m_page_table_cache.erase(req.page_id);
    // client_context.m_page.erase(req.page_id);

    DLOG("CN %u: Del page %lu cache.", client_context.m_client_id, req.page_id);
    cache_lock->unlock();
    DLOG("CN %u: removePageCache page %lu unlock", client_context.m_client_id, req.page_id);

    RemovePageCacheReply reply;
    return reply;
}

}  // namespace rpc_client
