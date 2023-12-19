#include "proto/rpc_client.hpp"

#include <mutex>

#include "lock.hpp"

namespace rpc_client {

void getCurrentWriteData(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                         GetCurrentWriteDataRequest& req,
                         ResponseHandle<GetCurrentWriteDataReply>& resp_handle) {
    resp_handle.Init(req.dio_write_size);
    auto& reply = resp_handle.Get();

    memcpy(reply.data, req.dio_write_buf, req.dio_write_size);
}

void getPagePastAccessFreq(ClientContext& client_context,
                           ClientToDaemonConnection& daemon_connection,
                           GetPagePastAccessFreqRequest& req,
                           ResponseHandle<GetPagePastAccessFreqReply>& resp_handle) {
    page_id_t coldest_page = invalid_page_id;
    float coldest_heat = MAXFLOAT;
    float sum_heat = 0, num_found = 0;

    for (int i = 0; i < req.num_detect_pages; i++) {
        // Getting the access heat of a sampled page
        bool found = false;
        FreqStats::Heatness page_heat;
        client_context.m_tcache_mgr.foreach_all([&](PageThreadLocalCache& tcache) {
            auto cache = tcache.page_cache_table.FindCache(req.pages[i]);
            if (cache) {
                page_heat = page_heat + cache->Heat();
                found = true;
            }
        });

        if (found) {
            ++num_found;
            sum_heat += page_heat.last_heat;
            // Get the coldest page
            if (coldest_heat > page_heat.last_heat) {
                coldest_page = req.pages[i];
                coldest_heat = page_heat.last_heat;
            }
        }
    }

    // calc the average heat of sampled pages
    float avg_heat = sum_heat / (num_found + 1e-9);

    // DLOG("CN: %u getPagePastAccessFreq: finished.", client_context.m_client_id);
    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.coldest_page_id = coldest_page;
    reply.coldest_page_heat = coldest_heat;
    reply.avg_heat = avg_heat;
}

void removePageCache(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                     RemovePageCacheRequest& req,
                     ResponseHandle<RemovePageCacheReply>& resp_handle) {
    client_context.m_tcache_mgr.foreach_all([&](PageThreadLocalCache& tcache) {
        auto page_cache_meta = tcache.page_cache_table.FindOrCreateCacheMeta(req.page_id);
        auto page_cache = tcache.page_cache_table.FindCache(page_cache_meta);
        if (page_cache == nullptr) {
            return;
        }

        std::unique_lock<Mutex> cache_lock(page_cache_meta->ref_lock);

        tcache.page_cache_table.RemoveCache(page_cache_meta);
    });

    // DLOG("CN %u: Del page %lu cache.", client_context.m_client_id, req.page_id);

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.ret = true;
}

}  // namespace rpc_client
