#include "proto/rpc_daemon.hpp"

#include "common.hpp"
#include "config.hpp"
#include "log.hpp"
#include "utils.hpp"

namespace rpc_daemon {

GetPageRefReply getPageRef(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, page_id_t page_id) {
    PageMetadata* page_metadata;
    bool ret = daemon_context.page_table.find(page_id, &page_metadata);

    if (!ret) {
        // TODO: 本地缺页
        DLOG_FATAL("Not Support");
    }

    page_metadata->ref_client.insert(&client_connection);

    return {
        .local_get = true,
        .offset = page_metadata->cxl_memory_offset,
    };
}

bool allocPageMemory(DaemonContext& daemon_context, DaemonToMasterConnection& master_connection,
                     page_id_t page_id, size_t slab_size) {
    DLOG_ASSERT(daemon_context.current_used_page_num < daemon_context.max_data_page_num,
                "Can't allocate more page memory");

    offset_t cxl_memory_offset = daemon_context.cxl_page_allocator->allocate(1);
    DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

    PageMetadata* page_metadata = new PageMetadata(slab_size);
    page_metadata->cxl_memory_offset = cxl_memory_offset;

    daemon_context.page_table.insert(page_id, page_metadata);

    return true;
}

rchms::GAddr alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                   size_t n) {
    // alloc size aligned by cache line
    n = div_floor(n, min_slab_size);

    size_t slab_cls = n / min_slab_size - 1;
    std::list<page_id_t>& slab_list = daemon_context.can_alloc_slab_class_lists[slab_cls];

    if (slab_list.empty()) {
        size_t slab_size = (slab_cls + 1) * min_slab_size;
        // TODO: 向Master调用pageAlloc(slab_size)
    }

    page_id_t page_id = slab_list.front();
    PageMetadata* page_metadata;
    bool ret = daemon_context.page_table.find(page_id, &page_metadata);
    DLOG_ASSERT(ret, "Can't find page id %lu", page_id);

    DLOG_ASSERT(!page_metadata->slab_allocator.full(), "Can't allocate the page %lu continuely",
                page_id);

    offset_t page_offset = page_metadata->slab_allocator.allocate(1);
    DLOG_ASSERT(page_offset != -1, "Can't alloc page slab, because page %lu is full", page_id);

    if (page_metadata->slab_allocator.full()) {
        slab_list.erase(slab_list.begin());
    }

    return GetGAddr(page_id, page_offset);
}

bool free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
          rchms::GAddr gaddr, size_t n) {
    DLOG_FATAL("Not Support");
    return false;
}

}  // namespace rpc_daemon
