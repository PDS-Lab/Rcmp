#include "proto/rpc_daemon.hpp"

#include "common.hpp"
#include "config.hpp"
#include "log.hpp"
#include "utils.hpp"

namespace rpc_daemon {

GetPageRefReply getPageRef(DaemonContext& daemon_context,
                           DaemonToClientConnection& client_connection, GetPageRefRequest& req) {
    PageMetadata* page_metadata;
    bool ret = daemon_context.page_table.find(req.page_id, &page_metadata);

    if (!ret) {
        // TODO: 本地缺页
        DLOG_FATAL("Not Support");
    }

    page_metadata->ref_client.insert(&client_connection);

    GetPageRefReply reply;
    reply.local_get = true;
    reply.offset = page_metadata->cxl_memory_offset;
    return reply;
}

AllocPageMemoryReply allocPageMemory(DaemonContext& daemon_context,
                                     DaemonToMasterConnection& master_connection,
                                     AllocPageMemoryRequest& req) {
    DLOG_ASSERT(daemon_context.current_used_page_num < daemon_context.max_data_page_num,
                "Can't allocate more page memory");

    offset_t cxl_memory_offset = daemon_context.cxl_page_allocator->allocate(1);
    DLOG_ASSERT(cxl_memory_offset != -1, "Can't allocate cxl memory");

    PageMetadata* page_metadata = new PageMetadata(req.slab_size);
    page_metadata->cxl_memory_offset = cxl_memory_offset;

    daemon_context.page_table.insert(req.page_id, page_metadata);

    AllocPageMemoryReply reply;
    reply.ret = true;
    return reply;
}

AllocReply alloc(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
                 AllocRequest& req) {
    // alloc size aligned by cache line
    size_t n = div_floor(req.n, min_slab_size);

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

    AllocReply reply;
    reply.gaddr = GetGAddr(page_id, page_offset);
    return reply;
}

FreeReply free(DaemonContext& daemon_context, DaemonToClientConnection& client_connection,
               FreeRequest& req) {
    DLOG_FATAL("Not Support");
}

}  // namespace rpc_daemon
