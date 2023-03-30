#include "rchms.hpp"

#include <cstdint>

#include "common.hpp"
#include "impl.hpp"
#include "status.hpp"

namespace rchms {

PoolContext::PoolContext(ClientOptions options) {
    PoolContextImpl *impl = new PoolContextImpl();
    DLOG_ASSERT(impl != nullptr, "Can't alloc ContextImpl");
    impl->options = options;

    // TODO: 与master建立连接，获取到本rack的daemon ip、port

    // TODO: 与daemon建立连接
}

PoolContext::~PoolContext() {}

PoolContext *Open(ClientOptions options) {
    PoolContext *pool_ctx = new PoolContext(options);
    return pool_ctx;
}

void Close(PoolContext *pool_ctx) {
    // TODO: 关闭连接

    delete pool_ctx;
}

GAddr PoolContext::Alloc(size_t size) {
    // TODO: 向daemon发送alloc(size)请求
    DLOG_FATAL("Not Support");
    return 0;
}

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t offset;
    bool ret = __impl->page_table_cache.find(page_id, &offset);
retry:
    if (ret) {
        memcpy(buf,
               reinterpret_cast<const void *>(reinterpret_cast<uintptr_t>(__impl->cxl_memory_addr) +
                                              offset),
               size);
        return Status::OK;
    } else {
        // TODO: 向daemon发送getPageRef(page_id)请求
        DLOG_FATAL("Not Support");
    }
}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t offset;
    bool ret = __impl->page_table_cache.find(page_id, &offset);
retry:
    if (ret) {
        memcpy(
            reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(__impl->cxl_memory_addr) + offset),
            buf, size);
        return Status::OK;
    } else {
        // TODO: 向daemon发送getPageRef(page_id)请求
        DLOG_FATAL("Not Support");
    }
}

Status PoolContext::Free(GAddr gaddr, size_t size) { DLOG_FATAL("Not Support"); }
}  // namespace rchms