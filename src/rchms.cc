#include "rchms.hpp"
#include "pool_impl.hpp"

namespace rchms {

PoolContext::PoolContext(ClientOptions options) {
    __PoolContextImpl *impl = new __PoolContextImpl();
    DLOG_ASSERT(impl != nullptr, "Can't alloc ContextImpl");
    impl->options = options;

    // TODO: 与master建立连接
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

GAddr PoolContext::Alloc(size_t size) {}

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {}

Status PoolContext::Free(GAddr gaddr, size_t size) {}
}  // namespace rchms