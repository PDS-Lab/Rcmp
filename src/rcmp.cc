#include "rcmp.hpp"

#include <atomic>

#include "impl.hpp"
#include "lock.hpp"
#include "proto/rpc_register.hpp"

using namespace std::chrono_literals;

namespace rcmp {

PoolContext::PoolContext(ClientOptions options) {
    m_impl = new PoolContextImpl();
    DLOG_ASSERT(m_impl != nullptr, "Can't alloc ContextImpl");

    m_impl->m_options = options;

    m_impl->InitCXLPool();
    m_impl->InitRPCNexus();
    m_impl->ConnectWithDaemon();
}

PoolContext::~PoolContext() {
    m_impl->m_msgq_stop = true;
    m_impl->m_msgq_worker.join();
    cxl_close_simulate(m_impl->m_cxl_devdax_fd, m_impl->m_cxl_format);
}

PoolContext *Open(ClientOptions options) {
    PoolContext *pool_ctx = new PoolContext(options);
    return pool_ctx;
}

void Close(PoolContext *pool_ctx) { delete pool_ctx; }

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {
    uint64_t perf_stat_timer, perf_stat_timer_;
    m_impl->m_stats.start_sample(perf_stat_timer);
    perf_stat_timer_ = perf_stat_timer;

    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *page_cache;
    LocalPageCacheMeta *page_cache_meta;

    // TODO: more page

    auto &ptl = PageThreadLocalCache::getInstance(m_impl->m_tcache_mgr).page_cache_table;

    page_cache_meta = ptl.FindOrCreateCacheMeta(page_id);

    std::unique_lock<Mutex> cache_lock(page_cache_meta->ref_lock);

    // DLOG("CN %u: Read page %lu lock", m_impl->m_client_id, page_id);

    page_cache = ptl.FindCache(page_cache_meta);

    m_impl->m_stats.page_cache_search_sample(perf_stat_timer);

    if (page_cache == nullptr) {
        m_impl->m_stats.local_page_miss_sample();

        auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
            rpc_daemon::getPageCXLRefOrProxy,
            {
                .mac_id = m_impl->m_client_id,
                .type = rpc_daemon::GetPageCXLRefOrProxyRequest::READ,
                .gaddr = gaddr,
                .u =
                    {
                        .read =
                            {
                                .cn_read_size = size,
                            },
                    },
            });

        auto &resp = fu.get();

        if (!resp.refs) {
            memcpy(buf, resp.read_data, size);
            return Status::OK;
        }

        page_cache = ptl.AddCache(page_cache_meta, resp.offset);

        m_impl->m_stats.page_cache_fault_sample(perf_stat_timer);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);
    } else {
        m_impl->m_stats.local_page_hit_sample();
    }

    page_cache->Update();

    m_impl->m_stats.page_cache_update_sample(perf_stat_timer);

    memcpy(
        buf,
        reinterpret_cast<const void *>(m_impl->GetVirtualAddr(page_cache->offset + in_page_offset)),
        size);

    m_impl->m_stats.cxl_read_sample(size, perf_stat_timer);

    m_impl->m_stats.read_sample(perf_stat_timer_);
    return Status::OK;
}

Status PoolContext::Write(GAddr gaddr, size_t size, const void *buf) {
    uint64_t perf_stat_timer, perf_stat_timer_;
    m_impl->m_stats.start_sample(perf_stat_timer);
    perf_stat_timer_ = perf_stat_timer;

    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *page_cache;
    LocalPageCacheMeta *page_cache_meta;

    // TODO: more page

    auto &ptl = PageThreadLocalCache::getInstance(m_impl->m_tcache_mgr).page_cache_table;

    page_cache_meta = ptl.FindOrCreateCacheMeta(page_id);

    std::unique_lock<Mutex> cache_lock(page_cache_meta->ref_lock);

    page_cache = ptl.FindCache(page_cache_meta);

    m_impl->m_stats.page_cache_search_sample(perf_stat_timer);

    if (page_cache == nullptr) {
        m_impl->m_stats.local_page_miss_sample();
        // DLOG("Write can't find page %ld m_page_table_cache.", page_id);

        MsgQFuture<rpc_daemon::GetPageCXLRefOrProxyReply, SpinPromise<msgq::MsgBuffer>> fu;

        if (size <= get_page_cxl_ref_or_proxy_write_raw_max_size) {
            fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
                rpc_daemon::getPageCXLRefOrProxy,
                sizeof(rpc_daemon::GetPageCXLRefOrProxyRequest) + size,
                [&](rpc_daemon::GetPageCXLRefOrProxyRequest *req_buf) {
                    req_buf->mac_id = m_impl->m_client_id;
                    req_buf->type = req_buf->WRITE_RAW;
                    req_buf->gaddr = gaddr;
                    req_buf->u.write_raw.cn_write_raw_size = size;
                    memcpy(req_buf->u.write_raw.cn_write_raw_buf, buf, size);
                });
        } else {
            fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
                rpc_daemon::getPageCXLRefOrProxy,
                {
                    .mac_id = m_impl->m_client_id,
                    .type = rpc_daemon::GetPageCXLRefOrProxyRequest::WRITE,
                    .gaddr = gaddr,
                    .u =
                        {
                            .write =
                                {
                                    .cn_write_size = size,
                                    .cn_write_buf = buf,
                                },
                        },
                });
        }

        auto &resp = fu.get();

        if (!resp.refs) {
            return Status::OK;
        }

        page_cache = ptl.AddCache(page_cache_meta, resp.offset);

        m_impl->m_stats.page_cache_fault_sample(perf_stat_timer);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);
    } else {
        m_impl->m_stats.local_page_hit_sample();
    }

    page_cache->Update();

    m_impl->m_stats.page_cache_update_sample(perf_stat_timer);

    memcpy(reinterpret_cast<void *>(m_impl->GetVirtualAddr(page_cache->offset + in_page_offset)),
           buf, size);

    m_impl->m_stats.cxl_write_sample(size, perf_stat_timer);

    m_impl->m_stats.write_sample(perf_stat_timer_);
    return Status::OK;
}

Status PoolContext::CAS(GAddr gaddr, uint64_t &expected, uint64_t desired, bool &ret) {
    uint64_t perf_stat_timer, perf_stat_timer_;
    m_impl->m_stats.start_sample(perf_stat_timer);
    perf_stat_timer_ = perf_stat_timer;

    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *page_cache;
    LocalPageCacheMeta *page_cache_meta;

    auto &ptl = PageThreadLocalCache::getInstance(m_impl->m_tcache_mgr).page_cache_table;

    page_cache_meta = ptl.FindOrCreateCacheMeta(page_id);

    std::unique_lock<Mutex> cache_lock(page_cache_meta->ref_lock);

    page_cache = ptl.FindCache(page_cache_meta);

    m_impl->m_stats.page_cache_search_sample(perf_stat_timer);

    if (page_cache == nullptr) {
        m_impl->m_stats.local_page_miss_sample();
        // DLOG("Write can't find page %ld m_page_table_cache.", page_id);

        MsgQFuture<rpc_daemon::GetPageCXLRefOrProxyReply, SpinPromise<msgq::MsgBuffer>> fu;
        fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
            rpc_daemon::getPageCXLRefOrProxy,
            {
                .mac_id = m_impl->m_client_id,
                .type = rpc_daemon::GetPageCXLRefOrProxyRequest::CAS,
                .gaddr = gaddr,
                .u = {.cas =
                          {
                              .expected = expected,
                              .desired = desired,
                          }},
            });

        auto &resp = fu.get();

        if (!resp.refs) {
            ret = (expected == resp.old_val);
            expected = resp.old_val;
            return Status::OK;
        }

        page_cache = ptl.AddCache(page_cache_meta, resp.offset);

        m_impl->m_stats.page_cache_fault_sample(perf_stat_timer);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);
    } else {
        m_impl->m_stats.local_page_hit_sample();
    }

    page_cache->Update();

    m_impl->m_stats.page_cache_update_sample(perf_stat_timer);

    ret = __atomic_compare_exchange_n(
        reinterpret_cast<uint64_t *>(m_impl->GetVirtualAddr(page_cache->offset + in_page_offset)),
        &expected, desired, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);

    m_impl->m_stats.cxl_cas_sample(perf_stat_timer);

    m_impl->m_stats.cas_sample(perf_stat_timer_);
    return Status::OK;
}

GAddr PoolContext::Alloc(size_t size) { DLOG_FATAL("Not Support"); }

Status PoolContext::Free(GAddr gaddr, size_t size) { DLOG_FATAL("Not Support"); }

GAddr PoolContext::AllocPage(size_t count) {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::allocPage, {
                                   .mac_id = m_impl->m_client_id,
                                   .count = count,
                               });

    auto &resp = fu.get();

    GAddr gaddr = GetGAddr(resp.start_page_id, 0);

    return gaddr;
}

Status PoolContext::FreePage(GAddr gaddr, size_t count) { DLOG_FATAL("Not Support"); }

// // TODO: write lock
// Status PoolContext::WriteBatch(GAddr gaddr, size_t size, void *buf) {
//     page_id_t page_id = GetPageID(gaddr);
//     offset_t in_page_offset = GetPageOffset(gaddr);
//     LocalPageCache *pageCache;

//     // TODO: more page
//     SharedMutex *cache_lock;
//     auto p_lock =
//         m_impl->m_ptl_cache_lock.find_or_emplace(page_id, []() { return new SharedMutex(); });
//     cache_lock = p_lock.first->second;
//     // DLOG("CN %u: write page %lu lock", m_impl->m_client_id, page_id);
//     cache_lock->lock_shared();

//     auto p = m_impl->m_page_table_cache.find(page_id);
//     if (p == m_impl->m_page_table_cache.end()) {
//         size_t bb_pos = m_impl->m_batch_cur;

//         memcpy(reinterpret_cast<void *>(m_impl->m_batch_buffer + bb_pos), buf, size);
//         m_impl->m_batch_list.push_back({gaddr, size, bb_pos});
//         m_impl->m_batch_cur += size;

//         if (m_impl->m_batch_cur > (64ul << 20)) {
//             if (m_impl->batch_flush_worker.joinable()) {
//                 m_impl->batch_flush_worker.join();
//             }

//             std::vector<std::tuple<rcmp::GAddr, size_t, offset_t>> batch_list;
//             batch_list.swap(m_impl->m_batch_list);

//             m_impl->batch_flush_worker = std::thread(
//                 [&, batch_list = std::move(batch_list), m_batch_buffer =
//                 m_impl->m_batch_buffer]() {
//                     std::unordered_map<rcmp::GAddr, std::pair<size_t, offset_t>> umap;
//                     for (auto &tu : batch_list) {
//                         umap[std::get<0>(tu)] = std::make_pair(std::get<1>(tu), std::get<2>(tu));
//                     }

//                     for (auto &p : umap) {
//                         Write(p.first, p.second.first, m_batch_buffer + p.second.second);
//                     }

//                     delete[] m_batch_buffer;
//                 });
//             m_impl->m_batch_buffer = new char[66ul << 20];
//             m_impl->m_batch_cur = 0;
//         }

//     } else {
//         pageCache = p->second;
//         memcpy(reinterpret_cast<void *>(
//                    reinterpret_cast<uintptr_t>(m_impl->m_cxl_format.page_data_start_addr) +
//                    pageCache->offset + in_page_offset),
//                buf, size);
//         pageCache->stats.add(getTimestamp());
//     }

//     cache_lock->unlock_shared();
//     // DLOG("CN %u: write page %lu unlock", m_impl->m_client_id, page_id);
//     return Status::OK;
// }

const ClientOptions &PoolContext::GetOptions() const { return m_impl->m_options; }

}  // namespace rcmp

void ClientContext::InitCXLPool() {
    m_cxl_memory_addr =
        cxl_open_simulate(m_options.cxl_devdax_path, m_options.cxl_memory_size, &m_cxl_devdax_fd);

    cxl_memory_open(m_cxl_format, m_cxl_memory_addr);
}

void ClientContext::InitRPCNexus() {
    m_udp_conn_recver =
        std::make_unique<UDPServer<msgq::MsgUDPConnPacket>>(m_options.client_port, 1000);

    m_msgq_nexus = std::make_unique<msgq::MsgQueueNexus>(m_cxl_format.msgq_zone_start_addr);

    m_msgq_nexus->register_req_func(RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData)::rpc_type,
                                    bind_msgq_rpc_func<false>(rpc_client::getCurrentWriteData));
    m_msgq_nexus->register_req_func(RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq)::rpc_type,
                                    bind_msgq_rpc_func<false>(rpc_client::getPagePastAccessFreq));
    m_msgq_nexus->register_req_func(RPC_TYPE_STRUCT(rpc_client::removePageCache)::rpc_type,
                                    bind_msgq_rpc_func<false>(rpc_client::removePageCache));
}

void ClientContext::InitFiberPool() { m_fiber_pool_.AddFiber(m_options.prealloc_fiber_num); }

void ClientContext::ConnectWithDaemon() {
    m_local_rack_daemon_connection.rack_id = m_options.rack_id;
    m_local_rack_daemon_connection.msgq_conn = std::make_unique<MsgQClient>(
        msgq::MsgQueueRPC{m_msgq_nexus.get(), m_msgq_nexus->GetPublicMsgQ(), nullptr, this});

    /* 3. 发送 join rack rpc */
    auto fu = m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::joinRack, {
                                  .client_ipv4 = m_options.client_ip,
                                  .client_port = m_options.client_port,
                                  .rack_id = m_options.rack_id,
                              });

    /* 4. The daemon sends an udp message telling the offset address of the recv queue */
    msgq::MsgUDPConnPacket msg;
    m_udp_conn_recver->recv_blocking(msg);

    m_msgq_rpc = std::make_unique<msgq::MsgQueueRPC>(
        m_msgq_nexus.get(), m_msgq_nexus->GetPublicMsgQ(),
        reinterpret_cast<msgq::MsgQueue *>(
            (reinterpret_cast<uintptr_t>(m_cxl_format.msgq_zone_start_addr) + msg.recv_q_off)),
        this);

    InitMsgQPooller();

    /* 5. Formal reception of rpc messages */
    auto &resp = fu.get();

    m_client_id = resp.client_mac_id;
    m_local_rack_daemon_connection.daemon_id = resp.daemon_mac_id;
    m_local_rack_daemon_connection.msgq_conn = std::make_unique<MsgQClient>(*m_msgq_rpc);

    DLOG("Connect with rack %d daemon %d success, my id is %d", m_options.rack_id,
         m_local_rack_daemon_connection.daemon_id, m_client_id);
}

void ClientContext::InitMsgQPooller() {
    // Launch of poll workers
    m_msgq_stop = false;
    m_msgq_worker = std::thread([this]() {
        boost::fibers::use_scheduling_algorithm<priority_scheduler>();
        InitFiberPool();

        while (!m_msgq_stop) {
            m_msgq_rpc->run_event_loop_once();
            boost::this_fiber::yield();
        }

        m_fiber_pool_.EraseAll();
    });
}

/*********************** for test **************************/

namespace rcmp {
void PoolContext::__DumpStats() {
    auto &stats = m_impl->m_stats;
    auto &msgq_stats = m_impl->m_msgq_nexus->m_stats;
    uint64_t total_local_cnt = stats.local_hit + stats.local_miss;

    DLOG(
        "write lat: %fns, read lat: %fns, local hit: %lu, local miss: %lu, cxl write lat: %fns, "
        "cxl read lat: %fns, page table lat: "
        "%fns, page fault lat: %fns, page sync lat: %fns, msgq send lat: %fns, msgq recv lat: %fns",
        1.0 * stats.write_time / (stats.cxl_write_io + 1),
        1.0 * stats.read_time / (stats.cxl_read_io + 1), stats.local_hit, stats.local_miss,
        1.0 * stats.cxl_write_time / (stats.cxl_write_io + 1),
        1.0 * stats.cxl_read_time / (stats.cxl_read_io + 1),
        1.0 * stats.local_cache_search_time / (total_local_cnt + 1),
        1.0 * stats.local_cache_fault_time / (stats.local_miss + 1),
        1.0 * stats.local_cache_update_time / (total_local_cnt + 1),
        1.0 * msgq_stats.send_time / (msgq_stats.send_io + 1),
        1.0 * msgq_stats.recv_time / (msgq_stats.recv_io + 1));
}

void PoolContext::__ClearStats() { m_impl->m_stats = {}; }

Status PoolContext::__TestDataSend1(int *array, size_t size) {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::__testdataSend1, sizeof(rpc_daemon::__TestDataSend1Request),
        [&](rpc_daemon::__TestDataSend1Request *req_buf) {
            req_buf->mac_id = m_impl->m_client_id;
            req_buf->size = size;
            memcpy(req_buf->data, array, size * sizeof(int));
        });

    auto &resp = fu.get();

    assert(resp.size == size);
    for (size_t i = 0; i < resp.size; i++) {
        assert(resp.data[i] == array[i]);
    }

    return Status::OK;
}

Status PoolContext::__TestDataSend2(int *array, size_t size) {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::__testdataSend2, sizeof(rpc_daemon::__TestDataSend2Request),
        [&](rpc_daemon::__TestDataSend2Request *req_buf) {
            req_buf->mac_id = m_impl->m_client_id;
            req_buf->size = size;
            memcpy(req_buf->data, array, size * sizeof(int));
        });

    auto &resp = fu.get();

    assert(resp.size == size);
    for (size_t i = 0; i < resp.size; i++) {
        assert(resp.data[i] == array[i]);
    }

    return Status::OK;
}

Status PoolContext::__NotifyPerf() {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::__notifyPerf, {
                                      .mac_id = m_impl->m_client_id,
                                  });

    fu.get();

    return Status::OK;
}

Status PoolContext::__StopPerf() {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::__stopPerf, {
                                    .mac_id = m_impl->m_client_id,
                                });

    fu.get();

    return Status::OK;
}

}  // namespace rcmp
