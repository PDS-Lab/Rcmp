#include "rchms.hpp"

#include <future>
#include <memory>

#include "common.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"
#include "promise.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_register.hpp"
#include "status.hpp"

using namespace std::chrono_literals;

namespace rchms {

PoolContext::PoolContext(ClientOptions options) {
    m_impl = new PoolContextImpl();
    DLOG_ASSERT(m_impl != nullptr, "Can't alloc ContextImpl");

    m_impl->m_options = options;

    // 1. 打开cxl设备
    m_impl->m_cxl_memory_addr =
        cxl_open_simulate(m_impl->m_options.cxl_devdax_path, m_impl->m_options.cxl_memory_size,
                          &m_impl->m_cxl_devdax_fd);

    cxl_memory_open(m_impl->m_cxl_format, m_impl->m_cxl_memory_addr);

    // 2. 与daemon建立连接
    m_impl->m_udp_conn_recver =
        std::make_unique<UDPServer<msgq::MsgUDPConnPacket>>(m_impl->m_options.client_port, 1000);

    m_impl->m_msgq_nexus =
        std::make_unique<msgq::MsgQueueNexus>(m_impl->m_cxl_format.msgq_zone_start_addr);

    m_impl->m_msgq_nexus->register_req_func(
        RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData)::rpc_type,
        bind_msgq_rpc_func<false>(rpc_client::getCurrentWriteData));
    m_impl->m_msgq_nexus->register_req_func(
        RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq)::rpc_type,
        bind_msgq_rpc_func<false>(rpc_client::getPagePastAccessFreq));
    m_impl->m_msgq_nexus->register_req_func(RPC_TYPE_STRUCT(rpc_client::removePageCache)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_client::removePageCache));

    m_impl->m_local_rack_daemon_connection.rack_id = m_impl->m_options.rack_id;
    m_impl->m_local_rack_daemon_connection.msgq_conn =
        std::make_unique<MsgQClient>(msgq::MsgQueueRPC{
            m_impl->m_msgq_nexus.get(), m_impl->m_msgq_nexus->GetPublicMsgQ(), nullptr, m_impl});

    // 3. 发送join rack rpc
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::joinRack, {
                                  .client_ipv4 = m_impl->m_options.client_ip,
                                  .client_port = m_impl->m_options.client_port,
                                  .rack_id = m_impl->m_options.rack_id,
                              });

    // 4. daemon会发送udp消息，告诉recv queue的偏移地址
    msgq::MsgUDPConnPacket msg;
    m_impl->m_udp_conn_recver->recv_blocking(msg);

    m_impl->m_msgq_rpc = std::make_unique<msgq::MsgQueueRPC>(
        m_impl->m_msgq_nexus.get(), m_impl->m_msgq_nexus->GetPublicMsgQ(),
        reinterpret_cast<msgq::MsgQueue *>(
            (reinterpret_cast<uintptr_t>(m_impl->m_cxl_format.msgq_zone_start_addr) +
             msg.recv_q_off)),
        m_impl);

    // 启动polling worker
    m_impl->m_msgq_stop = false;
    m_impl->m_msgq_worker = std::thread([this]() {
        while (!m_impl->m_msgq_stop) {
            m_impl->m_msgq_rpc->run_event_loop_once();
        }
    });

    // 5. 正式接收rpc消息
    auto &resp = fu.get();

    m_impl->m_client_id = resp.client_mac_id;
    m_impl->m_local_rack_daemon_connection.daemon_id = resp.daemon_mac_id;
    m_impl->m_local_rack_daemon_connection.msgq_conn =
        std::make_unique<MsgQClient>(*m_impl->m_msgq_rpc);

    DLOG("Connect with rack %d daemon %d success, my id is %d", m_impl->m_options.rack_id,
         m_impl->m_local_rack_daemon_connection.daemon_id, m_impl->m_client_id);
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

void Close(PoolContext *pool_ctx) {
    // TODO: 关闭连接

    delete pool_ctx;
}

Status PoolContext::Read(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *page_cache;

    // TODO: more page

    SharedResourceLock<page_id_t, LockResourceManager<page_id_t, SharedMutex>> cache_lock(
        m_impl->m_ptl_cache_lock, page_id);

    // DLOG("CN %u: Read page %lu lock", m_impl->m_client_id, page_id);

    page_cache = m_impl->m_page_table_cache.FindCache(page_id);
    if (page_cache == nullptr) {
        // m_impl->m_stats.local_miss++;

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

        page_cache = m_impl->m_page_table_cache.AddCache(page_id, resp.offset);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);
    }

    memcpy(
        buf,
        reinterpret_cast<const void *>(m_impl->GetVirtualAddr(page_cache->offset + in_page_offset)),
        size);

    page_cache->Update();
    return Status::OK;
}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *page_cache;

    // TODO: more page

    SharedResourceLock<page_id_t, LockResourceManager<page_id_t, SharedMutex>> cache_lock(
        m_impl->m_ptl_cache_lock, page_id);

    page_cache = m_impl->m_page_table_cache.FindCache(page_id);
    if (page_cache == nullptr) {
        // m_impl->m_stats.local_miss++;
        // DLOG("Write can't find page %ld m_page_table_cache.", page_id);

        decltype(((MsgQClient *)0)->call<SpinPromise>(rpc_daemon::getPageCXLRefOrProxy, {})) fu;

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

        page_cache = m_impl->m_page_table_cache.AddCache(page_id, resp.offset);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);
    }

    memcpy(reinterpret_cast<void *>(m_impl->GetVirtualAddr(page_cache->offset + in_page_offset)),
           buf, size);

    page_cache->Update();
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
//     // 上读锁
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

//             std::vector<std::tuple<rchms::GAddr, size_t, offset_t>> batch_list;
//             batch_list.swap(m_impl->m_batch_list);

//             m_impl->batch_flush_worker = std::thread(
//                 [&, batch_list = std::move(batch_list), m_batch_buffer =
//                 m_impl->m_batch_buffer]() {
//                     std::unordered_map<rchms::GAddr, std::pair<size_t, offset_t>> umap;
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
//         // 更新page访问请况统计
//         pageCache->stats.add(getTimestamp());
//     }

//     // 解锁
//     cache_lock->unlock_shared();
//     // DLOG("CN %u: write page %lu unlock", m_impl->m_client_id, page_id);
//     return Status::OK;
// }

const ClientOptions &PoolContext::GetOptions() const { return m_impl->m_options; }

}  // namespace rchms

/*********************** for test **************************/

namespace rchms {

void PoolContext::__DumpStats() {
    DLOG("local hit: %lu, local miss: %lu", m_impl->m_stats.local_hit, m_impl->m_stats.local_miss);
}

Status PoolContext::__TestDataSend1(int *array, size_t size) {
    auto fu = m_impl->m_local_rack_daemon_connection.msgq_conn->call<SpinPromise>(
        rpc_daemon::__testdataSend1, sizeof(rpc_daemon::__TestDataSend1Request),
        [&](rpc_daemon::__TestDataSend1Request *req_buf) {
            req_buf->mac_id = m_impl->m_client_id;
            req_buf->size = size;
            memcpy(req_buf->data, array, size * sizeof(int));
        });

    auto &resp = fu.get();

    // check
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

    // check
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

}  // namespace rchms
