#include "rchms.hpp"

#include <cstdint>
#include <future>

#include "common.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_daemon.hpp"
#include "status.hpp"

using namespace std::chrono_literals;

namespace rchms {

PoolContext::PoolContext(ClientOptions options) {
    __impl = new PoolContextImpl();
    DLOG_ASSERT(__impl != nullptr, "Can't alloc ContextImpl");
    __impl->m_options = options;

    // 1. 打开cxl设备
    __impl->m_cxl_memory_addr =
        cxl_open_simulate(__impl->m_options.cxl_devdax_path, __impl->m_options.cxl_memory_size,
                          &__impl->m_cxl_devdax_fd);

    cxl_memory_open(__impl->m_cxl_format, __impl->m_cxl_memory_addr);

    // 2. 与daemon建立连接
    __impl->m_udp_conn_recver.reset(
        new UDPServer<msgq::MsgUDPConnPacket>(__impl->m_options.client_port, 1000));

    __impl->m_msgq_nexus.reset(new msgq::MsgQueueNexus(__impl->m_cxl_format.msgq_zone_start_addr));
    __impl->m_msgq_rpc.reset(new msgq::MsgQueueRPC(__impl->m_msgq_nexus.get(), __impl));
    __impl->m_msgq_rpc->m_send_queue = __impl->m_msgq_nexus->m_public_msgq;

    __impl->m_msgq_nexus->register_req_func(
        RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData)::rpc_type,
        bind_msgq_rpc_func<false>(rpc_client::getCurrentWriteData));
    __impl->m_msgq_nexus->register_req_func(
        RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq)::rpc_type,
        bind_msgq_rpc_func<false>(rpc_client::getPagePastAccessFreq));
    __impl->m_msgq_nexus->register_req_func(RPC_TYPE_STRUCT(rpc_client::removePageCache)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_client::removePageCache));

    // 3. 发送join rack rpc
    using JoinRackRPC = RPC_TYPE_STRUCT(rpc_daemon::joinRack);
    msgq::MsgBuffer req_raw =
        __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(JoinRackRPC::RequestType));
    auto req = reinterpret_cast<JoinRackRPC::RequestType *>(req_raw.get_buf());
    req->client_ipv4 = __impl->m_options.client_ip;
    req->client_port = __impl->m_options.client_port;
    req->rack_id = __impl->m_options.rack_id;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(JoinRackRPC::rpc_type, req_raw,
                                        msgq_general_promise_flag_cb, static_cast<void *>(&pro));

    // 4. daemon会发送udp消息，告诉recv queue的偏移地址
    msgq::MsgUDPConnPacket msg;
    __impl->m_udp_conn_recver->recv_blocking(msg);

    __impl->m_msgq_rpc->m_recv_queue = reinterpret_cast<msgq::MsgQueue *>(
        (reinterpret_cast<uintptr_t>(__impl->m_cxl_memory_addr) + msg.recv_q_off));

    // 5. 正式接收rpc消息
    while (fu.wait_for(1ns) == std::future_status::timeout) {
        __impl->m_msgq_rpc->run_event_loop_once();
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<JoinRackRPC::ResponseType *>(resp_raw.get_buf());

    __impl->m_client_id = resp->client_mac_id;
    __impl->m_local_rack_daemon_connection.daemon_id = resp->daemon_mac_id;
    __impl->m_local_rack_daemon_connection.rack_id = __impl->m_options.rack_id;
    __impl->m_local_rack_daemon_connection.msgq_rpc = __impl->m_msgq_rpc.get();

    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    __impl->m_msgq_stop = false;
    __impl->m_msgq_worker = std::thread([this]() {
        while (!__impl->m_msgq_stop) {
            __impl->m_msgq_rpc->run_event_loop_once();
        }
    });

    DLOG("Connect with rack %d daemon %d success, my id is %d", __impl->m_options.rack_id,
         __impl->m_local_rack_daemon_connection.daemon_id, __impl->m_client_id);
}

PoolContext::~PoolContext() {
    __impl->m_msgq_stop = true;
    __impl->m_msgq_worker.join();
    cxl_close_simulate(__impl->m_cxl_devdax_fd, __impl->m_cxl_format);
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
    LocalPageCache *pageCache;

    // TODO: more page
    SharedMutex *cache_lock;
    auto p_lock =
        __impl->m_ptl_cache_lock.find_or_emplace(page_id, []() { return new SharedMutex(); });
    cache_lock = p_lock.first->second;

    // 上读锁
    // DLOG("CN %u: Read page %lu lock", __impl->m_client_id, page_id);
    cache_lock->lock_shared();

    auto p = __impl->m_page_table_cache.find(page_id);
    if (p == __impl->m_page_table_cache.end()) {
        // __impl->m_stats.local_miss++;

        // DLOG("Read can't find page %ld m_page_table_cache.", page_id);
        using GetPageRefOrProxyRPC = RPC_TYPE_STRUCT(rpc_daemon::getPageCXLRefOrProxy);
        msgq::MsgBuffer req_raw =
            __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(GetPageRefOrProxyRPC::RequestType));
        auto req = reinterpret_cast<GetPageRefOrProxyRPC::RequestType *>(req_raw.get_buf());
        req->mac_id = __impl->m_client_id;
        req->type = req->READ;
        req->gaddr = gaddr;
        req->cn_read_size = size;

        SpinPromise<msgq::MsgBuffer> pro;
        SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
        __impl->m_msgq_rpc->enqueue_request(GetPageRefOrProxyRPC::rpc_type, req_raw,
                                            msgq_general_bool_flag_cb, static_cast<void *>(&pro));

        while (fu.wait_for(0s) == std::future_status::timeout) {
        }

        msgq::MsgBuffer resp_raw = fu.get();
        auto resp = reinterpret_cast<GetPageRefOrProxyRPC::ResponseType *>(resp_raw.get_buf());

        if (!resp->refs) {
            memcpy(buf, resp->read_data, size);
            __impl->m_msgq_rpc->free_msg_buffer(resp_raw);
            cache_lock->unlock_shared();
            return Status::OK;
        }

        pageCache = new LocalPageCache(8);
        pageCache->offset = resp->offset;

        __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

        __impl->m_page_table_cache.insert(page_id, pageCache);

        SharedMutex *cache_lock_new = new SharedMutex();
        __impl->m_ptl_cache_lock.insert(page_id, cache_lock_new);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);

    } else {
        pageCache = p->second;
        // __impl->m_stats.local_hit++;
    }

    memcpy(buf,
           reinterpret_cast<const void *>(
               reinterpret_cast<uintptr_t>(__impl->m_cxl_format.page_data_start_addr) +
               pageCache->offset + in_page_offset),
           size);

    // 更新page访问请况统计
    pageCache->stats.add(getTimestamp());
    // 解锁
    cache_lock->unlock_shared();
    // DLOG("CN %u: Read page %lu unlock", __impl->m_client_id, page_id);
    return Status::OK;
}

Status PoolContext::Write(GAddr gaddr, size_t size, void *buf) {
    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *pageCache;

    // TODO: more page
    SharedMutex *cache_lock;
    auto p_lock =
        __impl->m_ptl_cache_lock.find_or_emplace(page_id, []() { return new SharedMutex(); });
    cache_lock = p_lock.first->second;
    // 上读锁
    // DLOG("CN %u: write page %lu lock", __impl->m_client_id, page_id);
    cache_lock->lock_shared();

    auto p = __impl->m_page_table_cache.find(page_id);
    if (p == __impl->m_page_table_cache.end()) {
        // __impl->m_stats.local_miss++;
        // DLOG("Write can't find page %ld m_page_table_cache.", page_id);
        using GetPageRefOrProxyRPC = RPC_TYPE_STRUCT(rpc_daemon::getPageCXLRefOrProxy);

        msgq::MsgBuffer req_raw;
        size_t payload_size;

        // 如果不超过阈值，则直接将buf填入req中发送，减少一次get_current_write_data消息
        if (size <= get_page_cxl_ref_or_proxy_write_raw_max_size) {
            req_raw = __impl->m_msgq_rpc->alloc_msg_buffer(
                sizeof(GetPageRefOrProxyRPC::RequestType) + size);
            auto req = reinterpret_cast<GetPageRefOrProxyRPC::RequestType *>(req_raw.get_buf());
            req->mac_id = __impl->m_client_id;
            req->type = req->WRITE_RAW;
            req->gaddr = gaddr;
            req->cn_write_size = size;
            memcpy(req->cn_write_raw_buf, buf, size);
            // DLOG("Small size Write");
        } else {
            req_raw =
                __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(GetPageRefOrProxyRPC::RequestType));
            auto req = reinterpret_cast<GetPageRefOrProxyRPC::RequestType *>(req_raw.get_buf());
            req->mac_id = __impl->m_client_id;
            req->type = req->WRITE;
            req->gaddr = gaddr;
            req->cn_write_size = size;
            req->cn_write_buf = buf;
            // DLOG("large size Write");
        }

        SpinPromise<msgq::MsgBuffer> pro;
        SpinFuture<msgq::MsgBuffer> fu = pro.get_future();
        __impl->m_msgq_rpc->enqueue_request(GetPageRefOrProxyRPC::rpc_type, req_raw,
                                            msgq_general_bool_flag_cb, static_cast<void *>(&pro));

        while (fu.wait_for(0s) == std::future_status::timeout) {
        }

        msgq::MsgBuffer resp_raw = fu.get();
        auto resp = reinterpret_cast<GetPageRefOrProxyRPC::ResponseType *>(resp_raw.get_buf());

        if (!resp->refs) {
            __impl->m_msgq_rpc->free_msg_buffer(resp_raw);
            cache_lock->unlock_shared();
            return Status::OK;
        }

        pageCache = new LocalPageCache(8);
        pageCache->offset = resp->offset;
        __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

        __impl->m_page_table_cache.insert(page_id, pageCache);
        SharedMutex *cache_lock_new = new SharedMutex();
        __impl->m_ptl_cache_lock.insert(page_id, cache_lock_new);

        // DLOG("get ref: %ld --- %#lx", page_id, pageCache->offset);

    } else {
        pageCache = p->second;
        // __impl->m_stats.local_hit++;
    }

    memcpy(reinterpret_cast<void *>(
               reinterpret_cast<uintptr_t>(__impl->m_cxl_format.page_data_start_addr) +
               pageCache->offset + in_page_offset),
           buf, size);

    // 更新page访问请况统计
    pageCache->stats.add(getTimestamp());
    // 解锁
    cache_lock->unlock_shared();
    // DLOG("CN %u: write page %lu unlock", __impl->m_client_id, page_id);
    return Status::OK;
}

GAddr PoolContext::Alloc(size_t size) { DLOG_FATAL("Not Support"); }

Status PoolContext::Free(GAddr gaddr, size_t size) { DLOG_FATAL("Not Support"); }

GAddr PoolContext::AllocPage(size_t count) {
    using AllocRPC = RPC_TYPE_STRUCT(rpc_daemon::allocPage);
    msgq::MsgBuffer req_raw = __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(AllocRPC::RequestType));
    auto req = reinterpret_cast<AllocRPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->count = count;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(AllocRPC::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                        static_cast<void *>(&pro));

    while (fu.wait_for(0s) == std::future_status::timeout) {
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<AllocRPC::ResponseType *>(resp_raw.get_buf());

    GAddr gaddr = GetGAddr(resp->start_page_id, 0);

    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    return gaddr;
}

Status PoolContext::FreePage(GAddr gaddr, size_t count) { DLOG_FATAL("Not Support"); }

// TODO: write lock
Status PoolContext::WriteBatch(GAddr gaddr, size_t size, void *buf) {

    page_id_t page_id = GetPageID(gaddr);
    offset_t in_page_offset = GetPageOffset(gaddr);
    LocalPageCache *pageCache;

    // TODO: more page
    SharedMutex *cache_lock;
    auto p_lock =
        __impl->m_ptl_cache_lock.find_or_emplace(page_id, []() { return new SharedMutex(); });
    cache_lock = p_lock.first->second;
    // 上读锁
    // DLOG("CN %u: write page %lu lock", __impl->m_client_id, page_id);
    cache_lock->lock_shared();

    auto p = __impl->m_page_table_cache.find(page_id);
    if (p == __impl->m_page_table_cache.end()) {
        size_t bb_pos = __impl->m_batch_cur;

        memcpy(reinterpret_cast<void *>(__impl->m_batch_buffer + bb_pos), buf, size);
        __impl->m_batch_list.push_back({gaddr, size, bb_pos});
        __impl->m_batch_cur += size;

        if (__impl->m_batch_cur > (64ul << 20)) {
            if (__impl->batch_flush_worker.joinable()) {
                __impl->batch_flush_worker.join();
            }

            std::vector<std::tuple<rchms::GAddr, size_t, offset_t>> batch_list;
            batch_list.swap(__impl->m_batch_list);

            __impl->batch_flush_worker = std::thread(
                [&, batch_list = std::move(batch_list), m_batch_buffer = __impl->m_batch_buffer]() {
                    std::unordered_map<rchms::GAddr, std::pair<size_t, offset_t>> umap;
                    for (auto &tu : batch_list) {
                        umap[std::get<0>(tu)] = std::make_pair(std::get<1>(tu), std::get<2>(tu));
                    }

                    for (auto &p : umap) {
                        Write(p.first, p.second.first, m_batch_buffer + p.second.second);
                    }

                    delete[] m_batch_buffer;
                });
            __impl->m_batch_buffer = new char[66ul << 20];
            __impl->m_batch_cur = 0;
        }

    } else {
        pageCache = p->second;
        memcpy(reinterpret_cast<void *>(
                   reinterpret_cast<uintptr_t>(__impl->m_cxl_format.page_data_start_addr) +
                   pageCache->offset + in_page_offset),
               buf, size);
        // 更新page访问请况统计
        pageCache->stats.add(getTimestamp());
    }

    // 解锁
    cache_lock->unlock_shared();
    // DLOG("CN %u: write page %lu unlock", __impl->m_client_id, page_id);
    return Status::OK;
}

const ClientOptions &PoolContext::GetOptions() const { return __impl->m_options; }

}  // namespace rchms

ClientContext::ClientContext() : m_cort_sched(8) {}

mac_id_t ClientContext::GetMacID() const {
    return m_client_id;
}

ClientConnection *ClientContext::GetConnection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != m_client_id, "Can't find self connection");
    if (mac_id == m_local_rack_daemon_connection.daemon_id) {
        return &m_local_rack_daemon_connection;
    }
    DLOG_FATAL("Can't find mac %d", mac_id);
}

CortScheduler &ClientContext::GetCortSched() { return m_cort_sched; }

/*********************** for test **************************/

namespace rchms {

void PoolContext::__DumpStats() {
    DLOG("local hit: %lu, local miss: %lu", __impl->m_stats.local_hit, __impl->m_stats.local_miss);
}

Status PoolContext::__TestDataSend1(int *array, size_t size) {
    using DataSend1RPC = RPC_TYPE_STRUCT(rpc_daemon::__testdataSend1);
    msgq::MsgBuffer req_raw =
        __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(DataSend1RPC::RequestType));
    auto req = reinterpret_cast<DataSend1RPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->size = size;
    for (size_t i = 0; i < req->size; i++) {
        req->data[i] = i + req->mac_id;
    }
    memcpy(req->data, array, size * sizeof(int));

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(DataSend1RPC::rpc_type, req_raw,
                                        msgq_general_promise_flag_cb, static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<DataSend1RPC::ResponseType *>(resp_raw.get_buf());
    // check
    assert(resp->size == size);
    for (size_t i = 0; i < resp->size; i++) {
        assert(resp->data[i] == array[i]);
    }
    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

Status PoolContext::__TestDataSend2(int *array, size_t size) {
    using DataSend2RPC = RPC_TYPE_STRUCT(rpc_daemon::__testdataSend2);
    msgq::MsgBuffer req_raw =
        __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(DataSend2RPC::RequestType));
    auto req = reinterpret_cast<DataSend2RPC::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;
    req->size = size;
    for (size_t i = 0; i < req->size; i++) {
        req->data[i] = i + req->mac_id;
    }
    memcpy(req->data, array, size * sizeof(int));

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(DataSend2RPC::rpc_type, req_raw,
                                        msgq_general_promise_flag_cb, static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
    }

    msgq::MsgBuffer resp_raw = fu.get();
    auto resp = reinterpret_cast<DataSend2RPC::ResponseType *>(resp_raw.get_buf());
    // check
    for (size_t i = 0; i < resp->size; i++) {
        assert(resp->data[i] == array[i]);
    }
    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

Status PoolContext::__NotifyPerf() {
    using __notify_rpc = RPC_TYPE_STRUCT(rpc_daemon::__notifyPerf);
    msgq::MsgBuffer req_raw =
        __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(__notify_rpc::RequestType));
    auto req = reinterpret_cast<__notify_rpc::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(__notify_rpc::rpc_type, req_raw,
                                        msgq_general_promise_flag_cb, static_cast<void *>(&pro));
    while (fu.wait_for(1ns) == std::future_status::timeout) {
    }
    msgq::MsgBuffer resp_raw = fu.get();
    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

Status PoolContext::__StopPerf() {
    using __stop_rpc = RPC_TYPE_STRUCT(rpc_daemon::__stopPerf);
    msgq::MsgBuffer req_raw = __impl->m_msgq_rpc->alloc_msg_buffer(sizeof(__stop_rpc::RequestType));
    auto req = reinterpret_cast<__stop_rpc::RequestType *>(req_raw.get_buf());
    req->mac_id = __impl->m_client_id;

    std::promise<msgq::MsgBuffer> pro;
    std::future<msgq::MsgBuffer> fu = pro.get_future();
    __impl->m_msgq_rpc->enqueue_request(__stop_rpc::rpc_type, req_raw, msgq_general_promise_flag_cb,
                                        static_cast<void *>(&pro));
    while (fu.wait_for(1ns) == std::future_status::timeout) {
    }
    msgq::MsgBuffer resp_raw = fu.get();
    __impl->m_msgq_rpc->free_msg_buffer(resp_raw);

    return Status::OK;
}

}  // namespace rchms
