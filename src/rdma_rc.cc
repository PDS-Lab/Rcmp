#include "rdma_rc.hpp"

#include <arpa/inet.h>
#include <netdb.h>

#include <boost/fiber/operations.hpp>

#include "log.hpp"

namespace rdma_rc {

int RDMAConnection::MAX_SEND_WR = 64;
int RDMAConnection::MAX_SEND_SGE = 1;
int RDMAConnection::CQE_NUM = 128;
int RDMAConnection::RESOLVE_TIMEOUT_MS = 2000;
uint8_t RDMAConnection::RETRY_COUNT = 7;
int RDMAConnection::RNR_RETRY_COUNT = 7;
uint8_t RDMAConnection::INITIATOR_DEPTH = 2;
int RDMAConnection::RESPONDER_RESOURCES = 2;
int RDMAConnection::POLL_ENTRY_COUNT = 16;
bool RDMAConnection::RDMA_TIMEOUT_ENABLE = false;
uint32_t RDMAConnection::RDMA_TIMEOUT_MS = 2000;

std::function<void(rdma_cm_id *cm_id, void *param)> RDMAConnection::m_hook_connect_;
std::function<void(rdma_cm_id *cm_id)> RDMAConnection::m_hook_disconnect_;

bool RDMAConnection::m_rdma_conn_param_valid_() {
    ibv_device_attr device_attr;
    if (ibv_query_device(m_cm_ids_.back()->verbs, &device_attr) != 0) {
        DLOG_ERROR("ibv_query_device fail");
        return false;
    }
    m_atomic_support_ = device_attr.atomic_cap != IBV_ATOMIC_NONE;
    m_inline_support_ = m_cm_ids_.back()->verbs->device->transport_type != IBV_TRANSPORT_UNKNOWN;
    return device_attr.max_cqe >= CQE_NUM && device_attr.max_qp_wr >= MAX_SEND_WR &&
           device_attr.max_sge >= MAX_SEND_SGE &&
           device_attr.max_qp_rd_atom >= RESPONDER_RESOURCES &&
           device_attr.max_qp_init_rd_atom >= RESPONDER_RESOURCES &&
           device_attr.max_qp_rd_atom >= INITIATOR_DEPTH &&
           device_attr.max_qp_init_rd_atom >= INITIATOR_DEPTH;
}

int RDMAEnv::init() { return get_instance().__init__(); }

int RDMAEnv::__init__() {
    m_cm_client_channel_ = rdma_create_event_channel();
    if (!m_cm_client_channel_) {
        DLOG_ERROR("rdma_create_event_channel fail");
        return -1;
    }
    m_cm_server_channel_ = rdma_create_event_channel();
    if (!m_cm_server_channel_) {
        DLOG_ERROR("rdma_create_event_channel fail");
        return -1;
    }

    m_ibv_ctxs_ = rdma_get_devices(&m_nr_dev_);
    if (!m_ibv_ctxs_) {
        DLOG_ERROR("rdma_get_devices fail");
        return -1;
    }

    for (int i = 0; i < m_nr_dev_; ++i) {
        ibv_pd *pd = ibv_alloc_pd(m_ibv_ctxs_[i]);
        if (!pd) {
            DLOG_ERROR("ibv_alloc_pd fail");
            return -1;
        }
        ibv_comp_channel *comp_chan = ibv_create_comp_channel(m_ibv_ctxs_[i]);
        if (!comp_chan) {
            DLOG_ERROR("ibv_create_comp_channel fail");
            return -1;
        }
        m_pd_map_.emplace(m_ibv_ctxs_[i], pd);
        m_comp_chan_map_.emplace(m_ibv_ctxs_[i], comp_chan);
    }

    m_active_ = true;

    return 0;
}

RDMAEnv::~RDMAEnv() {
    for (auto &pd : m_pd_map_) {
        ibv_dealloc_pd(pd.second);
    }
    for (auto &comp_chan : m_comp_chan_map_) {
        ibv_destroy_comp_channel(comp_chan.second);
    }
    for (auto &cq : m_cq_map_) {
        ibv_destroy_cq(cq.second);
    }
    rdma_free_devices(m_ibv_ctxs_);
    rdma_destroy_event_channel(m_cm_client_channel_);
    rdma_destroy_event_channel(m_cm_server_channel_);
}

RDMAConnection::RDMAConnection()
    : m_stop_(false), m_pd_(nullptr), m_conn_type_(INVALID), m_inflight_count_(0) {}
RDMAConnection::~RDMAConnection() {
    m_stop_ = true;
    switch (m_conn_type_) {
        case SENDER:
            for (auto &cm_id : m_cm_ids_) {
                if (cm_id == nullptr) continue;
                rdma_disconnect(cm_id);
                rdma_destroy_qp(cm_id);
            }
            break;
        case LISTENER:
            m_conn_handler_->join();
            break;
        case INVALID:
            return;
    }
    for (auto &cm_id : m_cm_ids_) {
        if (cm_id == nullptr) continue;
        rdma_destroy_id(cm_id);
    }
}

static ibv_cq *create_cq(ibv_context *verbs, ibv_comp_channel *comp_chan) {
    auto it = RDMAEnv::get_instance().m_cq_map_.find(verbs);
    if (it != RDMAEnv::get_instance().m_cq_map_.end()) {
        return it->second;
    }

    ibv_cq *cq = ibv_create_cq(verbs, RDMAConnection::CQE_NUM, nullptr, comp_chan, 0);
    if (!cq) {
        DLOG_ERROR("ibv_create_cq fail");
        return nullptr;
    }

    if (ibv_req_notify_cq(cq, 0)) {
        DLOG_ERROR("ibv_req_notify_cq fail");
        return nullptr;
    }

    RDMAEnv::get_instance().m_cq_map_.emplace(verbs, cq);

    return cq;
}

int RDMAConnection::m_init_last_ibv_subconnection_() {
    if (!m_rdma_conn_param_valid_()) {
        DLOG_ERROR("rdma_conn_param_valid fail");
        return -1;
    }

    auto cm_id = m_cm_ids_.back();

    m_pd_ = RDMAEnv::get_instance().m_pd_map_[cm_id->verbs];
    if (!m_pd_) {
        DLOG_ERROR("ibv_alloc_pd fail");
        return -1;
    }

    m_comp_chan_ = RDMAEnv::get_instance().m_comp_chan_map_[cm_id->verbs];
    if (!m_comp_chan_) {
        DLOG_ERROR("ibv_create_comp_channel fail");
        return -1;
    }

    m_cq_ = create_cq(cm_id->verbs, m_comp_chan_);

    ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = MAX_SEND_WR;
    qp_attr.cap.max_send_sge = MAX_SEND_SGE;
    qp_attr.cap.max_recv_wr = MAX_SEND_WR;
    qp_attr.cap.max_recv_sge = MAX_SEND_SGE;
    qp_attr.cap.max_inline_data = 64;  // TODO
    qp_attr.send_cq = m_cq_;
    qp_attr.recv_cq = m_cq_;

    if (rdma_create_qp(cm_id, m_pd_, &qp_attr)) {
        DLOG_ERROR("rdma_create_qp fail");
        return -1;
    }

    return 0;
}

int RDMAConnection::listen(const std::string &ip) {
    m_conn_type_ = LISTENER;

    rdma_cm_id *cm_id = nullptr;

    if (rdma_create_id(RDMAEnv::get_instance().m_cm_server_channel_, &cm_id, NULL, RDMA_PS_TCP)) {
        DLOG_ERROR("rdma_create_id fail");
        return -1;
    }

    m_cm_ids_.push_back(cm_id);

    sockaddr_in sin;
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = inet_addr(ip.c_str());
    sin.sin_port = 0;
    if (sin.sin_addr.s_addr == INADDR_NONE) {
        DLOG_ERROR("inet_addr fail");
        return -1;
    }

    if (rdma_bind_addr(cm_id, (struct sockaddr *)&sin)) {
        DLOG_ERROR("rdma_bind_addr fail");
        return -1;
    }

    if (rdma_listen(cm_id, 1)) {
        DLOG_ERROR("rdma_listen fail");
        return -1;
    }

    DLOG("%s:%d", get_local_addr().first.c_str(), get_local_addr().second);

    m_pd_ = RDMAEnv::get_instance().m_pd_map_[cm_id->verbs];
    if (!m_pd_) {
        DLOG_ERROR("ibv_alloc_pd fail");
        return -1;
    }

    m_conn_handler_ = new std::thread(&RDMAConnection::m_handle_connection_, this);
    if (!m_conn_handler_) {
        DLOG_ERROR("rdma connect fail");
        return -1;
    }

    return 0;
}

int RDMAConnection::connect(const std::string &ip, uint16_t port, const void *param,
                            uint8_t param_size) {
    m_conn_type_ = SENDER;

    rdma_cm_id *cm_id = nullptr;

    rdma_event_channel *m_cm_channel_ = RDMAEnv::get_instance().m_cm_client_channel_;

    if (rdma_create_id(m_cm_channel_, &cm_id, NULL, RDMA_PS_TCP)) {
        DLOG_ERROR("rdma_create_id fail");
        return -1;
    }

    m_cm_ids_.push_back(cm_id);

    addrinfo *res;
    if (getaddrinfo(ip.c_str(), std::to_string(htons(port)).c_str(), NULL, &res) < 0) {
        DLOG_ERROR("getaddrinfo fail");
        return -1;
    }

    addrinfo *addr_tmp = nullptr;
    for (addr_tmp = res; addr_tmp; addr_tmp = addr_tmp->ai_next) {
        if (!rdma_resolve_addr(cm_id, NULL, addr_tmp->ai_addr, RESOLVE_TIMEOUT_MS)) {
            break;
        }
    }
    if (!addr_tmp) {
        DLOG_ERROR("rdma_resolve_addr fail");
        return -1;
    }

    rdma_cm_event *event;
    if (rdma_get_cm_event(m_cm_channel_, &event)) {
        DLOG_ERROR("rdma_get_cm_event fail");
        return -1;
    }

    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        DLOG_ERROR("RDMA_CM_EVENT_ADDR_RESOLVED fail: %s", rdma_event_str(event->event));
        return -1;
    }

    rdma_ack_cm_event(event);

    if (rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS)) {
        DLOG_ERROR("rdma_resolve_route fail");
        return -1;
    }

    if (rdma_get_cm_event(m_cm_channel_, &event)) {
        DLOG_ERROR("rdma_get_cm_event fail");
        return -1;
    }

    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        DLOG_ERROR("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
        return -1;
    }

    rdma_ack_cm_event(event);

    if (m_init_last_ibv_subconnection_()) {
        return -1;
    }

    rdma_conn_param conn_param = {};
    conn_param.responder_resources = RESPONDER_RESOURCES;
    conn_param.initiator_depth = INITIATOR_DEPTH;
    conn_param.rnr_retry_count = RNR_RETRY_COUNT;
    conn_param.retry_count = RETRY_COUNT;
    conn_param.private_data = param;
    conn_param.private_data_len = param_size;

    // TODO: Two daemon interconnections are prone to anomalies and fail to connect.

    if (rdma_connect(cm_id, &conn_param)) {
        DLOG_ERROR("rdma_connect fail");
        return -1;
    }

    if (rdma_get_cm_event(m_cm_channel_, &event)) {
        DLOG_ERROR("rdma_get_cm_event fail");
        return -1;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        DLOG_ERROR("RDMA_CM_EVENT_ESTABLISHED fail: %p %s", event->id,
                   rdma_event_str(event->event));
        return -1;
    }

    rdma_ack_cm_event(event);

    // for remote DISCONNECT
    cm_id->context = this;

    return 0;
}

void RDMAConnection::m_handle_connection_() {
    struct rdma_cm_event *event;

    while (!m_stop_) {
        if (rdma_get_cm_event(RDMAEnv::get_instance().m_cm_server_channel_, &event)) {
            DLOG_ERROR("rdma_get_cm_event fail");
            return;
        }

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            struct rdma_cm_id *cm_id = event->id;

            uint8_t param_buf[1ul << (sizeof(event->param.conn.private_data_len) * 8)];
            memcpy(param_buf, event->param.conn.private_data, event->param.conn.private_data_len);

            rdma_ack_cm_event(event);

            m_hook_connect_(cm_id, param_buf);
        } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
            rdma_ack_cm_event(event);
        } else {
            struct rdma_cm_id *cm_id = event->id;
            rdma_ack_cm_event(event);

            if (m_hook_disconnect_) m_hook_disconnect_(cm_id);
        }
    }
}

void RDMAConnection::m_init_last_subconnection_(RDMAConnection *init_conn) {
    if (init_conn->m_init_last_ibv_subconnection_()) {
        return;
    }

    rdma_conn_param conn_param = {};
    conn_param.responder_resources = RESPONDER_RESOURCES;
    conn_param.initiator_depth = INITIATOR_DEPTH;
    conn_param.rnr_retry_count = RNR_RETRY_COUNT;
    conn_param.retry_count = RETRY_COUNT;

    if (rdma_accept(init_conn->m_cm_ids_.back(), &conn_param)) {
        DLOG_ERROR("rdma_accept fail");
        return;
    }
}

std::pair<std::string, in_port_t> RDMAConnection::get_local_addr() {
    sockaddr_in *sin = (sockaddr_in *)rdma_get_local_addr(m_cm_ids_.front());
    return std::make_pair(inet_ntoa(sin->sin_addr), (sin->sin_port));
}
std::pair<std::string, in_port_t> RDMAConnection::get_peer_addr() {
    sockaddr_in *sin = (sockaddr_in *)rdma_get_peer_addr(m_cm_ids_.front());
    return std::make_pair(inet_ntoa(sin->sin_addr), (sin->sin_port));
}

ibv_mr *RDMAConnection::register_memory(size_t size) {
    void *ptr = aligned_alloc(4096, size);
    if (!ptr) {
        DLOG_ERROR("aligned_alloc fail");
        return nullptr;
    }
    return register_memory(ptr, size);
}

ibv_mr *RDMAConnection::register_memory(void *ptr, size_t size) {
    uint32_t access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    if (m_atomic_support_) {
        access |= IBV_ACCESS_REMOTE_ATOMIC;
    }
    ibv_mr *mr = ibv_reg_mr(m_pd_, ptr, size, access);
    if (!mr) {
        DLOG_ERROR("ibv_reg_mr fail");
        return nullptr;
    }
    return mr;
}

void RDMAConnection::deregister_memory(ibv_mr *mr, bool freed) {
    ibv_dereg_mr(mr);
    if (freed) {
        free(mr->addr);
    }
}

void RDMAConnection::register_connect_hook(
    std::function<void(rdma_cm_id *cm_id, void *param)> &&hook_connect) {
    m_hook_connect_ =
        std::forward<std::function<void(rdma_cm_id * cm_id, void *param)>>(hook_connect);
}

void RDMAConnection::register_disconnect_hook(
    std::function<void(rdma_cm_id *cm_id)> &&hook_disconnect) {
    m_hook_disconnect_ = std::forward<std::function<void(rdma_cm_id * cm_id)>>(hook_disconnect);
}

int RDMAConnection::prep_write(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                               uint32_t length, uint64_t remote_addr, uint32_t rkey,
                               bool inline_data) {
    sge_vec.push_back({});
    return prep_write(&sge_vec.back(), local_addr, lkey, length, remote_addr, rkey, inline_data);
}

int RDMAConnection::prep_read(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                              uint32_t length, uint64_t remote_addr, uint32_t rkey,
                              bool inline_data) {
    sge_vec.push_back({});
    return prep_read(&sge_vec.back(), local_addr, lkey, length, remote_addr, rkey, inline_data);
}

int RDMAConnection::prep_fetch_add(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                                   uint64_t remote_addr, uint32_t rkey, uint64_t n) {
    sge_vec.push_back({});
    return prep_fetch_add(&sge_vec.back(), local_addr, lkey, remote_addr, rkey, n);
}

int RDMAConnection::prep_cas(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                             uint64_t remote_addr, uint32_t rkey, uint64_t expected,
                             uint64_t desired) {
    sge_vec.push_back({});
    return prep_cas(&sge_vec.back(), local_addr, lkey, remote_addr, rkey, expected, desired);
}

int RDMAConnection::prep_write(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey, bool inline_data) {
    DEBUGY(inline_data && !m_inline_support_) {
        errno = EPERM;
        DLOG_ERROR("rdma write: this device don't support inline write");
        return -1;
    }

    *sge_wr = {ibv_sge{.addr = local_addr, .length = length, .lkey = lkey},
               ibv_send_wr{.num_sge = 1,
                           .opcode = IBV_WR_RDMA_WRITE,
                           .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}}};
    if (inline_data) sge_wr->wr.send_flags = IBV_SEND_INLINE;
    return 0;
}

int RDMAConnection::prep_read(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint32_t length,
                              uint64_t remote_addr, uint32_t rkey, bool inline_data) {
    DEBUGY(inline_data && !m_inline_support_) {
        errno = EPERM;
        DLOG_ERROR("rdma read: this device don't support inline read");
        return -1;
    }

    *sge_wr = {ibv_sge{.addr = local_addr, .length = length, .lkey = lkey},
               ibv_send_wr{.num_sge = 1,
                           .opcode = IBV_WR_RDMA_READ,
                           .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}}};
    if (inline_data) sge_wr->wr.send_flags = IBV_SEND_INLINE;
    return 0;
}

int RDMAConnection::prep_fetch_add(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey,
                                   uint64_t remote_addr, uint32_t rkey, uint64_t n) {
    DEBUGY(!m_atomic_support_) {
        errno = EPERM;
        DLOG_ERROR("rdma fetch add: this device don't support atomic operations");
        return -1;
    }
    DEBUGY(remote_addr % 8) {
        errno = EINVAL;
        DLOG_ERROR("rdma fetch add: remote addr must be 8-byte aligned");
        return -1;
    }

    *sge_wr = {ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey},
               ibv_send_wr{
                   .num_sge = 1,
                   .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
                   .wr = {.atomic =
                              {
                                  .remote_addr = remote_addr,
                                  .compare_add = n,
                                  .rkey = rkey,
                              }},
               }};

    return 0;
}

int RDMAConnection::prep_cas(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey,
                             uint64_t remote_addr, uint32_t rkey, uint64_t expected,
                             uint64_t desired) {
    DEBUGY(!m_atomic_support_) {
        errno = EPERM;
        DLOG_ERROR("rdma cas: this device don't support atomic operations");
        return -1;
    }
    DEBUGY(remote_addr % 8) {
        errno = EINVAL;
        DLOG_ERROR("rdma cas: remote addr must be 8-byte aligned");
        return -1;
    }

    *sge_wr = {ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey},
               ibv_send_wr{
                   .num_sge = 1,
                   .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
                   .wr = {.atomic =
                              {
                                  .remote_addr = remote_addr,
                                  .compare_add = expected,
                                  .swap = desired,
                                  .rkey = rkey,
                              }},
               }};

    return 0;
}

RDMAFuture RDMAConnection::submit(std::vector<SgeWr> &sge_vec) {
    return m_submit_impl(sge_vec.data(), sge_vec.size());
}

RDMAFuture RDMAConnection::submit(SgeWr *begin, size_t n) { return m_submit_impl(begin, n); }

RDMAFuture RDMAConnection::m_submit_impl(SgeWr *sge_wrs, size_t n) {
    RDMAFuture fu;

    for (size_t i = 0; i < n; ++i) {
        sge_wrs[i].wr.sg_list = &sge_wrs[i].sge;
        sge_wrs[i].wr.next = &sge_wrs[i + 1].wr;
    }

    // std::unique_lock<Mutex> lck(m_mu_);

    auto &m_current_sd_ = fu.m_sd_;
    if (m_current_sd_ == nullptr) {
        m_current_sd_ = std::make_unique<SyncData>();
        m_current_sd_->conn = this;
        m_current_sd_->timeout = false;
        m_current_sd_->inflight = n;
        m_current_sd_->wc_finish = false;
        m_current_sd_->props_size = 1;
        m_current_sd_->props[0] = &boost::this_fiber::properties<priority_props>();

        m_sw_head_ = sge_wrs;
        m_sw_tail_ = sge_wrs + n - 1;
    } else {
        DLOG_FATAL("Not Support for multi thread submit");

        // DLOG_ASSERT(m_current_sd_->props_size <= m_current_sd_->props.size());

        // fu.m_sd_ = m_current_sd_;
        // fu.m_sd_->inflight += n;
        // fu.m_sd_->props[fu.m_sd_->props_size](boost::this_fiber::properties<priority_props>());
        // fu.m_sd_->props_size++;

        // DLOG_ASSERT(m_sw_tail_ != nullptr);
        // m_sw_tail_->wr.next = &sge_wrs->wr;
        // m_sw_tail_ = sge_wrs + n - 1;

        // boost::this_fiber::properties<priority_props>().set_low_priority();

        // return fu;
    }

    // fu.m_sd_.swap(m_current_sd_);

    SgeWr *sge_wr_list = nullptr;
    SgeWr *sge_wr_list_tail = nullptr;

    std::swap(sge_wr_list, m_sw_head_);
    std::swap(sge_wr_list_tail, m_sw_tail_);

    // Select a qp transfer that needs to be thread-safe
    auto cm_id = m_cm_ids_.front();
    m_cm_ids_.pop_front();
    m_cm_ids_.push_back(cm_id);

    // lck.unlock();

    // It's now thread-safe to handle send wr

    boost::this_fiber::properties<priority_props>().set_low_priority();

    uint64_t wr_id = reinterpret_cast<uint64_t>(fu.m_sd_.get());

    ibv_send_wr *wr_head = &sge_wr_list->wr;
    sge_wr_list_tail->wr.wr_id = wr_id;
    sge_wr_list_tail->wr.next = nullptr;
    sge_wr_list_tail->wr.send_flags |= IBV_SEND_SIGNALED;

    // Detecting the number of wr's currently being sent
    uint32_t inflight = m_inflight_count_.load(std::memory_order_acquire);
    do {
        if (UNLIKELY((int)(inflight + fu.m_sd_->inflight) > MAX_SEND_WR)) {
            errno = ENOSPC;
            DLOG_ERROR("ibv_post_send too much inflight wr");
            boost::this_fiber::yield();
            m_poll_conn_sd_wr_();
            inflight = m_inflight_count_.load(std::memory_order_acquire);
            continue;
        }
        // printf("inflight+: %u\n", inflight);
    } while (!m_inflight_count_.compare_exchange_weak(inflight, inflight + fu.m_sd_->inflight,
                                                      std::memory_order_acquire));

    if (RDMAConnection::RDMA_TIMEOUT_ENABLE) fu.m_sd_->now_ms = getMsTimestamp();

    struct ibv_send_wr *bad_send_wr;
    int ret = ibv_post_send(cm_id->qp, wr_head, &bad_send_wr);
    if (UNLIKELY(ret != 0)) {
        DLOG_ERROR("ibv_post_send fail");
        goto need_retry;
    }

    return fu;

need_retry:

    fu.m_sd_ = nullptr;
    return fu;
}

int RDMAConnection::m_acknowledge_sd_cqe_(int rc, ibv_wc wcs[]) {
    for (int i = 0; i < rc; ++i) {
        auto &wc = wcs[i];

        SyncData *sd = reinterpret_cast<SyncData *>(wc.wr_id);
        sd->conn->m_inflight_count_.fetch_sub(sd->inflight, std::memory_order_release);

        // printf("inflight-: %u\n", sd->conn->m_inflight_count_.load());

        if (LIKELY(IBV_WC_SUCCESS == wc.status)) {
            // Break out as operation completed successfully
            if (LIKELY(!sd->timeout)) {
                sd->wc_finish = true;
                sd->cbk->set_value();
                for (size_t i = 0; i < sd->props_size; ++i) {
                    sd->props[i]->set_high_priority();
                }
            }
        } else {
            fprintf(stderr, "cmd_send status error: %s\n", ibv_wc_status_str(wc.status));
            return -1;
        }
    }
    return 0;
}

int RDMAFuture::try_get() {
    if (UNLIKELY(m_sd_->timeout)) {
        errno = ETIMEDOUT;
        DLOG_ERROR("rdma task timeout");
        return -1;
    }

    if (UNLIKELY(m_sd_->conn->m_poll_conn_sd_wr_() != 0)) {
        return -1;
    }

    if (m_sd_->wc_finish) {
        boost::this_fiber::properties<priority_props>().set_low_priority();
        return 0;
    }

    // timeout detection
    if (RDMAConnection::RDMA_TIMEOUT_ENABLE) {
        uint32_t now = getMsTimestamp();
        if (UNLIKELY(now - m_sd_->now_ms > RDMAConnection::RDMA_TIMEOUT_MS)) {
            m_sd_->timeout = true;
            errno = ETIMEDOUT;
            DLOG_ERROR("rdma task timeout");
            return -1;
        }
    }

    return 1;
}

int RDMAFuture::get() {
    m_sd_->cbk->get();

    boost::this_fiber::properties<priority_props>().set_low_priority();

    return 0;

    // while (1) {
    //     int ret = try_get();
    //     if (ret == 0) {
    //         return 0;
    //     } else if (UNLIKELY(ret == -1)) {
    //         return -1;
    //     }
    //     boost::this_fiber::yield();
    // }
}

int RDMAConnection::m_poll_conn_sd_wr_() {
    struct ibv_wc wcs[RDMAConnection::POLL_ENTRY_COUNT];

    int rc = ibv_poll_cq(m_cq_, RDMAConnection::POLL_ENTRY_COUNT, wcs);
    if (UNLIKELY(rc < 0)) {
        DLOG_ERROR("ibv_poll_cq fail");
        return -1;
    }

    int ret = RDMAConnection::m_acknowledge_sd_cqe_(rc, wcs);
    if (UNLIKELY(ret == -1)) {
        DLOG_ERROR("acknowledge_cqe fail");
        return -1;
    }

    return 0;
}

}  // namespace rdma_rc