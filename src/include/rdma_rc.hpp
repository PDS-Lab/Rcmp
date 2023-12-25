#pragma once

#include <rdma/rdma_cma.h>

#include <array>

#include "allocator.hpp"
#include "fiber_pool.hpp"
#include "promise.hpp"

namespace rdma_rc {

class RDMAEnv {
   public:
    RDMAEnv(const RDMAEnv &) = delete;
    RDMAEnv(RDMAEnv &&) = delete;
    RDMAEnv &operator=(const RDMAEnv &) = delete;
    RDMAEnv &operator=(RDMAEnv &&) = delete;

    static int init();
    static RDMAEnv &get_instance() {
        static RDMAEnv env;
        return env;
    }

    bool m_active_;
    rdma_event_channel *m_cm_client_channel_;
    rdma_event_channel *m_cm_server_channel_;
    ibv_context **m_ibv_ctxs_;
    int m_nr_dev_;

    std::map<ibv_context *, ibv_pd *> m_pd_map_;
    std::map<ibv_context *, ibv_comp_channel *> m_comp_chan_map_;
    std::map<ibv_context *, ibv_cq *> m_cq_map_;

   private:
    RDMAEnv() : m_active_(false) {}
    ~RDMAEnv();
    int __init__();
};

struct SgeWr {
    ibv_sge sge;
    ibv_send_wr wr;
};

struct RDMAConnection;

struct SyncData {
    uint32_t inflight;
    uint32_t now_ms;
    RDMAConnection *conn;
    volatile bool wc_finish;
    bool timeout;
    uint8_t props_size;
    std::array<priority_props *, 8> props;
    FutureControlBlock *cbk;

    SyncData() : cbk(ObjectPool<FutureControlBlock>().pop()) {}
    ~SyncData() { ObjectPool<FutureControlBlock>().put(cbk); }

    void *operator new(std::size_t size) { return ObjectPoolAllocator<SyncData>().allocate(1); }

    void operator delete(void *ptr) {
        ObjectPoolAllocator<SyncData>().deallocate(static_cast<SyncData *>(ptr), 1);
    }
};

struct RDMAFuture {
    int get();
    /**
     * @return
     *  *  0 - ok
     *  *  1 - pending
     *  * -1 - error
     */
    int try_get();

    std::unique_ptr<SyncData> m_sd_ = {nullptr};
};

struct RDMAConnection {
    // Global Options
    static int MAX_SEND_WR;
    static int MAX_SEND_SGE;
    static int CQE_NUM;
    static int RESOLVE_TIMEOUT_MS;
    static uint8_t RETRY_COUNT;
    static int RNR_RETRY_COUNT;
    static uint8_t INITIATOR_DEPTH;
    static int RESPONDER_RESOURCES;
    static int POLL_ENTRY_COUNT;
    static bool RDMA_TIMEOUT_ENABLE;
    static uint32_t RDMA_TIMEOUT_MS;

    RDMAConnection();
    ~RDMAConnection();

    /**
     * @brief Listening on RDMA card IP and port 0
     *
     * @param ip
     * @return int
     */
    int listen(const std::string &ip);
    /**
     * @brief Connecting the RDMA card IP and the other port
     *
     * @param ip
     * @param port
     * @param param
     * @param param_size
     * @return int
     */
    int connect(const std::string &ip, uint16_t port, const void *param, uint8_t param_size);

    std::pair<std::string, in_port_t> get_local_addr();
    std::pair<std::string, in_port_t> get_peer_addr();

    ibv_mr *register_memory(void *ptr, size_t size);
    ibv_mr *register_memory(size_t size);
    void deregister_memory(ibv_mr *mr, bool freed = true);

    // prep operations are thread-unsafety for the same `sge_vec`.

    int prep_write(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey, uint32_t length,
                   uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_read(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey, uint32_t length,
                  uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_fetch_add(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                       uint64_t remote_addr, uint32_t rkey, uint64_t n);
    int prep_cas(std::vector<SgeWr> &sge_vec, uint64_t local_addr, uint32_t lkey,
                 uint64_t remote_addr, uint32_t rkey, uint64_t expected, uint64_t desired);

    int prep_write(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint32_t length,
                   uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_read(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint32_t length,
                  uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_fetch_add(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                       uint32_t rkey, uint64_t n);
    int prep_cas(SgeWr *sge_wr, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                 uint32_t rkey, uint64_t expected, uint64_t desired);

    /**
     * @brief submit prep sge_vec
     */
    RDMAFuture submit(std::vector<SgeWr> &sge_vec);

    /**
     * @brief submit prep sgewr
     *
     * @warning The sge wr array must be reserved before future get
     *
     * @param begin
     * @param n
     * @return RDMAFuture
     */
    RDMAFuture submit(SgeWr *begin, size_t n);

    static std::function<void(rdma_cm_id *cm_id, void *param)> m_hook_connect_;
    static std::function<void(rdma_cm_id *cm_id)> m_hook_disconnect_;
    static void register_connect_hook(
        std::function<void(rdma_cm_id *cm_id, void *param)> &&hook_connect);
    static void register_disconnect_hook(std::function<void(rdma_cm_id *cm_id)> &&hook_disconnect);

    enum conn_type_t {
        INVALID,
        SENDER,
        LISTENER,
    };
    conn_type_t m_conn_type_;
    volatile bool m_stop_ : 1;
    bool m_atomic_support_ : 1;
    bool m_inline_support_ : 1;
    std::atomic<uint32_t> m_inflight_count_;
    ibv_comp_channel *m_comp_chan_;
    ibv_pd *m_pd_;
    ibv_cq *m_cq_;
    std::deque<rdma_cm_id *> m_cm_ids_;

    std::thread *m_conn_handler_;

    Mutex m_mu_;
    // std::unique_ptr<SyncData> m_current_sd_ = {nullptr};
    SgeWr *m_sw_head_ = nullptr;
    SgeWr *m_sw_tail_ = nullptr;

    bool m_rdma_conn_param_valid_();
    int m_init_last_ibv_subconnection_();
    void m_handle_connection_();
    int m_poll_conn_sd_wr_();
    static void m_init_last_subconnection_(RDMAConnection *init_conn);
    static int m_acknowledge_sd_cqe_(int rc, ibv_wc wcs[]);
    RDMAFuture m_submit_impl(SgeWr *sge_wrs, size_t n);
};

}  // namespace rdma_rc