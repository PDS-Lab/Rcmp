#pragma once

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <atomic>
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "utils.hpp"

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
    rdma_event_channel *m_cm_channel_;
    ibv_context **m_ibv_ctxs_;
    int m_nr_dev_;

    std::map<ibv_context *, ibv_pd *> m_pd_map_;

   private:
    RDMAEnv() : m_active_(false) {}
    ~RDMAEnv();
    int __init__();
};

struct SgeWr {
    ibv_sge sge;
    ibv_send_wr wr;
};

struct RDMABatch {
    std::vector<SgeWr> m_sge_wrs_;
};

struct SyncData;

struct RDMAFuture {
    int get();
    /**
     * @return
     *  *  0 - ok
     *  *  1 - pending
     *  * -1 - error
     */
    int try_get();

    SyncData *m_sd_;
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
     * @brief 监听RDMA网卡IP与端口0
     *
     * 通过get_local_addr()获取监听ip与端口
     *
     * @return int
     */
    int listen(const std::string &ip);
    /**
     * @brief 连接RDMA网卡IP和对方端口
     *
     * @param ip
     * @param port
     * @return int
     */
    int connect(const std::string &ip, uint16_t port, const void *param, uint8_t param_size);

    std::pair<std::string, in_port_t> get_local_addr();
    std::pair<std::string, in_port_t> get_peer_addr();

    ibv_mr *register_memory(void *ptr, size_t size);
    ibv_mr *register_memory(size_t size);
    void deregister_memory(ibv_mr *mr, bool freed = true);

    // prep 操作对于同一个 batch 均为 thread-unsafety

    int prep_write(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                   uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_read(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                  uint64_t remote_addr, uint32_t rkey, bool inline_data);
    int prep_fetch_add(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                       uint32_t rkey, uint64_t n);
    int prep_cas(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                 uint32_t rkey, uint64_t expected, uint64_t desired);

    /**
     * 提交prep队列
     *
     * @warning
     *  * 该操作成功后会清空batch
     */
    RDMAFuture submit(RDMABatch &b);

    static std::function<void(RDMAConnection *conn, void *param)> m_hook_connect_;
    static std::function<void(RDMAConnection *conn)> m_hook_disconnect_;
    static void register_connect_hook(std::function<void(RDMAConnection *conn, void *param)> &&hook_connect);
    static void register_disconnect_hook(
        std::function<void(RDMAConnection *conn)> &&hook_disconnect);

    enum conn_type_t {
        SENDER,
        LISTENER,
    };
    conn_type_t conn_type;
    volatile bool m_stop_ : 1;
    bool m_atomic_support_ : 1;
    bool m_inline_support_ : 1;
    std::atomic<uint32_t> m_inflight_count_ = {0};  // 与 m_sending_lock_ 间隔 >64B
    ibv_comp_channel *m_comp_chan_;
    rdma_cm_id *m_cm_id_;
    ibv_pd *m_pd_;
    ibv_cq *m_cq_;

    std::thread *m_conn_handler_;

    bool m_rdma_conn_param_valid_();
    int m_init_ibv_connection_();
    void m_handle_connection_();
    int m_poll_conn_sd_wr_();
    void m_init_connection_(RDMAConnection *init_conn);
    static int m_acknowledge_sd_cqe_(int rc, ibv_wc wcs[]);
};

}  // namespace rdma_rc