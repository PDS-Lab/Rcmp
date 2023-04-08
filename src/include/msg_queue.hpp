#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "allocator.hpp"

/**
 * @brief
 *
 *   msgq_s
 *
 *      [  public msgq  ][         private cn msgq          ]
 */

namespace msgq {

struct MsgBuffer;
struct MsgQueue;

struct MsgUDPConnPacket {
    uintptr_t recv_q_off;
};

using msgq_handler_t = void (*)(MsgBuffer &req, void *ctx);
using msgq_callback_t = void (*)(MsgBuffer &resp, void *arg);

struct MsgHeader final {
    bool invalid_flag : 1;
    enum : uint8_t { REQ, RESP } msg_type : 1;
    uint8_t rpc_type;
    size_t size : 32;  // 实际数据大小
    msgq_callback_t cb;
    void *arg;

    uint8_t data[0];
};

struct MsgBuffer {
    size_t size() const;
    void *get_buf() const;

    MsgQueue *m_q;
    MsgHeader *m_msg;  // 指向MsgHeader的地址
    size_t m_size;     // 实际数据大小
};

struct MsgQueue final {
    MsgQueue();
    ~MsgQueue();

    MsgHeader *alloc_msg_buffer(size_t size);
    void enqueue_msg();
    void dequeue_msg(std::vector<MsgHeader *> &hv);
    void free_msg_buffer();

    union po_val_t {
        struct {
            uint32_t pos;
            uint32_t cnt;
        };
        uint64_t raw;
    };

    volatile po_val_t m_prod_head;
    volatile po_val_t m_prod_tail;
    volatile po_val_t m_cons_head;
    volatile po_val_t m_cons_tail;

    uint8_t m_ring[0];

    MsgHeader *at(size_t i);
    static void update_ht(volatile po_val_t *ht, volatile po_val_t *ht_);
};

struct MsgQueueNexus {
    constexpr static size_t max_msgq_handler = (1 << (sizeof(uint8_t) * 8));

    MsgQueueNexus(void *msgq_zone_start_addr);

    void register_req_func(uint8_t rpc_type, msgq_handler_t handler);

    static msgq_handler_t __handlers[max_msgq_handler];

    void *m_msgq_zone_start_addr;
    MsgQueue *m_public_msgq;
};

struct MsgQueueRPC {
    MsgQueueRPC(MsgQueueNexus *nexus, void *ctx);

    /**
     * @brief 申请发送buffer
     *
     * @warning 该操作是阻塞式调用
     *
     * @param size
     * @return MsgBuffer
     */
    MsgBuffer alloc_msg_buffer(size_t size);

    /**
     * @brief 入队请求
     *
     * @param rpc_type
     * @param msg_buf
     * @param cb
     * @param arg
     */
    void enqueue_request(uint8_t rpc_type, MsgBuffer &msg_buf, msgq_callback_t cb, void *arg);

    /**
     * @brief 入队回复
     *
     * @param req_buf
     * @param resp_buf
     */
    void enqueue_response(MsgBuffer &req_buf, MsgBuffer &resp_buf);

    /**
     * @brief rpc队列轮询
     *
     */
    void run_event_loop_once();

    /**
     * @brief 释放buffer
     *
     * @param msg_buf
     */
    void free_msg_buffer(MsgBuffer &msg_buf);

    MsgQueueNexus *m_nexus;
    MsgQueue *m_send_queue;
    MsgQueue *m_recv_queue;
    void *m_ctx;
};

struct MsgQueueManager {
    const static size_t RING_BUF_LEN = 2048;
    const static size_t RING_ELEM_SIZE = sizeof(MsgQueue) + RING_BUF_LEN;

    void *start_addr;
    uint32_t ring_cnt;
    std::unique_ptr<SingleAllocator> msgq_allocator;
    std::unique_ptr<MsgQueueNexus> nexus;
    std::unique_ptr<MsgQueueRPC> rpc;

    MsgQueue *allocQueue();
    void freeQueue(MsgQueue *msgq);
};

}  // namespace msgq