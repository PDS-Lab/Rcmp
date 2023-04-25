#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "allocator.hpp"
#include "concurrent_queue.hpp"
#include "config.hpp"

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
    enum : uint8_t { REQ, RESP } msg_type : 1;
    uint8_t rpc_type;
    size_t size : 32;          // 实际数据大小
    offset_t buf_offset : 16;  // 根据MsgQueue::m_ring的地址
    msgq_callback_t cb;
    void *arg;

    static_assert(msgq_ring_buf_len < (1ul << 16), "");
};

struct MsgBuffer {
    size_t size() const;
    void *get_buf() const;

    MsgQueue *m_q;
    MsgHeader m_msg;
};

struct MsgQueue final {
    MsgQueue() = default;
    ~MsgQueue() = default;

    offset_t alloc_msg_buffer(size_t size);
    void enqueue_msg(MsgBuffer &msg_buf);
    uint32_t dequeue_msg(MsgHeader *hv, size_t max_deq);
    void free_msg_buffer(MsgBuffer &msg_buf);

    ConcurrentQueue<MsgHeader, 64, ConcurrentQueueProducerMode::MP, ConcurrentQueueConsumerMode::MC>
        msgq_q;
    RingAllocator<msgq_ring_buf_len> m_ra;
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

}  // namespace msgq