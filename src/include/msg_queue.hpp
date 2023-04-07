#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

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

constexpr static uint8_t INVALID_PADDING_FLAG = 255;

struct MsgHeader final {
    union {
        uint8_t rpc_type;
        uint8_t invalid_flag;
    };
    enum : uint8_t { REQ, RESP } msg_type;
    size_t size;  // 实际数据大小
    msgq_callback_t cb;
    void *arg;

    uint8_t data[0];
};

struct MsgDeqIter {
    MsgHeader* operator*();
    MsgDeqIter& operator++();
    bool operator==(const MsgDeqIter &it);
    bool operator!=(const MsgDeqIter &it);

    MsgQueue *q;
    MsgHeader* h;
    size_t rest_len;
};

struct MsgBuffer {
    size_t size() const;
    void *get_buf() const;

    MsgQueue *m_q;
    MsgHeader *m_msg;  // 指向MsgHeader的地址
    size_t m_size;     // 实际数据大小
};

struct MsgQueue final {
    constexpr static size_t RING_BUF_OFF = 16;

    MsgQueue();
    ~MsgQueue();

    MsgDeqIter end();
    MsgHeader *alloc_msg_buffer(size_t size);
    void enqueue_msg(uint8_t rpc_type, MsgHeader *h, size_t size);
    MsgDeqIter dequeue_msg();
    void free_msg_buffer(MsgHeader *h, size_t size, bool tail_valid);

    struct msg_ring_headtail {
        std::atomic<uint32_t> head; /**< prod/consumer head. */
        std::atomic<uint32_t> tail; /**< prod/consumer tail. */
    };

    msg_ring_headtail buf_head;  // enq
    msg_ring_headtail buf_tail;  // deq

    uint8_t ring_buf[0];

    void enqueue_invalid_msg(MsgHeader* h);
};

struct MsgQueueNexus {
    constexpr static size_t max_msgq_handler = (1 << (sizeof(uint8_t) * 8)) - 1;

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