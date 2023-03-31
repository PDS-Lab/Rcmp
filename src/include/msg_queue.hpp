#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "allocator.hpp"

struct MsgHeader final {
    uint8_t rpc_type;
    enum : uint8_t { REQ, RESP } msg_type;
    size_t size;
    void (*cb)(void *resp, size_t size, void *arg);
    void *arg;

    uint8_t data[0];
};

struct MsgQueue final {
    MsgQueue();
    ~MsgQueue();

    void *alloc_msg_buffer(size_t size);
    void enqueue_request(uint8_t rpc_type, void *obj_addr, size_t size,
                         void (*cb)(void *resp, size_t size, void *arg), void *arg);
    void run_event_loop_once();
    void free_msg_buffer(void *obj_addr, size_t size);

    struct msg_ring_headtail {
        std::atomic<uint32_t> head; /**< prod/consumer head. */
        std::atomic<uint32_t> tail; /**< prod/consumer tail. */
    };

    static void (*__handlers[(1 << (sizeof(uint8_t) * 8)) - 1])(void *req, size_t size, void *resp, void *ctx);

    void *ctx;
    msg_ring_headtail buf_head;  // enq
    msg_ring_headtail buf_tail;  // deq

    uint8_t buf[0];

    constexpr static size_t RING_BUF_OFF = 24;
};

struct MsgQueueManager {
    const static size_t MAX_RING_NUM = 16;
    const static size_t RING_BUF_LEN = 2048;
    const static size_t RING_ELEM_SIZE = sizeof(MsgQueue) + RING_BUF_LEN;
    const static size_t RING_BUF_OFF = sizeof(MsgQueue);
    const static size_t RING_BUF_END_OFF = sizeof(MsgQueue) + RING_BUF_LEN;
    const static int8_t INVALID_PADDING_FLAG = -1;

    uintptr_t start_addr;
    uint32_t ring_cnt;
    uint64_t start_off;  // 下一个可用的偏移位置
    uintptr_t head_off;
    uintptr_t tail_off;  // 相对偏移
    SingleAllocator msgq_allocator;

    MsgQueueManager();
    ~MsgQueueManager();

    MsgQueue *alloc_ring();
    static uint32_t BUF_PLUS(uint32_t p, uint32_t len) {
        return (((p) + (len)-RING_BUF_OFF) % (RING_BUF_LEN) + RING_BUF_OFF);
    }
    void free_ring(MsgQueue *msgq);
};