#include "msg_queue.hpp"

#include <cstdint>

#include "utils.hpp"

void (*MsgQueue::__handlers[(1 << (sizeof(uint8_t) * 8)) - 1])(void *req, size_t size, void *resp, void *ctx);

MsgQueueManager::MsgQueueManager()
    : msgq_allocator(MAX_RING_NUM * RING_ELEM_SIZE, RING_ELEM_SIZE),
      ring_cnt(0),
      start_off(0),
      head_off(0),
      tail_off(0) {}
MsgQueueManager::~MsgQueueManager() {}

MsgQueue* MsgQueueManager::alloc_ring() {
    uintptr_t ring_off = msgq_allocator.allocate(1);
    MsgQueue* r = reinterpret_cast<MsgQueue*>(start_addr + ring_off);
    new (r) MsgQueue();
    ring_cnt++;
    return r;
}

MsgQueue::MsgQueue() {
    buf_head.head = RING_BUF_OFF;
    buf_head.tail = RING_BUF_OFF;
    buf_tail.head = RING_BUF_OFF;
    buf_tail.tail = RING_BUF_OFF;
}

void* MsgQueue::alloc_msg_buffer(size_t size) {
    uint32_t head, head_next;
    uint32_t entries;
    uint32_t temp_len = size + sizeof(MsgHeader);
    int32_t diff = 0;

    int success;
    while (true) {
        do {
            temp_len = size;
            head = buf_head.head;
            if (buf_tail.tail > head) {
                entries = buf_tail.tail - head;
            } else {
                entries = MsgQueueManager::RING_BUF_LEN - (head - buf_tail.tail);
            }

            // printf("temp_len = %d, entries = %d, head = %d, buf_tail = %d\n",temp_len, entries,
            // head, buf_tail.tail);

            if (temp_len >= entries) {
                return nullptr;
            } else {
                diff = head + temp_len - MsgQueueManager::RING_BUF_END_OFF;
                if (diff > 0) {
                    temp_len = MsgQueueManager::RING_BUF_END_OFF - head;
                }
            }
            head_next = MsgQueueManager::BUF_PLUS(head, temp_len);

            success = buf_head.head.compare_exchange_weak(head, head_next);
            // printf("success = %d\n", success);
        } while (UNLIKELY(success == 0));

        if (diff > 0) {
            int8_t* invalid = (int8_t*)((uintptr_t)this + head);
            *invalid = MsgQueueManager::INVALID_PADDING_FLAG;
        } else {
            return (void*)((uintptr_t)this + head + sizeof(MsgHeader));
        }
    }
    // printf("temp_len = %d, head = %d, head_next = %d, entries = %d\n", temp_len, head, head_next,
    // entries); printf("msg_ring_get finished\n");
    return nullptr;
}

void MsgQueue::enqueue_request(uint8_t rpc_type, void* obj_addr, size_t size,
                               void (*cb)(void* resp, size_t size, void* arg), void* arg) {
    size += sizeof(MsgHeader);

    MsgHeader* h = (MsgHeader*)obj_addr - 1;
    h->rpc_type = rpc_type;
    h->cb = cb;
    h->arg = arg;
    h->size = size;

    uint32_t head, head_next;
    uintptr_t obj_off = (uintptr_t)h - (uintptr_t)this;
    uint32_t temp_len = size;
    int8_t* invalid;
    bool flag = true;

    int success;
    while (flag) {
        do {
            temp_len = size;
            head = buf_head.tail;
            head_next = MsgQueueManager::BUF_PLUS(obj_off, size);
            if (head != obj_off) {
                invalid = (int8_t*)((uintptr_t)this + head);
                // printf("invalid = %d\n", *invalid);
                if (*invalid == MsgQueueManager::INVALID_PADDING_FLAG) {
                    temp_len = MsgQueueManager::RING_BUF_END_OFF - head;
                    head_next = RING_BUF_OFF;
                } else  // 若不是填充，则等到相同为止
                {
                    continue;
                }
            } else {
                flag = false;
            }

            success = buf_head.tail.compare_exchange_weak(head, head_next);
        } while (UNLIKELY(success == 0));
    }
    // printf("msg_ring_enq finished\n");
}

void MsgQueue::run_event_loop_once() {
    uint32_t tail, tail_next;
    uintptr_t obj_off;
    uint32_t temp_len;
    int8_t* invalid;
    bool flag = true;
    int32_t entries;

    int success;
    while (flag) {
        do {
            tail = buf_tail.head;

            entries = (buf_head.tail - tail);
            if (entries < 0) {
                entries = MsgQueueManager::RING_BUF_END_OFF - tail;
            } else if (entries == 0) {
                return;
            } else {
                temp_len = entries;
            }

            tail_next = MsgQueueManager::BUF_PLUS(tail, temp_len);

            invalid = (int8_t*)((uintptr_t)this + tail);
            if (*invalid == MsgQueueManager::INVALID_PADDING_FLAG) {
                temp_len = MsgQueueManager::RING_BUF_END_OFF - tail;
                tail_next = RING_BUF_OFF;
            } else {
                flag = false;
            }
            success = buf_tail.head.compare_exchange_weak(tail, tail_next);
        } while (UNLIKELY(success == 0));
    }
    // printf("tail_next = %d, tail = %d temp_len = %u, buf_head.tail = %d, RING_BUF_END_OFF =
    // %d\n", tail_next, tail, temp_len, r->buf_head.tail, RING_BUF_END_OFF);

    MsgHeader* h = (MsgHeader*)((uintptr_t)this + tail);
    while (temp_len) {
        if (h->rpc_type == MsgQueueManager::INVALID_PADDING_FLAG) {
            free_msg_buffer(h->data, 0);
            h = (MsgHeader*)buf;
            temp_len -= MsgQueueManager::RING_BUF_END_OFF - (uintptr_t)h;
        } else {
            if (h->msg_type == MsgHeader::REQ) {
                // TODO: alloc buf
                void *resp_buf;
                __handlers[h->rpc_type](h->data, h->size, resp_buf, ctx);
                // TODO: enqueue_request
                free_msg_buffer(h->data, h->size);
            } else {
                h->cb(h->data, h->size, h->arg);
            }
            h = (MsgHeader*)((uint8_t*)h + h->size + sizeof(MsgHeader));
            temp_len -= h->size + sizeof(MsgHeader);
        }
    }
}

void MsgQueue::free_msg_buffer(void* obj_addr, size_t size) {
    uint32_t tail, tail_next;
    uintptr_t obj_off = (uintptr_t)obj_addr - (uintptr_t)this - sizeof(MsgHeader);
    int8_t* invalid;
    bool flag = true;

    int success = 0;

    while (flag) {
        do {
            tail = buf_tail.tail;
            tail_next = MsgQueueManager::BUF_PLUS(obj_off, size);
            // printf("tail = %d, tail_next = %d, obj_off = %d\n", tail, tail_next, obj_off);
            if (tail != obj_off) {
                invalid = (int8_t*)((uintptr_t)this + tail);
                if (*invalid == MsgQueueManager::INVALID_PADDING_FLAG) {
                    tail_next = RING_BUF_OFF;
                } else  // 若不是填充，则等到相同为止
                {
                    continue;
                }
            } else {
                if (size == 0) {
                    tail_next = RING_BUF_OFF;
                }

                flag = false;
            }

            success = buf_tail.tail.compare_exchange_weak(tail, tail_next);
            // printf("success = %d, RING_BUF_END_OFF = %d, RING_BUF_OFF = %d, tail = %d, tail_next
            // = %d\n", success, RING_BUF_END_OFF, RING_BUF_OFF, tail, tail_next);
        } while (UNLIKELY(success == 0));
    }

    // printf("msg_ring_put finished\n");
}