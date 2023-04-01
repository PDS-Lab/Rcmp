#include "msg_queue.hpp"

#include <cstdint>

#include "utils.hpp"

namespace msgq {

size_t MsgBuffer::size() {
    return m_size;
}

void* MsgBuffer::get_buf() {
    return m_msg->data;
}

    void MsgBuffer::reset(size_t size) {
        m_msg = m_q->alloc_msg_buffer(size);
        this->m_size = size;
    }

msgq_handler_t MsgQueueNexus::__handlers[max_msgq_handler];

MsgQueueNexus::MsgQueueNexus(void *msgq_zone_start_addr) : m_msgq_zone_start_addr(msgq_zone_start_addr) {}

    void MsgQueueNexus::register_req_func(uint8_t rpc_type, msgq_handler_t handler) {
        __handlers[rpc_type] = handler;
    }

    MsgQueueRPC::MsgQueueRPC(MsgQueueNexus *nexus, void *ctx) : m_nexus(nexus), m_ctx(ctx) {
        // TODO: set queue
    }

    MsgBuffer MsgQueueRPC::alloc_msg_buffer(size_t size) {
        MsgHeader *h = m_send_queue->alloc_msg_buffer(size);
        MsgBuffer buf;
        buf.m_size = size;
        buf.m_msg = h;
        buf.m_q = m_send_queue;
        return buf;
    }

    void MsgQueueRPC::enqueue_request(uint8_t rpc_type, MsgBuffer& msg_buf,
                         msgq_callback_t cb, void *arg) {
                            msg_buf.m_msg->cb = cb;
                            msg_buf.m_msg->arg = arg;
                            msg_buf.m_msg->msg_type = MsgHeader::REQ;
                            m_send_queue->enqueue_msg(rpc_type, msg_buf.m_msg, msg_buf.m_size);
                         }

    void MsgQueueRPC::run_event_loop_once() {
        MsgHeader* h;
        size_t temp_len;

        temp_len = m_recv_queue->dequeue_msg(&h);

    while (temp_len) {
        if (h->rpc_type == INVALID_PADDING_FLAG) {
            m_recv_queue->free_msg_buffer(h, 0, false);
            h = (MsgHeader*)m_recv_queue->ring_buf;
            temp_len -= MsgQueueManager::RING_BUF_LEN - (uintptr_t)h;
        } else {
            if (h->msg_type == MsgHeader::REQ) {
                MsgBuffer buf;
                buf.m_q = m_recv_queue;
                buf.m_msg = h;
                buf.m_size = h->size;
                MsgBuffer resp_buf;
                resp_buf.m_q = m_send_queue;
                resp_buf.m_msg->cb = h->cb;
                resp_buf.m_msg->arg = h->arg;
                resp_buf.m_msg->msg_type = MsgHeader::RESP;
                MsgQueueNexus::__handlers[h->rpc_type](buf, resp_buf, m_ctx);
                m_recv_queue->free_msg_buffer(h, h->size, true);
                m_send_queue->enqueue_msg(h->rpc_type, resp_buf.m_msg, resp_buf.m_size);
            } else {
                MsgBuffer buf;
                buf.m_q = m_recv_queue;
                buf.m_msg = h;
                buf.m_size = h->size;
                h->cb(buf, h->arg);
            }
            h = (MsgHeader*)((uint8_t*)h + h->size + sizeof(MsgHeader));
            temp_len -= h->size + sizeof(MsgHeader);
        }
    }
    }

    void MsgQueueRPC::free_msg_buffer(MsgBuffer& msg_buf) {
        msg_buf.m_q->free_msg_buffer(msg_buf.m_msg, msg_buf.m_size, true);
    }

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
    buf_head.head = 0;
    buf_head.tail = 0;
    buf_tail.head = 0;
    buf_tail.tail = 0;
}

MsgHeader * MsgQueue::alloc_msg_buffer(size_t size) {
    size += sizeof(MsgHeader);
    uint32_t head, head_next;
    uint32_t entries;
    uint32_t temp_len = size;
    int32_t diff = 0;

    bool success;
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
                diff = head + temp_len - MsgQueueManager::RING_BUF_LEN;
                if (diff > 0) {
                    temp_len = MsgQueueManager::RING_BUF_LEN - head;
                }
            }
            head_next = MsgQueueManager::BUF_PLUS(head, temp_len);

            success = buf_head.head.compare_exchange_weak(head, head_next);
            // printf("success = %d\n", success);
        } while (UNLIKELY(success == 0));

        if (diff > 0) {
            uint8_t* invalid = (uint8_t*)(ring_buf + head);
            *invalid = INVALID_PADDING_FLAG;
        } else {
            return (MsgHeader *)(ring_buf + head);
        }
    }
    // printf("temp_len = %d, head = %d, head_next = %d, entries = %d\n", temp_len, head, head_next,
    // entries); printf("msg_ring_get finished\n");
    return nullptr;
}

void MsgQueue::enqueue_msg(uint8_t rpc_type, MsgHeader *h, size_t size) {
    h->rpc_type = rpc_type;
    h->size = size;

    size += sizeof(MsgHeader);

    uint32_t head, head_next;
    uintptr_t obj_off = (uintptr_t)h - (uintptr_t)ring_buf;
    uint32_t temp_len = size;
    uint8_t* invalid;
    bool flag = true;

    bool success;
    while (flag) {
        do {
            temp_len = size;
            head = buf_head.tail;
            head_next = MsgQueueManager::BUF_PLUS(obj_off, size);
            if (head != obj_off) {
                invalid = (uint8_t*)(ring_buf + head);
                // printf("invalid = %d\n", *invalid);
                if (*invalid == INVALID_PADDING_FLAG) {
                    temp_len = MsgQueueManager::RING_BUF_LEN - head;
                    head_next = 0;
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

size_t MsgQueue::dequeue_msg(MsgHeader **start_h) {
    uint32_t tail, tail_next;
    uintptr_t obj_off;
    uint32_t temp_len;
    uint8_t* invalid;
    bool flag = true;
    int32_t entries;

    bool success;
    while (flag) {
        do {
            tail = buf_tail.head;

            entries = (buf_head.tail - tail);
            if (entries < 0) {
                entries = MsgQueueManager::RING_BUF_LEN - tail;
            } else if (entries == 0) {
                return 0;
            } else {
                temp_len = entries;
            }

            tail_next = MsgQueueManager::BUF_PLUS(tail, temp_len);

            invalid = (uint8_t*)(ring_buf + tail);
            if (*invalid == INVALID_PADDING_FLAG) {
                temp_len = MsgQueueManager::RING_BUF_LEN - tail;
                tail_next = 0;
            } else {
                flag = false;
            }
            success = buf_tail.head.compare_exchange_weak(tail, tail_next);
        } while (UNLIKELY(success == 0));
    }

    *start_h = (MsgHeader*)(ring_buf+ tail);
    return temp_len;

    // printf("tail_next = %d, tail = %d temp_len = %u, buf_head.tail = %d, RING_BUF_END_OFF =
    // %d\n", tail_next, tail, temp_len, r->buf_head.tail, RING_BUF_END_OFF);

   
}

void MsgQueue::free_msg_buffer(MsgHeader *h, size_t size, bool tail_valid) {
    uint32_t tail, tail_next;
    uintptr_t obj_off = (uintptr_t)h - (uintptr_t)ring_buf;
    uint8_t* invalid;
    bool flag = true;

    bool success = 0;
    while (flag) {
        do {
            tail = buf_tail.tail;
            tail_next = MsgQueueManager::BUF_PLUS(obj_off, size);
            // printf("tail = %d, tail_next = %d, obj_off = %d\n", tail, tail_next, obj_off);
            if (tail != obj_off) {
                invalid = (uint8_t*)(ring_buf + tail);
                if (*invalid == INVALID_PADDING_FLAG) {
                    tail_next = 0;
                } else  // 若不是填充，则等到相同为止
                {
                    continue;
                }
            } else {
                if (!tail_valid) {
                    tail_next = 0;
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

}