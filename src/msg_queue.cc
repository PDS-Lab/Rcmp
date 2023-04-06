#include "msg_queue.hpp"

#include <cstdint>

#include "log.hpp"
#include "utils.hpp"

namespace msgq {

msgq_handler_t MsgQueueNexus::__handlers[max_msgq_handler];

size_t MsgBuffer::size() const { return m_size; }
void* MsgBuffer::get_buf() const { return m_msg->data; }

MsgQueueNexus::MsgQueueNexus(void* msgq_zone_start_addr)
    : m_msgq_zone_start_addr(msgq_zone_start_addr),
      m_public_msgq(reinterpret_cast<MsgQueue*>(msgq_zone_start_addr)) {}

void MsgQueueNexus::register_req_func(uint8_t rpc_type, msgq_handler_t handler) {
    DLOG_ASSERT(rpc_type != INVALID_PADDING_FLAG);
    __handlers[rpc_type] = handler;
}

MsgQueueRPC::MsgQueueRPC(MsgQueueNexus* nexus, void* ctx) : m_nexus(nexus), m_ctx(ctx) {}

MsgBuffer MsgQueueRPC::alloc_msg_buffer(size_t size) {
    MsgHeader* h = m_send_queue->alloc_msg_buffer(size);
    MsgBuffer buf;
    buf.m_size = size;
    buf.m_msg = h;
    buf.m_q = m_send_queue;
    return buf;
}

void MsgQueueRPC::enqueue_request(uint8_t rpc_type, MsgBuffer& msg_buf, msgq_callback_t cb,
                                  void* arg) {
    msg_buf.m_msg->msg_type = MsgHeader::REQ;
    msg_buf.m_msg->cb = cb;
    msg_buf.m_msg->arg = arg;
    m_send_queue->enqueue_msg(rpc_type, msg_buf.m_msg, msg_buf.m_size);
}

void MsgQueueRPC::enqueue_response(MsgBuffer& req_buf, MsgBuffer& resp_buf) {
    resp_buf.m_msg->msg_type = MsgHeader::RESP;
    resp_buf.m_msg->cb = req_buf.m_msg->cb;
    resp_buf.m_msg->arg = req_buf.m_msg->arg;
    resp_buf.m_q->enqueue_msg(0, resp_buf.m_msg, resp_buf.m_size);
}

void MsgQueueRPC::run_event_loop_once() {
    auto it = m_recv_queue->dequeue_msg();
    while (it != m_recv_queue->end()) {
        MsgHeader* h = *it;
        MsgBuffer buf;
        buf.m_q = m_recv_queue;
        buf.m_msg = h;
        buf.m_size = h->size;
        if (h->msg_type == MsgHeader::REQ) {
            MsgQueueNexus::__handlers[h->rpc_type](buf, m_ctx);
        } else {
            h->cb(buf, h->arg);
        }
        ++it;
    }
}

void MsgQueueRPC::free_msg_buffer(MsgBuffer& msg_buf) {
    msg_buf.m_q->free_msg_buffer(msg_buf.m_msg, msg_buf.m_size, true);
}

MsgQueue* MsgQueueManager::allocQueue() {
    uintptr_t ring_off = msgq_allocator->allocate(1);
    DLOG_ASSERT(ring_off != -1, "Can't alloc msg queue");
    MsgQueue* r = reinterpret_cast<MsgQueue*>(reinterpret_cast<uintptr_t>(start_addr) + ring_off);
    new (r) MsgQueue();
    ring_cnt++;
    return r;
}

void MsgQueueManager::freeQueue(MsgQueue* msgq) { DLOG_FATAL("Not Support"); }

MsgQueue::MsgQueue() {
    buf_head.head = 0;
    buf_head.tail = 0;
    buf_tail.head = 0;
    buf_tail.tail = 0;
}

MsgDeqIter msgq::MsgQueue::end() {
    MsgDeqIter it;
    it.q = this;
    it.rest_len = 0;
    return it;
}

MsgHeader* MsgQueue::alloc_msg_buffer(size_t size) {
    uint32_t head, head_next;
    uint32_t entries;
    uint32_t temp_len;
    int32_t diff = 0;

    bool success;
    while (true) {
        do {
            temp_len = sizeof(MsgHeader) + size;
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
            head_next = (head + temp_len) % MsgQueueManager::RING_BUF_LEN;

            success = buf_head.head.compare_exchange_weak(head, head_next);
            // printf("success = %d\n", success);
        } while (UNLIKELY(success == 0));

        MsgHeader* h = reinterpret_cast<MsgHeader*>(ring_buf + head);
        if (diff > 0) {
            h->invalid_flag = INVALID_PADDING_FLAG;
        } else {
            h->size = size;
            return h;
        }
    }
    // printf("temp_len = %d, head = %d, head_next = %d, entries = %d\n", temp_len, head, head_next,
    // entries); printf("msg_ring_get finished\n");
    return nullptr;
}

void MsgQueue::enqueue_msg(uint8_t rpc_type, MsgHeader* h, size_t size) {
    h->rpc_type = rpc_type;
    h->size = size;

    size += sizeof(MsgHeader);

    uint32_t head, head_next;
    uintptr_t obj_off = (uintptr_t)h - (uintptr_t)ring_buf;
    uint32_t temp_len;
    bool flag = true;

    bool success = false;
    while (flag) {
        do {
            flag = true;
            temp_len = size;
            head = buf_head.tail;
            head_next = (obj_off + size) % MsgQueueManager::RING_BUF_LEN;
            if (head != obj_off) {
                MsgHeader* _h = reinterpret_cast<MsgHeader*>(ring_buf + head);
                // printf("invalid = %d\n", *invalid);
                if (_h->invalid_flag == INVALID_PADDING_FLAG) {
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

MsgDeqIter MsgQueue::dequeue_msg() {
    MsgDeqIter it;
    it.q = this;
    uint32_t tail, tail_next;
    uint32_t temp_len;
    int32_t entries;
    bool flag = true;

    bool success;
    while (flag) {
        do {
            flag = true;
            tail = buf_tail.head;

            entries = buf_head.tail - tail;
            if (entries < 0) {
                entries = MsgQueueManager::RING_BUF_LEN - tail;
            } else if (entries == 0) {
                it.rest_len = 0;
                return it;
            } else {
                temp_len = entries;
            }

            tail_next = (tail + temp_len) % MsgQueueManager::RING_BUF_LEN;

            MsgHeader* _h = reinterpret_cast<MsgHeader*>(ring_buf + tail);
            if (_h->invalid_flag == INVALID_PADDING_FLAG) {
                temp_len = MsgQueueManager::RING_BUF_LEN - tail;
                tail_next = 0;
            } else {
                flag = false;
            }
            success = buf_tail.head.compare_exchange_weak(tail, tail_next);
        } while (UNLIKELY(success == 0));
    }

    it.rest_len = temp_len;
    it.h = reinterpret_cast<MsgHeader*>(ring_buf + tail);
    return it;

    // printf("tail_next = %d, tail = %d temp_len = %u, buf_head.tail = %d, RING_BUF_END_OFF =
    // %d\n", tail_next, tail, temp_len, r->buf_head.tail, RING_BUF_END_OFF);
}

void MsgQueue::free_msg_buffer(MsgHeader* h, size_t size, bool tail_valid) {
    size += sizeof(MsgHeader);

    uint32_t tail, tail_next;
    uintptr_t obj_off = (uintptr_t)h - (uintptr_t)ring_buf;
    bool flag = true;

    bool success = false;
    while (flag) {
        do {
            flag = true;
            tail = buf_tail.tail;
            tail_next = (obj_off + size) % MsgQueueManager::RING_BUF_LEN;
            // printf("tail = %d, tail_next = %d, obj_off = %d\n", tail, tail_next, obj_off);
            if (tail != obj_off) {
                MsgHeader* _h = reinterpret_cast<MsgHeader*>(ring_buf + tail);
                if (_h->invalid_flag == INVALID_PADDING_FLAG) {
                    tail_next = 0;
                } else {
                    // 若不是填充，则等到相同为止
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

bool MsgDeqIter::operator==(const MsgDeqIter& it) { return q == it.q && rest_len == it.rest_len; }
bool MsgDeqIter::operator!=(const MsgDeqIter& it) { return q != it.q || rest_len != it.rest_len; }

MsgHeader* MsgDeqIter::operator*() {
    if (h->invalid_flag == INVALID_PADDING_FLAG) {
        q->free_msg_buffer(h, 0, false);
        rest_len -= MsgQueueManager::RING_BUF_LEN - reinterpret_cast<uintptr_t>(h);
        h = reinterpret_cast<MsgHeader*>(q->ring_buf);
    }
    return h;
}

MsgDeqIter& MsgDeqIter::operator++() {
    if (h->invalid_flag == INVALID_PADDING_FLAG) {
        q->free_msg_buffer(h, 0, false);
        rest_len -= MsgQueueManager::RING_BUF_LEN - reinterpret_cast<uintptr_t>(h);
        h = reinterpret_cast<MsgHeader*>(q->ring_buf);
    } else {
        rest_len -= h->size + sizeof(MsgHeader);
        h = reinterpret_cast<MsgHeader*>(reinterpret_cast<uintptr_t>(h) + h->size +
                                         sizeof(MsgHeader));
    }
    return *this;
}

}  // namespace msgq