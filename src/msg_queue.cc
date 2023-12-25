#include "msg_queue.hpp"

#include <atomic>
#include <cstdint>

#include "config.hpp"
#include "log.hpp"
#include "utils.hpp"

namespace msgq {

msgq_handler_t MsgQueueNexus::__handlers[max_msgq_handler];

MsgQueueNexus::MsgQueueNexus(void* msgq_zone_start_addr)
    : m_msgq_zone_start_addr(msgq_zone_start_addr),
      m_public_msgq(reinterpret_cast<MsgQueue*>(msgq_zone_start_addr)) {}

void MsgQueueNexus::register_req_func(uint8_t rpc_type, msgq_handler_t handler) {
    __handlers[rpc_type] = handler;
}

MsgQueueRPC::MsgQueueRPC(MsgQueueNexus* nexus, MsgQueue* send_queue, MsgQueue* recv_queue,
                         void* ctx)
    : m_nexus(nexus), m_ctx(ctx), m_recv_queue(recv_queue), m_send_queue(send_queue) {}

#if MSGQ_SINGLE_FIFO_ON == 1

size_t MsgBuffer::size() const { return m_size; }
void* MsgBuffer::get_buf() const { return m_msg->data; }

MsgBuffer MsgQueueRPC::alloc_msg_buffer(size_t size) {
    MsgBuffer buf;
    buf.m_size = size;
    buf.m_q = m_send_queue;
    buf.m_msg = m_send_queue->alloc_msg_buffer(size);
    return buf;
}

void MsgQueueRPC::free_msg_buffer(MsgBuffer& msg_buf) { msg_buf.m_q->free_msg_buffer(); }

void MsgQueueRPC::enqueue_request(uint8_t rpc_type, MsgBuffer& msg_buf, msgq_callback_t cb,
                                  void* arg) {
    msg_buf.m_msg->msg_type = MsgHeader::REQ;
    msg_buf.m_msg->cb = cb;
    msg_buf.m_msg->arg = arg;
    msg_buf.m_msg->rpc_type = rpc_type;
    m_send_queue->enqueue_msg();
}

void MsgQueueRPC::enqueue_response(MsgBuffer& req_buf, MsgBuffer& resp_buf) {
    resp_buf.m_msg->msg_type = MsgHeader::RESP;
    resp_buf.m_msg->cb = req_buf.m_msg->cb;
    resp_buf.m_msg->arg = req_buf.m_msg->arg;
    resp_buf.m_q->enqueue_msg();
}

void MsgQueueRPC::run_event_loop_once() {
    std::vector<MsgHeader*> hv;
    m_recv_queue->dequeue_msg(hv);
    for (auto& h : hv) {
        MsgBuffer buf;
        buf.m_q = m_recv_queue;
        buf.m_msg = h;
        buf.m_size = h->size;
        if (h->msg_type == MsgHeader::REQ) {
            MsgQueueNexus::__handlers[h->rpc_type](buf, m_ctx);
        } else {
            h->cb(buf, h->arg);
        }
    }
}

MsgQueue::MsgQueue() {
    m_prod_head.raw = 0;
    m_prod_tail.raw = 0;
    m_cons_head.raw = 0;
    m_cons_tail.raw = 0;
}

MsgHeader* MsgQueue::alloc_msg_buffer(size_t size) {
    MsgHeader *h, *inv_h;
    size_t inv_s;
    atomic_po_val_t oh, nh;

retry:
    oh = m_prod_head.load(std::memory_order_acquire);
    do {
        inv_h = nullptr;
        nh.cnt = oh.cnt + 1;
        nh.pos = oh.pos + size + sizeof(MsgHeader);

        /**
         * @brief out of ring bound
         *
         *       oh   ring_bound   nh
         *        |       |         |
         * [ring          ]...........
         *         \--------v------/
         *              msg_size
         *
         * after enqueue:
         *
         *        oh  ring_bound          nh%LEN
         *        |      |                  |
         *        Fxxxxxx][
         *           inv  \--------v-------/
         *                      msg_size
         */

        if (UNLIKELY(nh.pos % SZ != 0 && div_floor(oh.pos, SZ) != div_floor(nh.pos, SZ))) {
            // invalid tail
            inv_s = align_ceil(oh.pos, SZ) - oh.pos;
            nh.pos += inv_s;
            inv_h = at(oh.pos);
            h = at(SZ);
        } else {
            h = at(oh.pos);
        }
        if (UNLIKELY(nh.pos - m_cons_tail.pos > SZ)) {
            goto retry;
        }
    } while (!m_prod_head.compare_exchange_weak(oh, nh, std::memory_order_acquire,
                                                std::memory_order_acquire));

    if (UNLIKELY(inv_h != nullptr)) {
        inv_h->invalid_flag = true;
    }
    h->invalid_flag = false;
    h->size = size;
    return h;
}

void MsgQueue::enqueue_msg() { update_ht(&m_prod_head, &m_prod_tail); }

void MsgQueue::dequeue_msg(std::vector<MsgHeader*>& hv) {
    size_t s;
    atomic_po_val_t ot, nt;
    hv.clear();
    ot = m_cons_head.load(std::memory_order_acquire);
    do {
        s = m_prod_tail.pos - ot.pos;
        if (s == 0) {
            return;
        }
        nt.cnt = ot.cnt + 1;
        nt.pos = ot.pos + s;
    } while (UNLIKELY(!m_cons_head.compare_exchange_weak(ot, nt, std::memory_order_acquire,
                                                         std::memory_order_acquire)));

    size_t tmp_s = 0;
    do {
        MsgHeader* h = at(ot.pos + tmp_s);
        if (UNLIKELY(h->invalid_flag)) {
            tmp_s += SZ - (reinterpret_cast<uint8_t*>(h) - m_ring);
        } else {
            hv.push_back(h);
            tmp_s += h->size + sizeof(MsgHeader);
        }
    } while (tmp_s != s);

    m_cons_head.fetch_add_cnt(hv.size() - 1, std::memory_order_acquire);
}

void MsgQueue::free_msg_buffer() { update_ht(&m_cons_head, &m_cons_tail); }

MsgHeader* MsgQueue::at(size_t i) { return reinterpret_cast<MsgHeader*>(m_ring + (i % SZ)); }

void MsgQueue::update_ht(atomic_po_val_t* ht, atomic_po_val_t* ht_) {
    atomic_po_val_t h, oh, nh;
    oh = ht_->load(std::memory_order_acquire);
    do {
        h = ht->load(std::memory_order_relaxed);
        nh.raw = oh.raw;
        if ((++nh.cnt) == h.cnt) {
            nh.pos = h.pos;
        }
    } while (UNLIKELY(
        !ht_->compare_exchange_weak(oh, nh, std::memory_order_release, std::memory_order_acquire)));
}

#else

size_t MsgBuffer::size() const { return m_msg.size; }

void* MsgBuffer::get_buf() const {
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(m_q->m_ra.base()) +
                                   m_msg.buf_offset);
}

MsgBuffer MsgQueueRPC::alloc_msg_buffer(size_t size) {
    MsgBuffer buf;
    buf.m_q = m_send_queue;
    buf.m_msg.size = size;
    buf.m_msg.buf_offset = m_send_queue->alloc_msg_buffer(size);
    return buf;
}

void MsgQueueRPC::free_msg_buffer(MsgBuffer& msg_buf) { msg_buf.m_q->free_msg_buffer(msg_buf); }

void MsgQueueRPC::enqueue_request(uint8_t rpc_type, MsgBuffer& msg_buf, msgq_callback_t cb,
                                  void* arg) {
    msg_buf.m_msg.msg_type = MsgHeader::REQ;
    msg_buf.m_msg.cb = cb;
    msg_buf.m_msg.arg = arg;
    msg_buf.m_msg.rpc_type = rpc_type;
    m_nexus->m_stats.start_sample(msg_buf.m_msg.send_ts);
    m_send_queue->enqueue_msg(msg_buf);
}

void MsgQueueRPC::enqueue_response(MsgBuffer& req_buf, MsgBuffer& resp_buf) {
    resp_buf.m_msg.msg_type = MsgHeader::RESP;
    resp_buf.m_msg.cb = req_buf.m_msg.cb;
    resp_buf.m_msg.arg = req_buf.m_msg.arg;
    m_nexus->m_stats.start_sample(resp_buf.m_msg.send_ts);
    resp_buf.m_q->enqueue_msg(resp_buf);
}

void MsgQueueRPC::run_event_loop_once() {
    MsgHeader hv[64];
    uint32_t s = m_recv_queue->dequeue_msg(hv, 64);
    for (uint32_t i = 0; i < s; ++i) {
        MsgHeader& h = hv[i];
        MsgBuffer buf;
        buf.m_q = m_recv_queue;
        buf.m_msg = h;

        if (h.msg_type == MsgHeader::REQ) {
            m_nexus->m_stats.send_sample(h.size, h.send_ts);
            MsgQueueNexus::__handlers[h.rpc_type](buf, m_ctx);
        } else {
            m_nexus->m_stats.recv_sample(h.size, h.send_ts);
            h.cb(buf, h.arg);
        }
    }
}

offset_t MsgQueue::alloc_msg_buffer(size_t size) {
    void* ptr;
    bool noticed = false;
    do {
        ptr = m_ra.allocate(size);
        if (!ptr) {
            if (!noticed) {
                DLOG_WARNING("msg queue full");
                noticed = true;
            }
            boost::this_fiber::yield();
            std::this_thread::yield();
        }
    } while (ptr == nullptr);
    return reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(m_ra.base());
}

void MsgQueue::enqueue_msg(MsgBuffer& msg_buf) { msgq_q.ForceEnqueue(msg_buf.m_msg); }

uint32_t MsgQueue::dequeue_msg(MsgHeader* hv, size_t max_deq) {
    return msgq_q.TryDequeue(hv, hv + max_deq);
}

void MsgQueue::free_msg_buffer(MsgBuffer& msg_buf) {
    m_ra.deallocate(msg_buf.get_buf(), msg_buf.size());
}

#endif  // MSGQ_SINGLE_FIFO_ON

}  // namespace msgq

msgq::MsgQueue* MsgQueueManager::allocQueue() {
    uintptr_t ring_off = msgq_allocator->allocate(1);
    DLOG_ASSERT(ring_off != -1, "Can't alloc msg queue");
    msgq::MsgQueue* r =
        reinterpret_cast<msgq::MsgQueue*>(reinterpret_cast<uintptr_t>(start_addr) + ring_off);
    new (r) msgq::MsgQueue();
    ring_cnt++;
    return r;
}

void MsgQueueManager::freeQueue(msgq::MsgQueue* msgq) { DLOG_FATAL("Not Support"); }