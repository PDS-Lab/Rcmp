#include "msg_queue.hpp"

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
    __handlers[rpc_type] = handler;
}

MsgQueueRPC::MsgQueueRPC(MsgQueueNexus* nexus, void* ctx) : m_nexus(nexus), m_ctx(ctx) {}

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
    static thread_local std::vector<MsgHeader*> hv;
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
    m_prod_head.raw = 0;
    m_prod_tail.raw = 0;
    m_cons_head.raw = 0;
    m_cons_tail.raw = 0;
}

MsgHeader* MsgQueue::alloc_msg_buffer(size_t size) {
    MsgHeader *h, *inv_h;
    size_t inv_s;
    union po_val_t oh, nh;

retry:
    oh.raw = __atomic_load_n(&m_prod_head.raw, __ATOMIC_ACQUIRE);
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

        if (UNLIKELY(div_floor(oh.pos, MsgQueueManager::RING_BUF_LEN) !=
                     div_floor(nh.pos, MsgQueueManager::RING_BUF_LEN))) {
            // invalid tail
            inv_s = align_ceil(oh.pos, MsgQueueManager::RING_BUF_LEN) - oh.pos;
            nh.pos += inv_s;
            inv_h = at(oh.pos);
            h = at(MsgQueueManager::RING_BUF_LEN);
        } else {
            h = at(oh.pos);
        }
        if (UNLIKELY(nh.pos - m_cons_tail.pos > MsgQueueManager::RING_BUF_LEN)) {
            goto retry;
        }
    } while (!__atomic_compare_exchange_n(&m_prod_head.raw, &oh.raw, nh.raw, true, __ATOMIC_ACQUIRE,
                                          __ATOMIC_ACQUIRE));

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
    union po_val_t ot, nt;
    hv.clear();
    ot.raw = __atomic_load_n(&m_cons_head.raw, __ATOMIC_ACQUIRE);
    do {
        s = m_prod_tail.pos - ot.pos;
        if (s == 0) {
            return;
        }
        nt.cnt = ot.cnt + 1;
        nt.pos = ot.pos + s;
    } while (UNLIKELY(!__atomic_compare_exchange_n(&m_cons_head.raw, &ot.raw, nt.raw, true,
                                                   __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)));

    size_t tmp_s = 0;
    do {
        MsgHeader* h = at(ot.pos + tmp_s);
        if (UNLIKELY(h->invalid_flag)) {
            tmp_s += MsgQueueManager::RING_BUF_LEN - ((uint8_t*)h - m_ring);
        } else {
            hv.push_back(h);
            tmp_s += h->size + sizeof(MsgHeader);
        }
    } while (tmp_s != s);

    __atomic_fetch_add(&m_cons_head.cnt, hv.size() - 1, __ATOMIC_ACQUIRE);
}

void MsgQueue::free_msg_buffer() { update_ht(&m_cons_head, &m_cons_tail); }

MsgHeader* MsgQueue::at(size_t i) {
    return reinterpret_cast<MsgHeader*>(m_ring + (i % MsgQueueManager::RING_BUF_LEN));
}

void MsgQueue::update_ht(volatile po_val_t* ht, volatile po_val_t* ht_) {
    union po_val_t h, oh, nh;
    oh.raw = __atomic_load_n(&ht_->raw, __ATOMIC_ACQUIRE);
    do {
        h.raw = __atomic_load_n(&ht->raw, __ATOMIC_RELAXED);
        nh.raw = oh.raw;
        if ((++nh.cnt) == h.cnt) {
            nh.pos = h.pos;
        }
    } while (UNLIKELY(!__atomic_compare_exchange_n(&ht_->raw, &oh.raw, nh.raw, true,
                                                   __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)));
}

}  // namespace msgq