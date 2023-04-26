#include "msg_queue.hpp"

#include <cstdint>

#include "config.hpp"
#include "log.hpp"
#include "utils.hpp"

namespace msgq {

msgq_handler_t MsgQueueNexus::__handlers[max_msgq_handler];

size_t MsgBuffer::size() const { return m_msg.size; }
void* MsgBuffer::get_buf() const {
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(m_q->m_ra.base()) +
                                   m_msg.buf_offset);
}

MsgQueueNexus::MsgQueueNexus(void* msgq_zone_start_addr)
    : m_msgq_zone_start_addr(msgq_zone_start_addr),
      m_public_msgq(reinterpret_cast<MsgQueue*>(msgq_zone_start_addr)) {}

void MsgQueueNexus::register_req_func(uint8_t rpc_type, msgq_handler_t handler) {
    __handlers[rpc_type] = handler;
}

MsgQueueRPC::MsgQueueRPC(MsgQueueNexus* nexus, void* ctx) : m_nexus(nexus), m_ctx(ctx) {}

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
    m_send_queue->enqueue_msg(msg_buf);
}

void MsgQueueRPC::enqueue_response(MsgBuffer& req_buf, MsgBuffer& resp_buf) {
    resp_buf.m_msg.msg_type = MsgHeader::RESP;
    resp_buf.m_msg.cb = req_buf.m_msg.cb;
    resp_buf.m_msg.arg = req_buf.m_msg.arg;
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
            MsgQueueNexus::__handlers[h.rpc_type](buf, m_ctx);
        } else {
            h.cb(buf, h.arg);
        }
    }
}

offset_t MsgQueue::alloc_msg_buffer(size_t size) {
    void* ptr;
    do {
        ptr = m_ra.alloc(size);
    } while (ptr == nullptr);
    return reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(m_ra.base());
}

void MsgQueue::enqueue_msg(MsgBuffer& msg_buf) {
    msgq_q.forceEnqueue(msg_buf.m_msg);
}

uint32_t MsgQueue::dequeue_msg(MsgHeader* hv, size_t max_deq) {
    return msgq_q.tryDequeue(hv, hv + max_deq);
}

void MsgQueue::free_msg_buffer(MsgBuffer& msg_buf) { m_ra.free(msg_buf.get_buf()); }

}  // namespace msgq