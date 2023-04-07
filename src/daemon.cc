#include <chrono>
#include <future>

#include "common.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"

using namespace std::chrono_literals;

DaemonContext &DaemonContext::getInstance() {
    static DaemonContext daemon_ctx;
    return daemon_ctx;
}

void DaemonContext::initCXLPool() {
    // 1. 打开cxl设备映射
    m_cxl_memory_addr =
        cxl_open_simulate(m_options.cxl_devdax_path, m_options.cxl_memory_size, &m_cxl_devdax_fd);

    cxl_memory_init(cxl_format, m_cxl_memory_addr, m_options.cxl_memory_size,
                    m_options.cxl_msgq_size);

    // 2. 确认page个数
    m_total_page_num = cxl_format.super_block->page_data_zone_size / page_size;
    m_max_swap_page_num = m_options.swap_zone_size / page_size;
    m_max_data_page_num = m_total_page_num - m_max_swap_page_num;
    m_cxl_page_allocator.reset(
        new SingleAllocator(cxl_format.super_block->page_data_zone_size, page_size));

    m_current_used_page_num = 0;
    m_current_used_swap_page_num = 0;
}

void DaemonContext::initRPCNexus() {
    // 1. init erpc
    std::string server_uri = m_options.daemon_ip + ":" + std::to_string(m_options.daemon_port);
    m_erpc_ctx.nexus.reset(new erpc::NexusWrap(server_uri));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    auto rpc = erpc::IBRpcWrap(m_erpc_ctx.nexus.get(), nullptr, 0, smhw);
    m_erpc_ctx.rpc_set.push_back(rpc);
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);

    // 2. init msgq
    msgq_manager.nexus.reset(new msgq::MsgQueueNexus(cxl_format.msgq_zone_start_addr));
    msgq_manager.start_addr = cxl_format.msgq_zone_start_addr;
    msgq_manager.msgq_allocator.reset(
        new SingleAllocator(m_options.cxl_msgq_size, msgq::MsgQueueManager::RING_ELEM_SIZE));

    msgq::MsgQueue *public_q = msgq_manager.allocQueue();
    DLOG_ASSERT(public_q == msgq_manager.nexus->m_public_msgq);

    // 3. bind rpc function
    msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::joinRack)::rpc_type,
                                          bind_msgq_rpc_func<true>(rpc_daemon::joinRack));
    msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::alloc)::rpc_type,
                                          bind_msgq_rpc_func<false>(rpc_daemon::alloc));
    msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::getPageRef)::rpc_type,
                                          bind_msgq_rpc_func<false>(rpc_daemon::getPageRef));
    msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::dataSend)::rpc_type,
                                          bind_msgq_rpc_func<false>(rpc_daemon::dataSend));
    msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::dataSend1)::rpc_type,
                                          bind_msgq_rpc_func<false>(rpc_daemon::dataSend1));

    msgq_manager.rpc.reset(new msgq::MsgQueueRPC(msgq_manager.nexus.get(), this));
    msgq_manager.rpc->m_recv_queue = msgq_manager.nexus->m_public_msgq;
}

void DaemonContext::connectWithMaster() {
    auto rpc = get_erpc();

    std::string master_uri = m_options.master_ip + ":" + std::to_string(m_options.master_port);
    m_erpc_ctx.master_session = rpc.create_session(master_uri, 0);

    using JoinDaemonRPC = RPC_TYPE_STRUCT(rpc_master::joinDaemon);

    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinDaemonRPC::RequestType));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinDaemonRPC::ResponseType));

    auto req = reinterpret_cast<JoinDaemonRPC::RequestType *>(req_raw.get_buf());
    req->free_page_num = m_max_data_page_num;
    req->rack_id = m_options.rack_id;
    req->with_cxl = m_options.with_cxl;

    std::promise<void> pro;
    std::future<void> fu = pro.get_future();
    rpc.enqueue_request(m_erpc_ctx.master_session, JoinDaemonRPC::rpc_type, req_raw, resp_raw,
                        erpc_general_promise_flag_cb, static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto resp = reinterpret_cast<JoinDaemonRPC::ResponseType *>(resp_raw.get_buf());

    m_master_connection.ip = m_options.master_ip;
    m_master_connection.port = m_options.master_port;
    m_master_connection.master_id = resp->master_mac_id;
    m_daemon_id = resp->daemon_mac_id;

    rpc.free_msg_buffer(req_raw);
    rpc.free_msg_buffer(resp_raw);

    DLOG_ASSERT(m_master_connection.master_id == master_id, "Fail to get master id");
    DLOG_ASSERT(m_daemon_id != master_id, "Fail to get daemon id");

    DLOG("Connection with master OK, my id is %d", m_daemon_id);
}

DaemonConnection *DaemonContext::get_connection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != m_daemon_id, "Can't find self connection");
    if (mac_id == master_id) {
        return &m_master_connection;
    } else {
        DaemonConnection *ctx;
        bool ret = m_connect_table.find(mac_id, &ctx);
        DLOG_ASSERT(ret, "Can't find mac %d", mac_id);
        return ctx;
    }
}

erpc::IBRpcWrap DaemonContext::get_erpc() { return m_erpc_ctx.rpc_set[0]; }

PageMetadata::PageMetadata(size_t slab_size) : slab_allocator(page_size, slab_size) {}

int main(int argc, char *argv[]) {
    rchms::DaemonOptions options;
    options.master_ip = "192.168.1.88";
    options.master_port = 31850;
    options.daemon_ip = "192.168.1.88";
    options.daemon_port = 31851;
    options.rack_id = 0;
    options.with_cxl = true;
    options.cxl_devdax_path = "testfile";
    options.cxl_memory_size = 10 << 20;
    options.swap_zone_size = 2 << 20;
    options.max_client_limit = 2; // 暂时未使用
    options.cxl_msgq_size = 20 << 10;

    DaemonContext &daemon_ctx = DaemonContext::getInstance();
    daemon_ctx.m_options = options;

    daemon_ctx.initCXLPool();
    daemon_ctx.initRPCNexus();
    daemon_ctx.connectWithMaster();

    // TODO: 开始监听master的RRPC

    // TODO: 开始监听client的msg queue

    while (true) {
        daemon_ctx.msgq_manager.rpc->run_event_loop_once();
    }

    getchar();
    getchar();
    return 0;
}