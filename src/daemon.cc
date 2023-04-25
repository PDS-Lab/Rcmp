#include <chrono>
#include <cstdint>
#include <future>

#include "cmdline.h"
#include "common.hpp"
#include "config.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"
#include "rdma_rc.hpp"
#include "utils.hpp"

using namespace std::chrono_literals;

DaemonContext &DaemonContext::getInstance() {
    static DaemonContext daemon_ctx;
    return daemon_ctx;
}

void DaemonContext::initCXLPool() {
    // 1. 打开cxl设备映射
    m_cxl_memory_addr =
        cxl_open_simulate(m_options.cxl_devdax_path, m_options.cxl_memory_size, &m_cxl_devdax_fd);

    cxl_memory_init(m_cxl_format, m_cxl_memory_addr, m_options.cxl_memory_size,
                    m_options.cxl_msgq_size);

    // 2. 确认page个数
    m_total_page_num = m_cxl_format.super_block->page_data_zone_size / page_size;
    m_max_swap_page_num = m_options.swap_zone_size / page_size;
    m_max_data_page_num = m_total_page_num - m_max_swap_page_num;
    m_cxl_page_allocator.reset(
        new SingleAllocator(m_cxl_format.super_block->page_data_zone_size, page_size));

    m_current_used_page_num = 0;
    m_current_used_swap_page_num = 0;
}

void DaemonContext::initCoroutinePool() {
    m_cort_sched.reset(new CortScheduler(m_options.prealloc_cort_num));
}

void DaemonContext::initRPCNexus() {
    // 1. init erpc
    std::string server_uri = m_options.daemon_ip + ":" + std::to_string(m_options.daemon_port);
    m_erpc_ctx.nexus.reset(new erpc::NexusWrap(server_uri));

    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::crossRackConnect)::rpc_type,
                                        bind_erpc_func<true>(rpc_daemon::crossRackConnect));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::getPageRDMARef)::rpc_type,
                                        bind_erpc_func<true>(rpc_daemon::getPageRDMARef));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::delPageRDMARef)::rpc_type,
                                        bind_erpc_func<true>(rpc_daemon::delPageRDMARef));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::tryMigratePage)::rpc_type,
                                        bind_erpc_func<true>(rpc_daemon::tryMigratePage));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    erpc::IBRpcWrap rpc(m_erpc_ctx.nexus.get(), this, 0, smhw);
    m_erpc_ctx.rpc_set.push_back(std::move(rpc));
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);

    // 2. init msgq
    m_msgq_manager.nexus.reset(new msgq::MsgQueueNexus(m_cxl_format.msgq_zone_start_addr));
    m_msgq_manager.start_addr = m_cxl_format.msgq_zone_start_addr;
    m_msgq_manager.msgq_allocator.reset(
        new SingleAllocator(m_options.cxl_msgq_size, MsgQueueManager::RING_ELEM_SIZE));

    msgq::MsgQueue *public_q = m_msgq_manager.allocQueue();
    DLOG_ASSERT(public_q == m_msgq_manager.nexus->m_public_msgq);

    m_msgq_manager.rpc.reset(new msgq::MsgQueueRPC(m_msgq_manager.nexus.get(), this));
    m_msgq_manager.rpc->m_recv_queue = m_msgq_manager.nexus->m_public_msgq;

    // 3. bind rpc function
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::joinRack)::rpc_type,
                                            bind_msgq_rpc_func<true>(rpc_daemon::joinRack));
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::alloc)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::alloc));
    m_msgq_manager.nexus->register_req_func(
        RPC_TYPE_STRUCT(rpc_daemon::getPageCXLRefOrProxy)::rpc_type,
        bind_msgq_rpc_func<false>(rpc_daemon::getPageCXLRefOrProxy));
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::allocPage)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::allocPage));

    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::__testdataSend1)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::__testdataSend1));
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::__testdataSend2)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::__testdataSend2));
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::__notifyPerf)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::__notifyPerf));
    m_msgq_manager.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::__stopPerf)::rpc_type,
                                            bind_msgq_rpc_func<false>(rpc_daemon::__stopPerf));
}

void DaemonContext::initRDMARC() {
    rdma_rc::RDMAEnv::init();

    rdma_rc::RDMAConnection::register_connect_hook([this](rdma_rc::RDMAConnection *rdma_conn,
                                                          void *param_) {
        auto param = reinterpret_cast<RDMARCConnectParam *>(param_);
        auto conn_ = get_connection(param->mac_id);
        switch (param->role) {
            case MN:
            case CN:
            case CXL_CN:
                DLOG_FATAL("Not Support");
                break;
            case DAEMON:
            case CXL_DAEMON: {
                DaemonToDaemonConnection *conn = dynamic_cast<DaemonToDaemonConnection *>(conn_);
                conn->rdma_conn = rdma_conn;
            } break;
        }

        DLOG("[RDMA_RC] Get New Connect: %s", rdma_conn->get_peer_addr().first.c_str());
    });
    rdma_rc::RDMAConnection::register_disconnect_hook([](rdma_rc::RDMAConnection *conn) {
        DLOG("[RDMA_RC] Disconnect: %s", conn->get_peer_addr().first.c_str());
    });
    m_listen_conn.listen(m_options.daemon_rdma_ip);
}

void DaemonContext::connectWithMaster() {
    auto &rpc = get_erpc();

    std::string master_uri = m_options.master_ip + ":" + std::to_string(m_options.master_port);
    m_master_connection.peer_session = rpc.create_session(master_uri, 0);

    using JoinDaemonRPC = RPC_TYPE_STRUCT(rpc_master::joinDaemon);

    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinDaemonRPC::RequestType));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(JoinDaemonRPC::ResponseType));

    auto req = reinterpret_cast<JoinDaemonRPC::RequestType *>(req_raw.get_buf());
    req->ip = m_options.daemon_ip;
    req->port = m_options.daemon_port;
    req->free_page_num = m_max_data_page_num;
    req->rack_id = m_options.rack_id;
    req->with_cxl = m_options.with_cxl;

    std::promise<void> pro;
    std::future<void> fu = pro.get_future();
    rpc.enqueue_request(m_master_connection.peer_session, JoinDaemonRPC::rpc_type, req_raw,
                        resp_raw, erpc_general_promise_flag_cb, static_cast<void *>(&pro));

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto resp = reinterpret_cast<JoinDaemonRPC::ResponseType *>(resp_raw.get_buf());

    m_master_connection.ip = m_options.master_ip;
    m_master_connection.port = m_options.master_port;
    m_master_connection.master_id = resp->master_mac_id;
    m_daemon_id = resp->daemon_mac_id;

    std::string peer_ip(resp->rdma_ipv4.get_string());
    uint16_t peer_port(resp->rdma_port);

    rpc.free_msg_buffer(req_raw);
    rpc.free_msg_buffer(resp_raw);

    DLOG_ASSERT(m_master_connection.master_id == master_id, "Fail to get master id");
    DLOG_ASSERT(m_daemon_id != master_id, "Fail to get daemon id");

    RDMARCConnectParam param;
    param.mac_id = m_daemon_id;
    param.role = CXL_DAEMON;

    m_master_connection.rdma_conn = new rdma_rc::RDMAConnection();

    m_master_connection.rdma_conn->connect(peer_ip, peer_port, &param, sizeof(param));

    DLOG("Connection with master OK, my id is %d", m_daemon_id);
}

void DaemonContext::registerCXLMR() {
    uintptr_t cxl_start_ptr = reinterpret_cast<uintptr_t>(m_cxl_format.start_addr);
    while (cxl_start_ptr < reinterpret_cast<uintptr_t>(m_cxl_format.end_addr)) {
        ibv_mr *mr = m_listen_conn.register_memory(reinterpret_cast<void *>(cxl_start_ptr),
                                                   mem_region_aligned_size);
        m_rdma_page_mr_table.push_back(mr);
        cxl_start_ptr += mem_region_aligned_size;
    }

    // 继续注册尾部不足mem region的内存
    if (cxl_start_ptr != reinterpret_cast<uintptr_t>(m_cxl_format.end_addr)) {
        cxl_start_ptr -= mem_region_aligned_size;
        ibv_mr *mr = m_listen_conn.register_memory(
            reinterpret_cast<void *>(cxl_start_ptr),
            reinterpret_cast<uintptr_t>(m_cxl_format.end_addr) - cxl_start_ptr);
        m_rdma_page_mr_table.push_back(mr);
    }
}

DaemonConnection *DaemonContext::get_connection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != m_daemon_id, "Can't find self connection");
    if (mac_id == master_id) {
        return &m_master_connection;
    }

    auto it = m_connect_table.find(mac_id);
    DLOG_ASSERT(it != m_connect_table.end(), "Can't find mac %d", mac_id);
    return it->second;
}

CortScheduler &DaemonContext::get_cort_sched() { return *m_cort_sched; }

erpc::IBRpcWrap &DaemonContext::get_erpc() { return m_erpc_ctx.rpc_set[0]; }

ibv_mr *DaemonContext::get_mr(void *p) {
    if (p >= m_cxl_format.start_addr && p < m_cxl_format.end_addr) {
        return m_rdma_page_mr_table[(reinterpret_cast<uintptr_t>(p) -
                                     reinterpret_cast<const uintptr_t>(m_cxl_format.start_addr)) /
                                    mem_region_aligned_size];
    } else {
        return m_rdma_mr_table[reinterpret_cast<void *>(
            align_floor(reinterpret_cast<uintptr_t>(p), mem_region_aligned_size))];
    }
}

RemotePageMetaCache::RemotePageMetaCache(size_t max_recent_record) : stats(max_recent_record) {}

msgq::MsgQueue *MsgQueueManager::allocQueue() {
    uintptr_t ring_off = msgq_allocator->allocate(1);
    DLOG_ASSERT(ring_off != -1, "Can't alloc msg queue");
    msgq::MsgQueue *r =
        reinterpret_cast<msgq::MsgQueue *>(reinterpret_cast<uintptr_t>(start_addr) + ring_off);
    new (r) msgq::MsgQueue();
    ring_cnt++;
    return r;
}

void MsgQueueManager::freeQueue(msgq::MsgQueue *msgq) { DLOG_FATAL("Not Support"); }

int main(int argc, char *argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("master_ip");
    cmd.add<uint16_t>("master_port");
    cmd.add<std::string>("daemon_ip");
    cmd.add<std::string>("daemon_rdma_ip");
    cmd.add<uint16_t>("daemon_port");
    cmd.add<rack_id_t>("rack_id");
    cmd.add<std::string>("cxl_devdax_path");
    cmd.add<size_t>("cxl_memory_size");
    bool ret = cmd.parse(argc, argv);
    DLOG_ASSERT(ret);

    rchms::DaemonOptions options;
    options.master_ip = cmd.get<std::string>("master_ip");
    options.master_port = cmd.get<uint16_t>("master_port");
    options.daemon_ip = cmd.get<std::string>("daemon_ip");
    options.daemon_rdma_ip = cmd.get<std::string>("daemon_rdma_ip");
    options.daemon_port = cmd.get<uint16_t>("daemon_port");
    options.rack_id = cmd.get<rack_id_t>("rack_id");
    options.with_cxl = true;
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.swap_zone_size = 2 << 20;
    options.max_client_limit = 2;  // 暂时未使用
    options.cxl_msgq_size = 5 << 10;
    options.prealloc_cort_num = 8;

    DaemonContext &daemon_context = DaemonContext::getInstance();
    daemon_context.m_options = options;

    daemon_context.initCXLPool();
    daemon_context.initCoroutinePool();
    daemon_context.initRDMARC();
    daemon_context.initRPCNexus();
    daemon_context.connectWithMaster();
    daemon_context.registerCXLMR();

    while (true) {
        daemon_context.m_msgq_manager.rpc->run_event_loop_once();
        daemon_context.get_erpc().run_event_loop_once();
        daemon_context.get_cort_sched().runOnce();
    }

    return 0;
}