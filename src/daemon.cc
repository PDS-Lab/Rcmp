#include <boost/fiber/algo/round_robin.hpp>
#include <boost/fiber/operations.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>

#include "cmdline.h"
#include "common.hpp"
#include "config.hpp"
#include "cxl.hpp"
#include "eRPC/erpc.h"
#include "impl.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "promise.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_daemon.hpp"
#include "proto/rpc_register.hpp"
#include "rdma_rc.hpp"
#include "utils.hpp"

using namespace std::chrono_literals;

DaemonContext &DaemonContext::getInstance() {
    static DaemonContext daemon_ctx;
    return daemon_ctx;
}

void DaemonContext::InitCXLPool() {
    // 1. 打开cxl设备映射
    m_cxl_memory_addr =
        cxl_open_simulate(m_options.cxl_devdax_path, m_options.cxl_memory_size, &m_cxl_devdax_fd);

    cxl_memory_init(m_cxl_format, m_cxl_memory_addr, m_options.cxl_memory_size,
                    (m_options.max_client_limit + 1) * MsgQueueManager::RING_ELEM_SIZE);

    // 2. 确认page个数
    m_page_table.total_page_num = m_cxl_format.super_block->page_data_zone_size / page_size;
    m_page_table.max_swap_page_num = m_options.swap_zone_size / page_size;
    m_page_table.max_data_page_num = m_page_table.total_page_num - m_page_table.max_swap_page_num;
    m_page_table.page_allocator =
        std::make_unique<SingleAllocator<page_size>>(m_cxl_format.super_block->page_data_zone_size);

    m_page_table.current_used_page_num = 0;

    DLOG("total_page_num: %lu", m_page_table.total_page_num);
    DLOG("max_swap_page_num: %lu", m_page_table.max_swap_page_num);
    DLOG("max_data_page_num: %lu", m_page_table.max_data_page_num);
}

void DaemonContext::InitRPCNexus() {
    // 1. init erpc
    std::string server_uri = erpc::concat_server_uri(m_options.daemon_ip, m_options.daemon_port);
    m_erpc_ctx.nexus = std::make_unique<erpc::NexusWrap>(server_uri);

    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::crossRackConnect)::rpc_type,
                                        bind_erpc_func<true>(rpc_daemon::crossRackConnect));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::getPageRDMARef)::rpc_type,
                                        bind_erpc_func<false>(rpc_daemon::getPageRDMARef));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::allocPageMemory)::rpc_type,
                                        bind_erpc_func<false>(rpc_daemon::allocPageMemory));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::delPageRDMARef)::rpc_type,
                                        bind_erpc_func<false>(rpc_daemon::delPageRDMARef));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_daemon::tryMigratePage)::rpc_type,
                                        bind_erpc_func<false>(rpc_daemon::tryMigratePage));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    erpc::IBRpcWrap rpc(m_erpc_ctx.nexus.get(), this, 0, smhw);
    m_erpc_ctx.rpc_set.push_back(std::move(rpc));
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);

    // 2. init msgq
    m_msgq_manager.nexus = std::make_unique<msgq::MsgQueueNexus>(m_cxl_format.msgq_zone_start_addr);
    m_msgq_manager.start_addr = m_cxl_format.msgq_zone_start_addr;
    m_msgq_manager.msgq_allocator =
        std::make_unique<SingleAllocator<MsgQueueManager::RING_ELEM_SIZE>>(
            m_cxl_format.super_block->msgq_zone_size);

    msgq::MsgQueue *public_q = m_msgq_manager.allocQueue();
    DLOG_ASSERT(public_q == m_msgq_manager.nexus->GetPublicMsgQ());

    m_msgq_manager.rpc = std::make_unique<msgq::MsgQueueRPC>(
        m_msgq_manager.nexus.get(), nullptr, m_msgq_manager.nexus->GetPublicMsgQ(), this);

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

void DaemonContext::InitRDMARC() {
    rdma_rc::RDMAEnv::init();

    rdma_rc::RDMAConnection::register_connect_hook([this](rdma_rc::RDMAConnection *rdma_conn,
                                                          void *param_) {
        auto param = reinterpret_cast<RDMARCConnectParam *>(param_);
        auto conn_ = GetConnection(param->mac_id);
        switch (param->role) {
            case MN:
            case CN:
            case CXL_CN:
                DLOG_FATAL("Not Support");
                break;
            case DAEMON:
            case CXL_DAEMON: {
                DaemonToDaemonConnection *conn = dynamic_cast<DaemonToDaemonConnection *>(conn_);
                conn->rdma_conn.reset(rdma_conn);
            } break;
        }

        DLOG("[RDMA_RC] Get New Connect: %s", rdma_conn->get_peer_addr().first.c_str());
    });

    rdma_rc::RDMAConnection::register_disconnect_hook([](rdma_rc::RDMAConnection *conn) {
        DLOG("[RDMA_RC] Disconnect: %s", conn->get_peer_addr().first.c_str());
    });

    m_listen_conn.listen(m_options.daemon_rdma_ip);
}

void DaemonContext::ConnectWithMaster() {
    auto &rpc = GetErpc();

    auto &master_connection = m_conn_manager.GetMasterConnection();

    master_connection.erpc_conn =
        std::make_unique<ErpcClient>(rpc, m_options.master_ip, m_options.master_port);

    auto fu = master_connection.erpc_conn->call<SpinPromise>(
        rpc_master::joinDaemon, {
                                    .ip = m_options.daemon_ip,
                                    .port = m_options.daemon_port,
                                    .rack_id = m_options.rack_id,
                                    .with_cxl = m_options.with_cxl,
                                    .free_page_num = m_page_table.max_data_page_num,
                                });

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto &resp = fu.get();

    master_connection.ip = m_options.master_ip;
    master_connection.port = m_options.master_port;
    master_connection.master_id = resp.master_mac_id;
    m_daemon_id = resp.daemon_mac_id;

    DLOG_ASSERT(master_connection.master_id == master_id, "Fail to get master id");
    DLOG_ASSERT(m_daemon_id != master_id, "Fail to get daemon id");

    std::string peer_ip(resp.rdma_ipv4.get_string());
    uint16_t peer_port(resp.rdma_port);

    RDMARCConnectParam param;
    param.mac_id = m_daemon_id;
    param.role = CXL_DAEMON;

    master_connection.rdma_conn = std::make_unique<rdma_rc::RDMAConnection>();
    master_connection.rdma_conn->connect(peer_ip, peer_port, &param, sizeof(param));

    DLOG("Connection with master OK, my id is %d", m_daemon_id);

    for (size_t i = 0; i < resp.other_rack_count; ++i) {
        auto &rack_info = resp.other_rack_infos[i];

        // 与该daemon建立erpc与RDMA RC
        DaemonToDaemonConnection *dd_conn = new DaemonToDaemonConnection();
        dd_conn->erpc_conn = std::make_unique<ErpcClient>(rpc, rack_info.daemon_ipv4.get_string(),
                                                          rack_info.daemon_erpc_port);
        dd_conn->daemon_id = rack_info.daemon_id;
        dd_conn->rack_id = rack_info.rack_id;
        dd_conn->ip = rack_info.daemon_ipv4.get_string();
        dd_conn->port = rack_info.daemon_erpc_port;
        DLOG("First connect daemon: %u", dd_conn->daemon_id);

        auto fu = dd_conn->erpc_conn->call<SpinPromise>(rpc_daemon::crossRackConnect,
                                                        {
                                                            .mac_id = m_daemon_id,
                                                            .ip = m_options.daemon_ip,
                                                            .port = m_options.daemon_port,
                                                            .rack_id = m_options.rack_id,
                                                            .conn_mac_id = rack_info.daemon_id,
                                                        });

        while (fu.wait_for(1ns) == std::future_status::timeout) {
            rpc.run_event_loop_once();
        }

        auto &conn_resp = fu.get();

        std::string peer_ip(conn_resp.rdma_ipv4.get_string());
        uint16_t peer_port(conn_resp.rdma_port);

        RDMARCConnectParam param;
        param.mac_id = m_daemon_id;
        param.role = CXL_DAEMON;

        DLOG("Connect with daemon %d [%s] ...", dd_conn->daemon_id, peer_ip.c_str());

        dd_conn->rdma_conn = std::make_unique<rdma_rc::RDMAConnection>();
        dd_conn->rdma_conn->connect(peer_ip, peer_port, &param, sizeof(param));

        m_conn_manager.AddConnection(dd_conn->daemon_id, dd_conn);

        DLOG("Connection with daemon %d OK", dd_conn->daemon_id);
    }
}

void DaemonContext::RegisterCXLMR() {
    uintptr_t cxl_start_ptr = reinterpret_cast<uintptr_t>(m_cxl_format.start_addr);
    // ! cxl mmapped is aligned by 2GB
    while (cxl_start_ptr < reinterpret_cast<uintptr_t>(m_cxl_format.end_addr)) {
        ibv_mr *mr = m_listen_conn.register_memory(reinterpret_cast<void *>(cxl_start_ptr),
                                                   mem_region_aligned_size);
        m_rdma_page_mr_table.push_back(mr);
        cxl_start_ptr += mem_region_aligned_size;
    }
}

ibv_mr *DaemonContext::GetMR(void *p) {
    if (p >= m_cxl_format.start_addr && p < m_cxl_format.end_addr) {
        return m_rdma_page_mr_table[(reinterpret_cast<uintptr_t>(p) -
                                     reinterpret_cast<const uintptr_t>(m_cxl_format.start_addr)) /
                                    mem_region_aligned_size];
    } else {
        return m_rdma_mr_table[reinterpret_cast<void *>(
            align_floor(reinterpret_cast<uintptr_t>(p), mem_region_aligned_size))];
    }
}

RemotePageMetaCache::RemotePageMetaCache(size_t max_recent_record, float hot_decay_lambda)
    : stats(max_recent_record, hot_decay_lambda, hot_stat_freq_timeout_interval) {}

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
    cmd.add<float>("hot_decay");
    cmd.add<size_t>("hot_swap_watermark");
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
    options.swap_zone_size = 100ul << 20;
    options.max_client_limit = 32;
    options.prealloc_cort_num = 8;
    options.hot_decay_lambda = cmd.get<float>("hot_decay");
    options.hot_swap_watermark = cmd.get<size_t>("hot_swap_watermark");

    DaemonContext &daemon_context = DaemonContext::getInstance();
    daemon_context.m_options = options;

    daemon_context.InitCXLPool();
    daemon_context.InitRDMARC();
    daemon_context.InitRPCNexus();
    daemon_context.ConnectWithMaster();
    daemon_context.RegisterCXLMR();

    std::thread log_worker = std::thread([&daemon_context]() {
        while (true) {
            std::this_thread::sleep_for(5s);
            DLOG("page hit: %lu, page miss: %lu, direct io: %lu, swap: %lu",
                 daemon_context.m_stats.page_hit, daemon_context.m_stats.page_miss,
                 daemon_context.m_stats.page_dio, daemon_context.m_stats.page_swap);
        }
    });

    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::round_robin>();
    while (true) {
        daemon_context.m_msgq_manager.rpc->run_event_loop_once();
        daemon_context.GetErpc().run_event_loop_once();
        boost::this_fiber::yield();
    }

    log_worker.join();

    return 0;
}