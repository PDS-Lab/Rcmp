#include <chrono>
#include <future>

#include "common.hpp"
#include "cxl.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"

using namespace std::chrono_literals;

DaemonContext &DaemonContext::getInstance() {
    static DaemonContext daemon_ctx;
    return daemon_ctx;
}

void DaemonContext::createCXLPool() {
    // 1. 打开cxl设备映射
    m_cxl_memory_addr =
        cxl_open_simulate(m_options.cxl_devdax_path, m_options.cxl_memory_size, &m_cxl_devdax_fd);

    // 2. 初始化CN连接
    size_t total_msg_queue_zone_size =
        m_options.max_client_limit * m_options.cxl_msg_queue_size * 2;
    m_cxl_msg_queue_allocator.reset(
        new SingleAllocator(total_msg_queue_zone_size, m_options.cxl_msg_queue_size));

    // 3. 确认page个数
    void *cxl_page_start_addr =
        reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(m_cxl_memory_addr) +
                                 align_by(total_msg_queue_zone_size, page_size));
    size_t total_page_size = m_options.cxl_memory_size - total_msg_queue_zone_size;
    m_total_page_num = total_page_size / page_size;
    m_max_swap_page_num = m_options.swap_zone_size / page_size;
    m_max_data_page_num = m_total_page_num - m_max_swap_page_num;
    m_cxl_page_allocator.reset(new SingleAllocator(total_page_size, page_size));

    m_current_used_page_num = 0;
    m_current_used_swap_page_num = 0;
}

void DaemonContext::connectWithMaster() {
    std::string server_uri = m_options.daemon_ip + ":" + std::to_string(m_options.daemon_port);
    m_erpc_ctx.nexus.reset(new erpc::NexusWrap(server_uri));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    auto rpc = erpc::IBRpcWrap(m_erpc_ctx.nexus.get(), nullptr, 0, smhw);
    m_erpc_ctx.rpc_set.push_back(rpc);
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);
    rpc = get_erpc();

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
    options.master_ip = "192.168.1.51";
    options.master_port = 31850;
    options.daemon_ip = "192.168.1.51";
    options.daemon_port = 31851;
    options.rack_id = 0;
    options.with_cxl = true;
    options.cxl_devdax_path = "testfile";
    options.cxl_memory_size = 10 << 20;
    options.swap_zone_size = 2 << 20;
    options.max_client_limit = 2;
    options.cxl_msg_queue_size = 4 << 10;

    DaemonContext &daemon_ctx = DaemonContext::getInstance();
    daemon_ctx.m_options = options;

    daemon_ctx.createCXLPool();
    daemon_ctx.connectWithMaster();

    // TODO: 开始监听master的RRPC

    // TODO: 开始监听client的msg queue

    getchar();
    getchar();
    return 0;
}