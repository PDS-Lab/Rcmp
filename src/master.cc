#include <type_traits>

#include "common.hpp"
#include "eRPC/erpc.h"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc.hpp"
#include "proto/rpc_adaptor.hpp"

MasterContext &MasterContext::getInstance() {
    static MasterContext master_ctx;
    return master_ctx;
}

MasterConnection *MasterContext::get_connection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != m_master_id, "Can't find self connection");
    MasterConnection *ctx;
    bool ret = m_cluster_manager.connect_table.find(mac_id, &ctx);
    DLOG_ASSERT(ret, "Can't find mac %d", mac_id);
    return ctx;
}

erpc::IBRpcWrap MasterContext::get_erpc() { return m_erpc_ctx.rpc_set[0]; }

int main(int argc, char *argv[]) {
    rchms::MasterOptions options;
    options.master_ip = "192.168.1.51";
    options.master_port = 31850;
    options.max_cluster_mac_num = 100;

    MasterContext &master_ctx = MasterContext::getInstance();
    master_ctx.m_options = options;

    master_ctx.m_page_id_allocator.reset(new IDGenerator());
    master_ctx.m_cluster_manager.mac_id_allocator.reset(new IDGenerator());

    master_ctx.m_cluster_manager.mac_id_allocator->addCapacity(
        master_ctx.m_options.max_cluster_mac_num);
    IDGenerator::id_t id = master_ctx.m_cluster_manager.mac_id_allocator->gen();
    DLOG_ASSERT(id == master_id, "Can't alloc master mac id");
    master_ctx.m_master_id = master_id;

    // TODO: 开始监听Daemon、client的连接

    std::string master_uri =
        master_ctx.m_options.master_ip + ":" + std::to_string(master_ctx.m_options.master_port);
    master_ctx.m_erpc_ctx.nexus.reset(new erpc::NexusWrap(master_uri));

    using JoinDaemonRPC = RPC_TYPE_STRUCT(rpc_master::joinDaemon);

    master_ctx.m_erpc_ctx.nexus->register_req_func(JoinDaemonRPC::rpc_type, bind_erpc_func<true>(rpc_master::joinDaemon));

    erpc::SMHandlerWrap smhw;
    smhw.set_null();

    auto rpc =
        erpc::IBRpcWrap(master_ctx.m_erpc_ctx.nexus.get(), &MasterContext::getInstance(), 0, smhw);
    master_ctx.m_erpc_ctx.rpc_set.push_back(rpc);
    DLOG_ASSERT(master_ctx.m_erpc_ctx.rpc_set.size() == 1);

    master_ctx.m_erpc_ctx.running = true;
    while (master_ctx.m_erpc_ctx.running) {
        rpc.run_event_loop_once();
    }

    return 0;
}