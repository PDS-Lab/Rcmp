#include <boost/fiber/operations.hpp>
#include <memory>
#include <type_traits>

#include "cmdline.h"
#include "common.hpp"
#include "eRPC/erpc.h"
#include "impl.hpp"
#include "log.hpp"
#include "proto/rpc_adaptor.hpp"
#include "proto/rpc_master.hpp"
#include "proto/rpc_register.hpp"
#include "rdma_rc.hpp"

void MasterContext::InitCluster() {
    m_cluster_manager.mac_id_allocator = std::make_unique<IDGenerator>();
    m_cluster_manager.mac_id_allocator->Expand(m_options.max_cluster_mac_num);
    IDGenerator::id_t id = m_cluster_manager.mac_id_allocator->Gen();
    DLOG_ASSERT(id == master_id, "Can't alloc master mac id");
    m_master_id = master_id;

    m_page_directory.page_id_allocator = std::make_unique<IDGenerator>();
    m_page_directory.page_id_allocator->Expand(1);
    id = m_page_directory.page_id_allocator->Gen();
    // 保证page id不为0，这就保证了分配的GAddr是非null
    DLOG_ASSERT(id == 0, "Can't init page id");
}

void MasterContext::InitRDMARC() {
    rdma_rc::RDMAEnv::init();

    m_listen_conn.register_connect_hook([&](rdma_rc::RDMAConnection *rdma_conn, void *param_) {
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
                MasterToDaemonConnection *conn = dynamic_cast<MasterToDaemonConnection *>(conn_);
                conn->rdma_conn.reset(rdma_conn);
            } break;
        }
    });

    m_listen_conn.listen(m_options.master_ip);
}

void MasterContext::InitRPCNexus() {
    std::string master_uri = erpc::concat_server_uri(m_options.master_ip, m_options.master_port);
    m_erpc_ctx.nexus = std::make_unique<erpc::NexusWrap>(master_uri);

    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::joinDaemon)::rpc_type,
                                        bind_erpc_func<true>(rpc_master::joinDaemon));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::joinClient)::rpc_type,
                                        bind_erpc_func<true>(rpc_master::joinClient));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::allocPage)::rpc_type,
                                        bind_erpc_func<false>(rpc_master::allocPage));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::latchRemotePage)::rpc_type,
                                        bind_erpc_func<false>(rpc_master::latchRemotePage));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::unLatchRemotePage)::rpc_type,
                                        bind_erpc_func<false>(rpc_master::unLatchRemotePage));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(rpc_master::unLatchPageAndSwap)::rpc_type,
                                        bind_erpc_func<false>(rpc_master::unLatchPageAndSwap));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    erpc::IBRpcWrap rpc(m_erpc_ctx.nexus.get(), &MasterContext::getInstance(), 0, smhw);
    m_erpc_ctx.rpc_set.push_back(std::move(rpc));
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);
}

void MasterContext::InitFiberPool() { m_fiber_pool_.AddFiber(m_options.prealloc_fiber_num); }

int main(int argc, char *argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("master_ip");
    cmd.add<uint16_t>("master_port");
    bool ret = cmd.parse(argc, argv);
    DLOG_ASSERT(ret);

    rchms::MasterOptions options;
    options.master_ip = cmd.get<std::string>("master_ip");
    options.master_port = cmd.get<uint16_t>("master_port");

    MasterContext &master_context = MasterContext::getInstance();
    master_context.m_options = options;

    master_context.InitCluster();
    master_context.InitRPCNexus();
    master_context.InitRDMARC();
    master_context.InitFiberPool();

    DLOG("START OK");

    while (true) {
        master_context.GetErpc().run_event_loop_once();
        boost::this_fiber::yield();
    }

    return 0;
}