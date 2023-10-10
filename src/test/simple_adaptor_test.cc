#include <boost/coroutine2/all.hpp>
#include <boost/fiber/algo/round_robin.hpp>
#include <boost/fiber/future/async.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/policy.hpp>
#include <set>

#include "cmdline.h"
#include "common.hpp"
#include "impl.hpp"
#include "log.hpp"
#include "options.hpp"
#include "promise.hpp"
#include "proto/rpc_caller.hpp"
#include "proto/rpc_master.hpp"
#include "utils.hpp"

using namespace std;
using namespace std::chrono_literals;

struct Request {
    mac_id_t mac_id;
    char data[64];
};
struct Reply {
    char data[64];
};
void dummy(MasterContext& master_context, MasterToDaemonConnection& daemon_connection, Request& req,
           ResponseHandle<Reply>& resp_handle);

struct JoinReq {
    mac_id_t mac_id;
};
struct JoinReply {
    mac_id_t daemon_mac_id;
    mac_id_t master_mac_id;
};
void joinDaemon(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                JoinReq& req, ResponseHandle<JoinReply>& resp_handle);

BIND_RPC_TYPE_STRUCT(joinDaemon);
BIND_RPC_TYPE_STRUCT(dummy);

void joinDaemon(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                JoinReq& req, ResponseHandle<JoinReply>& resp_handle) {
    mac_id_t mac_id = master_context.m_cluster_manager.mac_id_allocator->Gen();
    daemon_connection.daemon_id = mac_id;

    master_context.m_cluster_manager.connect_table.insert(daemon_connection.daemon_id,
                                                          &daemon_connection);

    resp_handle.Init();
    auto& reply = resp_handle.Get();
    reply.daemon_mac_id = mac_id;
    reply.master_mac_id = master_context.m_master_id;

    DLOG("Connect with daemon [rack:%d --- id:%d]", daemon_connection.rack_id,
         daemon_connection.daemon_id);
}

Request Greq;

void dummy(MasterContext& master_context, MasterToDaemonConnection& daemon_connection, Request& req,
           ResponseHandle<Reply>& resp_handle) {
    resp_handle.Init(0);
    auto& reply = resp_handle.Get();
    memcpy(reply.data, req.data, sizeof(req.data));
}

void MasterContext::InitCluster() {
    m_cluster_manager.mac_id_allocator = std::make_unique<IDGenerator>();
    m_cluster_manager.mac_id_allocator->Expand(m_options.max_cluster_mac_num);
    IDGenerator::id_t id = m_cluster_manager.mac_id_allocator->Gen();
    DLOG_ASSERT(id == master_id, "Can't alloc master mac id");
    m_master_id = master_id;
}

void MasterContext::InitRPCNexus() {
    std::string master_uri = erpc::concat_server_uri(m_options.master_ip, m_options.master_port);
    m_erpc_ctx.nexus = std::make_unique<erpc::NexusWrap>(master_uri);

    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(joinDaemon)::rpc_type,
                                        bind_erpc_func<true>(joinDaemon));
    m_erpc_ctx.nexus->register_req_func(RPC_TYPE_STRUCT(dummy)::rpc_type,
                                        bind_erpc_func<true>(dummy));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    erpc::IBRpcWrap rpc(m_erpc_ctx.nexus.get(), &MasterContext::getInstance(), 0, smhw);
    m_erpc_ctx.rpc_set.push_back(std::move(rpc));
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);
}

void DaemonContext::InitRPCNexus() {
    // 1. init erpc
    std::string server_uri = erpc::concat_server_uri(m_options.daemon_ip, m_options.daemon_port);
    m_erpc_ctx.nexus = std::make_unique<erpc::NexusWrap>(server_uri);

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    erpc::IBRpcWrap rpc(m_erpc_ctx.nexus.get(), this, 0, smhw);
    m_erpc_ctx.rpc_set.push_back(std::move(rpc));
    DLOG_ASSERT(m_erpc_ctx.rpc_set.size() == 1);
}

void DaemonContext::ConnectWithMaster() {
    auto& rpc = GetErpc();

    auto& master_connection = m_conn_manager.GetMasterConnection();

    master_connection.erpc_conn =
        std::make_unique<ErpcClient>(rpc, m_options.master_ip, m_options.master_port);

    auto fu = master_connection.erpc_conn->call<SpinPromise>(joinDaemon, {});

    while (fu.wait_for(1ns) == std::future_status::timeout) {
        rpc.run_event_loop_once();
    }

    auto& resp = fu.get();

    master_connection.ip = m_options.master_ip;
    master_connection.port = m_options.master_port;
    master_connection.master_id = resp.master_mac_id;
    m_daemon_id = resp.daemon_mac_id;

    DLOG_ASSERT(master_connection.master_id == master_id, "Fail to get master id");
    DLOG_ASSERT(m_daemon_id != master_id, "Fail to get daemon id");

    DLOG("Connection with master OK, my id is %d", m_daemon_id);
}

void erpc_general_promise_cb(void*, void* pr) {
    SpinPromise<void>* pro = reinterpret_cast<SpinPromise<void>*>(pr);
    pro->set_value();
}

int main(int argc, char* argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("master_ip");
    cmd.add<std::string>("daemon_ip", 0, "", false);
    cmd.add<bool>("is_master");
    bool ret = cmd.parse(argc, argv);
    DLOG_ASSERT(ret);

    for (int i = 0; i < sizeof(Greq.data); ++i) {
        Greq.data[i] = i;
    }

    if (cmd.get<bool>("is_master")) {
        rcmp::MasterOptions options;
        options.master_ip = cmd.get<std::string>("master_ip");
        options.master_port = 31850;

        MasterContext& master_context = MasterContext::getInstance();
        master_context.m_options = options;

        master_context.InitCluster();
        master_context.InitRPCNexus();

        DLOG("START OK");

        while (true) {
            master_context.GetErpc().run_event_loop_once();
            boost::this_fiber::yield();
        }
    }

    else {
        rcmp::DaemonOptions options;
        options.master_ip = cmd.get<std::string>("master_ip");
        options.master_port = 31850;
        options.daemon_ip = cmd.get<std::string>("daemon_ip");
        options.daemon_port = 31851;
        options.rack_id = 0;
        options.with_cxl = true;

        DaemonContext& daemon_context = DaemonContext::getInstance();
        daemon_context.m_options = options;

        daemon_context.InitRPCNexus();
        daemon_context.ConnectWithMaster();

        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::round_robin>();

        uint64_t cnt = 0;
        uint64_t success_cnt = 0;

        while (true) {
            daemon_context.GetErpc().run_event_loop_once();
            boost::this_fiber::yield();

            {
                if (cnt == 100000) {
                    if (success_cnt == cnt) break;
                    continue;
                }
                if (cnt - success_cnt >= 8) continue;

                boost::fibers::async(boost::fibers::launch::dispatch, [&]() {
                    Request req = Greq;
                    req.mac_id = daemon_context.m_daemon_id;
                    req.data[rand() % sizeof(req.data)] = getUsTimestamp();

                    auto fu = daemon_context.m_conn_manager.GetMasterConnection()
                                  .erpc_conn->call<CortPromise>(dummy, std::move(req));

                    auto& resp = fu.get();

                    for (int i = 0; i < sizeof(req.data); ++i) {
                        DLOG_ASSERT(resp.data[i] == req.data[i]);
                    }

                    success_cnt++;
                });

                cnt++;
            }
        }
    }

    return 0;
}