#include <type_traits>

#include "common.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "master_impl.hpp"
#include "proto/rpc_master.hpp"

MasterContext &MasterContext::getInstance() {
    static MasterContext master_ctx;
    return master_ctx;
}

void req_joinDaemon(erpc::ReqHandle *req_handle, void *context) {
    MasterContext &master_ctx = MasterContext::getInstance();
    erpc::ReqHandleWrap req_wrap(req_handle);

    auto resp_raw = req_wrap.get_pre_resp_msgbuf();
    auto req_raw = req_wrap.get_req_msgbuf();

    auto req = reinterpret_cast<rpc_master::JoinDaemonRequest *>(req_raw.get_buf());
    auto resp = reinterpret_cast<rpc_master::JoinDaemonReply *>(resp_raw.get_buf());

    MasterToDaemonConnection *daemon_connection = new MasterToDaemonConnection();
    *resp = rpc_master::joinDaemon(master_ctx, *daemon_connection, *req);

    DLOG("Connection with daemon %d OK", resp->your_mac_id);

    master_ctx.erpc_ctx.rpc_set[0].resize_msg_buffer(resp_raw, sizeof(rpc_master::JoinDaemonReply));
    master_ctx.erpc_ctx.rpc_set[0].enqueue_response(req_wrap, resp_raw);
}

int main(int argc, char *argv[]) {
    rchms::MasterOptions options;
    options.master_ip = "192.168.1.51";
    options.master_port = 31850;
    options.max_cluster_mac_num = 100;

    MasterContext &master_ctx = MasterContext::getInstance();
    master_ctx.options = options;

    master_ctx.page_id_allocator.reset(new IDGenerator());
    master_ctx.cluster_manager.mac_id_allocator.reset(new IDGenerator());

    master_ctx.cluster_manager.mac_id_allocator->addCapacity(
        master_ctx.options.max_cluster_mac_num);
    IDGenerator::id_t id = master_ctx.cluster_manager.mac_id_allocator->gen();
    DLOG_ASSERT(id == master_id, "Can't alloc master mac id");
    master_ctx.master_id = master_id;

    // TODO: 开始监听Daemon、client的连接

    std::string master_uri =
        master_ctx.options.master_ip + ":" + std::to_string(master_ctx.options.master_port);
    master_ctx.erpc_ctx.nexus.reset(new erpc::NexusWrap(master_uri));

    master_ctx.erpc_ctx.nexus->register_req_func(1, req_joinDaemon);

    erpc::SMHandlerWrap smhw;
    smhw.set_null();

    auto rpc = erpc::IBRpcWrap(master_ctx.erpc_ctx.nexus.get(), nullptr, 0, smhw);
    master_ctx.erpc_ctx.rpc_set.push_back(rpc);

    master_ctx.erpc_ctx.running = true;
    while (master_ctx.erpc_ctx.running) {
        rpc.run_event_loop_once();
    }

    return 0;
}