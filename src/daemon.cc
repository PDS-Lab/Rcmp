#include <fcntl.h>
#include <sys/mman.h>

#include "common.hpp"
#include "daemon_impl.hpp"
#include "log.hpp"
#include "proto/rpc_master.hpp"

DaemonContext &DaemonContext::getInstance() {
    static DaemonContext daemon_ctx;
    return daemon_ctx;
}

PageMetadata::PageMetadata(size_t slab_size) : slab_allocator(page_size, slab_size) {}

void joinDaemon_cb(void *_c, void *_tag) {
    auto f = reinterpret_cast<volatile bool *>(_tag);
    *f = true;
}

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
    daemon_ctx.options = options;

    // TODO: simulate cxl file open
    daemon_ctx.cxl_devdax_fd =
        open(daemon_ctx.options.cxl_devdax_path.c_str(), O_RDWR | O_CREAT, 0666);
    DLOG_ASSERT(daemon_ctx.cxl_devdax_fd != -1, "Failed to open cxl dev: %s",
                daemon_ctx.options.cxl_devdax_path.c_str());
    daemon_ctx.cxl_memory_addr =
        mmap(nullptr, daemon_ctx.options.cxl_memory_size, PROT_READ | PROT_WRITE, MAP_SHARED,
             daemon_ctx.cxl_devdax_fd, 0);
    DLOG_ASSERT(daemon_ctx.cxl_memory_addr != MAP_FAILED, "Failed to mmap cxl dev: %s",
                daemon_ctx.options.cxl_devdax_path.c_str());

    size_t total_msg_queue_zone_size =
        daemon_ctx.options.max_client_limit * daemon_ctx.options.cxl_msg_queue_size * 2;
    daemon_ctx.cxl_msg_queue_allocator.reset(
        new Allocator(total_msg_queue_zone_size, daemon_ctx.options.cxl_msg_queue_size));

    void *cxl_page_start_addr = reinterpret_cast<void *>(
        reinterpret_cast<uintptr_t>(daemon_ctx.cxl_memory_addr) + align_by(total_msg_queue_zone_size, page_size));
    size_t total_page_size = daemon_ctx.options.cxl_memory_size - total_msg_queue_zone_size;
    daemon_ctx.total_page_num = total_page_size / page_size;
    daemon_ctx.max_swap_page_num = daemon_ctx.options.swap_zone_size / page_size;
    daemon_ctx.max_data_page_num = daemon_ctx.total_page_num - daemon_ctx.max_swap_page_num;
    daemon_ctx.cxl_page_allocator.reset(new Allocator(total_page_size, page_size));

    daemon_ctx.current_used_page_num = 0;
    daemon_ctx.current_used_swap_page_num = 0;

    // TODO: 与master建立连接

    std::string server_uri =
        daemon_ctx.options.daemon_ip + ":" + std::to_string(daemon_ctx.options.daemon_port);
    daemon_ctx.erpc_ctx.nexus.reset(new erpc::NexusWrap(server_uri));

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();

    auto rpc = erpc::IBRpcWrap(daemon_ctx.erpc_ctx.nexus.get(), nullptr, 0, smhw);
    daemon_ctx.erpc_ctx.rpc_set.push_back(rpc);

    std::string master_uri =
        daemon_ctx.options.master_ip + ":" + std::to_string(daemon_ctx.options.master_port);
    daemon_ctx.erpc_ctx.master_session = rpc.create_session(master_uri, 0);

    auto req_raw = rpc.alloc_msg_buffer_or_die(sizeof(rpc_master::JoinDaemonRequest));
    auto resp_raw = rpc.alloc_msg_buffer_or_die(sizeof(rpc_master::JoinDaemonReply));

    auto req = reinterpret_cast<rpc_master::JoinDaemonRequest *>(req_raw.get_buf());
    req->free_page_num = daemon_ctx.max_data_page_num;
    req->rack_id = daemon_ctx.options.rack_id;
    req->with_cxl = daemon_ctx.options.with_cxl;

    volatile bool f = false;
    rpc.enqueue_request(daemon_ctx.erpc_ctx.master_session, 1, req_raw, resp_raw, joinDaemon_cb,
                        const_cast<bool *>(&f));

    while (!f) {
        rpc.run_event_loop_once();
    }

    auto resp = reinterpret_cast<rpc_master::JoinDaemonReply *>(resp_raw.get_buf());

    daemon_ctx.master_connection.ip = daemon_ctx.options.master_ip;
    daemon_ctx.master_connection.port = daemon_ctx.options.master_port;
    daemon_ctx.master_connection.master_id = resp->my_mac_id;
    daemon_ctx.daemon_id = resp->your_mac_id;

    DLOG_ASSERT(daemon_ctx.master_connection.master_id == master_id, "Fail to get master id");
    DLOG_ASSERT(daemon_ctx.daemon_id != master_id, "Fail to get daemon id");

    DLOG("Connection with master OK, my id is %d", daemon_ctx.daemon_id);

    getchar();
    getchar();

    // TODO: 开始监听master的RRPC

    // TODO: 开始监听client的msg queue

    return 0;
}