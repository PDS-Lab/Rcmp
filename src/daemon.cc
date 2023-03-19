#include <fcntl.h>
#include <sys/mman.h>

#include <cstdint>
#include <memory>
#include <type_traits>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "config.hpp"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"

struct ClientConnection {
    MsgQueue send_msg_queue;        // 发给client的msg queue
    MsgQueue recv_msg_queue;        // 接收client消息的msg queue

    std::string ip;
    uint16_t port;

    mac_id_t client_id;
};

struct PageMetadata {
    Allocator slab_allocator;
};

struct DaemonContext {
    rchms::DaemonOptions options;

    std::string ip;
    uint16_t port;

    mac_id_t daemon_id;     // 节点id，由master分配

    int cxl_devdax_fd;
    void *cxl_memory_addr;

    std::unique_ptr<Allocator> cxl_msg_queue_allocator;
    std::vector<ClientConnection> client_connect_table;
    std::unique_ptr<Allocator> cxl_page_allocator;
    ConcurrentHashMap<page_id_t, PageMetadata> page_table;
};

int main(int argc, char *argv[]) {
    rchms::DaemonOptions options;

    DaemonContext daemon_ctx;
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
        new Allocator(reinterpret_cast<uintptr_t>(daemon_ctx.cxl_memory_addr),
                      total_msg_queue_zone_size, daemon_ctx.options.cxl_msg_queue_size));

    void *cxl_page_start_addr = reinterpret_cast<void *>(
        reinterpret_cast<uintptr_t>(daemon_ctx.cxl_memory_addr) + total_msg_queue_zone_size);
    size_t total_page_size = daemon_ctx.options.cxl_memory_size - total_msg_queue_zone_size;
    size_t total_page_num = total_page_size / page_size;
    size_t swap_page_num = daemon_ctx.options.swap_zone_size / page_size;
    size_t total_free_page_num = total_page_num - swap_page_num;
    daemon_ctx.cxl_page_allocator.reset(new Allocator(
        reinterpret_cast<uintptr_t>(cxl_page_start_addr), total_page_size, page_size));

    // TODO: 与master建立连接

    // TODO: 开始监听master的RRPC

    // TODO: 开始监听client的msg queue

}