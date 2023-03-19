#include "daemon_impl.hpp"

#include <fcntl.h>
#include <sys/mman.h>

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
        new Allocator(total_msg_queue_zone_size, daemon_ctx.options.cxl_msg_queue_size));

    void *cxl_page_start_addr = reinterpret_cast<void *>(
        reinterpret_cast<uintptr_t>(daemon_ctx.cxl_memory_addr) + total_msg_queue_zone_size);
    size_t total_page_size = daemon_ctx.options.cxl_memory_size - total_msg_queue_zone_size;
    daemon_ctx.total_page_num = total_page_size / page_size;
    daemon_ctx.max_swap_page_num = daemon_ctx.options.swap_zone_size / page_size;
    daemon_ctx.max_data_page_num = daemon_ctx.total_page_num - daemon_ctx.max_swap_page_num;
    daemon_ctx.cxl_page_allocator.reset(new Allocator(total_page_size, page_size));

    daemon_ctx.current_used_page_num = 0;
    daemon_ctx.current_used_swap_page_num = 0;

    // TODO: 与master建立连接

    // TODO: 开始监听master的RRPC

    // TODO: 开始监听client的msg queue

    return 0;
}