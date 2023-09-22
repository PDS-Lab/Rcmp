#include "cxl.hpp"

#include <fcntl.h>
#include <sys/mman.h>

#include <cstdint>
#include <cstdlib>

#include "config.hpp"
#include "log.hpp"
#include "utils.hpp"

void *cxl_open_simulate(std::string file, size_t size, int *fd) {
    *fd = open(file.c_str(), O_RDWR | O_CREAT, 0666);
    DLOG_ASSERT(*fd != -1, "Failed to open cxl dev: %s", file.c_str());

    void *addr = aligned_alloc(mem_region_aligned_size, size);
    free(addr);
    addr = mmap(addr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED | MAP_LOCKED, *fd, 0);
    DLOG_ASSERT(addr != MAP_FAILED, "Failed to mmap cxl dev: %s", file.c_str());
    return addr;
}

void cxl_close_simulate(int fd, CXLMemFormat &format) {
    munmap(const_cast<void *>(format.start_addr), format.super_block->total_size);
    close(fd);
}

void cxl_memory_init(CXLMemFormat &format, void *cxl_memory_addr, size_t size,
                     size_t msgq_zone_size) {
    DLOG_ASSERT(size > mem_region_aligned_size, "The size of cxl memory needs larger than 2GB");

    CXLSuperBlock *super_block = reinterpret_cast<CXLSuperBlock *>(cxl_memory_addr);
    super_block->total_size = size;
    super_block->msgq_zone_size = msgq_zone_size;
    super_block->reserve_heap_size =
        align_ceil(cxl_super_block_size + msgq_zone_size, mem_region_aligned_size) -
        (cxl_super_block_size + msgq_zone_size);
    super_block->page_data_zone_size = align_floor(
        size - cxl_super_block_size - msgq_zone_size - super_block->reserve_heap_size, page_size);

    cxl_memory_open(format, cxl_memory_addr);
}

void cxl_memory_open(CXLMemFormat &format, void *cxl_memory_addr) {
    format.start_addr = cxl_memory_addr;
    format.super_block = reinterpret_cast<CXLSuperBlock *>(cxl_memory_addr);
    format.msgq_zone_start_addr = reinterpret_cast<void *>(
        (reinterpret_cast<uintptr_t>(cxl_memory_addr) + cxl_super_block_size));
    format.reserve_zone_addr =
        reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(format.msgq_zone_start_addr) +
                                  format.super_block->msgq_zone_size));
    format.page_data_start_addr =
        reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(format.reserve_zone_addr) +
                                  format.super_block->reserve_heap_size));
    format.end_addr =
        reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(format.page_data_start_addr) +
                                  format.super_block->page_data_zone_size));

    DLOG("super_block: %p", format.super_block);
    DLOG("msgq_zone_start_addr: %p", format.msgq_zone_start_addr);
    DLOG("reserve_zone_addr: %p", format.reserve_zone_addr);
    DLOG("page_data_start_addr: %p", format.page_data_start_addr);
    DLOG("end_addr: %p", format.end_addr);
}