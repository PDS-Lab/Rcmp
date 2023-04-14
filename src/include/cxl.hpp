#pragma once

#include <string>

constexpr static size_t cxl_super_block_size = 4096;

struct CXLSuperBlock {
    size_t total_size;
    size_t msgq_zone_size;
    size_t reserve_heap_size;
    size_t page_data_zone_size;
};

/**
 * @brief cxl memory block format
 * 
 *   2GB align                               2GB align
 *
 *      0     4096        4096+msgq    align(4096+msgq,2GB)                 align(psize)     total
 *
 *      [sp blk][    msgq    ][     reserve     ][             page data           ][   unused   ]
 */

struct CXLMemFormat {
    const void *start_addr;
    CXLSuperBlock *super_block;
    void *msgq_zone_start_addr;
    void *reserve_zone_addr;
    void *page_data_start_addr;
    const void *end_addr;
};

void *cxl_open_simulate(std::string file, size_t size, int *fd);
void cxl_close_simulate(int fd, CXLMemFormat &format);
void cxl_memory_init(CXLMemFormat &format, void* cxl_memory_addr, size_t size, size_t msgq_zone_size);
void cxl_memory_open(CXLMemFormat &format,void* cxl_memory_addr);
