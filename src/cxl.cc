#include "cxl.hpp"

#include <fcntl.h>
#include <sys/mman.h>

#include "log.hpp"

void *cxl_open_simulate(std::string file, size_t size, int *fd) {
    *fd = open(file.c_str(), O_RDWR | O_CREAT, 0666);
    DLOG_ASSERT(*fd != -1, "Failed to open cxl dev: %s", file.c_str());
    void *addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, *fd, 0);
    DLOG_ASSERT(addr != MAP_FAILED, "Failed to mmap cxl dev: %s", file.c_str());
    return addr;
}