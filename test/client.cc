#include <cassert>
#include "log.hpp"
#include "rchms.hpp"

int main(int argc, char *argv[]) {
    rchms::ClientOptions options;
    options.client_ip = "192.168.1.51";
    options.client_port = 37846;
    options.cxl_devdax_path = "testfile";
    options.cxl_memory_size =  10 << 20;
    options.rack_id = 0;
    options.with_cxl = true;

    rchms::PoolContext *pool = rchms::Open(options);

    int n = 4;
    rchms::GAddr g1 = pool->Alloc(sizeof(n));
    DLOG("g1: %lx", g1);
    pool->Write(g1, sizeof(n), &n);
    n = 0;
    pool->Read(g1, sizeof(n), &n);
    DLOG_ASSERT(n == 4);
    
    getchar();
    getchar();
    getchar();

    return 0;
}