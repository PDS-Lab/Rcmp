#include <cassert>

#include "log.hpp"
#include "rchms.hpp"
#include "../test/common_sta.h"

int main(int argc, char *argv[]) {
    rchms::ClientOptions options;
    options.client_ip = "192.168.1.88";
    assert(argc == 2);
    options.client_port = 37846 + atoi(argv[1]);

    options.cxl_devdax_path = "../testfile";
    options.cxl_memory_size = 10 << 20;
    options.rack_id = 0;
    options.with_cxl = true;

    rchms::PoolContext *pool = rchms::Open(options);

    #if 1
    int n = 4;
    // rchms::GAddr g1 = pool->Alloc(sizeof(n));
    // DLOG("g1: %lx", g1);
    // pool->Write(g1, sizeof(n), &n);
    // n = 0;
    // pool->Read(g1, sizeof(n), &n);
    // DLOG_ASSERT(n == 4);
    
    const size_t count = 100000;
    const size_t size  = 64;
    const size_t size1  = 72;
    int a[size];
    int b[size1];
    for (size_t i = 0; i < size; i++)
    {
        a[i] = -1;
        // a[i] = i + atoi(argv[1]);
    }
    for (size_t i = 0; i < size1; i++)
    {
        b[i] = -1;
        // a[i] = i + atoi(argv[1]);
    }
    for (size_t i = 0; i < count; i++)
    {
        if ( atoi(argv[1]) == 0 )
        {
            pool->DataSend(a, size);
        }
        else
        {
            pool->DataSend1(b, size1);
            // rchms::GAddr g1 = pool->Alloc(sizeof(n));
        }
        
        
    }
    
    
    #endif

    // const size_t count = 1000000;
    // const size_t size  = 64;
    // int a[size];
    // ChronoTimer time_start;
    // double end;
    
    // for (size_t i = 0; i < count; i++)
    // {
    //     // a[0] = i;
    //     time_start.reset();
    //     pool->DataSend(a, size);
    //     end = time_start.get_us();
    //     printf("latency = %f\n", end);
    // }
    DLOG("DataSend finished.\n");

    getchar();
    getchar();
    getchar();

    return 0;
}