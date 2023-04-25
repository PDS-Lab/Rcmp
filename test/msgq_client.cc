#include <cassert>

#include "log.hpp"
#include "rchms.hpp"

int main(int argc, char *argv[]) {
    assert(argc == 2);
    rchms::ClientOptions options;
    options.client_ip = "192.168.1.51";
    options.client_port = 37846 + atoi(argv[1]);

    options.cxl_devdax_path = "testfile";
    options.cxl_memory_size = 10 << 20;
    options.rack_id = 0;
    options.with_cxl = true;

    rchms::PoolContext *pool = rchms::Open(options);

    const size_t count = 100000;
    const size_t size1 = 64;
    const size_t size2 = 72;
    int a[size1];
    int b[size2];
    for (size_t i = 0; i < size1; i++) {
        a[i] = -1;
    }
    for (size_t i = 0; i < size2; i++) {
        b[i] = -1;
    }
    for (size_t i = 0; i < count; i++) {
        if (atoi(argv[1]) == 0) {
            pool->__TestDataSend1(a, size1);
        } else {
            pool->__TestDataSend2(b, size2);
        }
    }

    DLOG("DataSend finished.\n");

    return 0;
}