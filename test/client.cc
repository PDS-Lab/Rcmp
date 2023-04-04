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
    
    getchar();
    getchar();
    getchar();

    return 0;
}