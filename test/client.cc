#include <cassert>
#include <vector>

#include "log.hpp"
#include "rchms.hpp"

using namespace std;

int main(int argc, char *argv[]) {
    rchms::ClientOptions options;
    options.client_ip = "192.168.1.51";
    options.client_port = 37846;
    options.cxl_devdax_path = "testfile";
    options.cxl_memory_size = 20 << 20;
    options.rack_id = 0;
    options.with_cxl = true;

    rchms::PoolContext *pool = rchms::Open(options);

    vector<rchms::GAddr> v;
    for (int i = 0; i < 100000; ++i) {
        v.push_back(pool->Alloc(sizeof(int)));
    }
    for (int i = 0; i < v.size(); ++i) {
        pool->Write(v[i], sizeof(int), &i);
    }
    for (int i = 0; i < v.size(); ++i) {
        int n;
        pool->Read(v[i], sizeof(int), &n);
        DLOG_EXPR(n, ==, i);
    }

    DLOG("OK");

    return 0;
}