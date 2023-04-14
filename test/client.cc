#include <cassert>
#include <vector>

#include "cmdline.h"
#include "log.hpp"
#include "rchms.hpp"

using namespace std;

int main(int argc, char *argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("client_ip");
    cmd.add<uint16_t>("client_port");
    cmd.add<uint32_t>("rack_id");
    cmd.add<std::string>("cxl_devdax_path");
    cmd.add<size_t>("cxl_memory_size");
    cmd.parse(argc, argv);

    rchms::ClientOptions options;
    options.client_ip = cmd.get<std::string>("client_ip");
    options.client_port = cmd.get<uint16_t>("client_port");
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.rack_id = cmd.get<uint32_t>("rack_id");
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