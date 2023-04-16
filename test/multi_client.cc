#include <cassert>
#include <cstdint>
#include <vector>

#include "cmdline.h"
#include "log.hpp"
#include "rchms.hpp"
#include "utils.hpp"

using namespace std;

int main(int argc, char *argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("client_ip");
    cmd.add<uint16_t>("client_port");
    cmd.add<uint32_t>("rack_id");
    cmd.add<std::string>("cxl_devdax_path");
    cmd.add<size_t>("cxl_memory_size");
    bool ret = cmd.parse(argc, argv);
    DLOG_ASSERT(ret);

    rchms::ClientOptions options;
    options.client_ip = cmd.get<std::string>("client_ip");
    options.client_port = cmd.get<uint16_t>("client_port");
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.rack_id = cmd.get<uint32_t>("rack_id");
    options.with_cxl = true;

    rchms::PoolContext *pool = rchms::Open(options);

    while (1) {
        std::string cmdstr;
        cout << "> ";
        cin >> cmdstr;
        if (cmdstr == "a") {
            rchms::GAddr gaddr = pool->Alloc(8);
            cout << gaddr << endl;
        } else if (cmdstr == "r") {
            rchms::GAddr gaddr;
            cin >> gaddr;
            uint64_t n;
            pool->Read(gaddr, 8, &n);
            cout << n << endl;
        } else if (cmdstr == "w") {
            rchms::GAddr gaddr;
            uint64_t n;
            cin >> gaddr >> n;
            pool->Write(gaddr, 8, &n);
        } else if (cmdstr == "run") {
            rchms::GAddr gaddr;
            size_t iteration;
            cin >> cmdstr >> gaddr >> iteration;
            uint64_t n;
            if (cmdstr == "r") {
                // __DEBUG_START_PERF();
                pool->__NotifyPerf();
                for (size_t i = 0; i < iteration; ++i) {
                    pool->Read(gaddr, 8, &n);
                }
                cout << "OK" << endl;
                pool->__StopPerf();
                exit(0);
            } else if (cmdstr == "w") {
                // __DEBUG_START_PERF();
                pool->__NotifyPerf();
                for (size_t i = 0; i < iteration; ++i) {
                    pool->Write(gaddr, 8, &n);
                }
                cout << "OK" << endl;
                pool->__StopPerf();
                exit(0);
            } else {
                goto ill_err;
            }
        } else if (cmdstr == "?") {
            cout << "Usage:\n"
                    "\ta \t\t alloc 8B int gaddr\n"
                    "\tr <gaddr> \t\t read gaddr 8B int\n"
                    "\tw <gaddr> <int> \t\twrite gaddr 8B int\n"
                    "\t? \t\t for help"
                 << endl;
        } else {
        ill_err:
            cout << "Illegal Operation" << endl;
        }
    }

    return 0;
}