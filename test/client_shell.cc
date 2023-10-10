#include <cassert>
#include <cstdint>
#include <vector>

#include "cmdline.h"
#include "log.hpp"
#include "rcmp.hpp"
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

    rcmp::ClientOptions options;
    options.client_ip = cmd.get<std::string>("client_ip");
    options.client_port = cmd.get<uint16_t>("client_port");
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.rack_id = cmd.get<uint32_t>("rack_id");
    options.with_cxl = true;

    rcmp::PoolContext *pool = rcmp::Open(options);

    while (1) {
        std::string cmdstr;
        cout << "> ";
        cin >> cmdstr;
        if (cmdstr == "a") {
            rcmp::GAddr gaddr = pool->AllocPage(1);
            cout << gaddr << endl;
        } else if (cmdstr == "f") {
            rcmp::GAddr gaddr;
            cin >> gaddr;
            pool->FreePage(gaddr, 1);
        } else if (cmdstr == "r") {
            rcmp::GAddr gaddr;
            cin >> gaddr;
            uint64_t n;
            pool->Read(gaddr, 8, &n);
            cout << n << endl;
        } else if (cmdstr == "w") {
            rcmp::GAddr gaddr;
            uint64_t n;
            cin >> gaddr >> n;
            pool->Write(gaddr, 8, &n);
        } else if (cmdstr == "cas") {
            rcmp::GAddr gaddr;
            uint64_t e, d;
            bool ret;
            cin >> gaddr >> e >> d;
            pool->CAS(gaddr, e, d, ret);
            if (!ret) {
                cout << "CAS Fail, expected " << e << endl;
            }
            cout << ret << endl;
        } else if (cmdstr == "?") {
            cout << "Usage:\n"
                    "\ta \t\t alloc 2MB gaddr\n"
                    "\tf \t\t free 2MB gaddr\n"
                    "\tr <gaddr> \t\t read gaddr 8B int\n"
                    "\tw <gaddr> <int> \t\twrite gaddr 8B int\n"
                    "\tcas <gaddr> <int> <int> \t\tcas gaddr 8B int, expected, desired\n"
                    "\tq \t\t quit\n"
                    "\t? \t\t for help"
                 << endl;
        } else {
        ill_err:
            cout << "Illegal Operation" << endl;
        }
    }

    return 0;
}