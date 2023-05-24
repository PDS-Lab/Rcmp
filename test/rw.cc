#include "cmdline.h"
#include "log.hpp"
#include "microbench_core.hpp"
#include "options.hpp"
#include "rchms.hpp"
#include "stats.hpp"
#include "utils.hpp"

using namespace std;

struct RCHMSMemPool : public MemPoolBase {
    virtual GAddr Alloc(size_t s) override { return ref->AllocPage(s / alloc_unit); }
    virtual void Write(GAddr gaddr, size_t s, void *buf) override { ref->Write(gaddr, s, buf); }
    virtual void WriteBatch(GAddr gaddr, size_t s, void *buf) override {
        // ref->WriteBatch(gaddr, s, buf);
    }
    virtual void Read(GAddr gaddr, size_t s, void *buf) override { ref->Read(gaddr, s, buf); }

    rchms::PoolContext *ref;
};

int main(int argc, char *argv[]) {
    cmdline::parser cmd;
    cmd.add<std::string>("client_ip");
    cmd.add<uint16_t>("client_port");
    cmd.add<uint32_t>("rack_id");
    cmd.add<std::string>("cxl_devdax_path");
    cmd.add<size_t>("cxl_memory_size");
    cmd.add<size_t>("iteration");
    cmd.add<size_t>("payload_size");
    cmd.add<uint64_t>("addr_range");
    cmd.add<int>("read_ratio");
    cmd.add<uint64_t>("start_addr");
    cmd.add<size_t>("alloc_page_cnt");
    cmd.add<int>("thread");
    cmd.add<int>("node_id");
    cmd.add<int>("no_node");
    bool ret = cmd.parse(argc, argv);
    DLOG_ASSERT(ret);

    rchms::ClientOptions options;
    options.client_ip = cmd.get<std::string>("client_ip");
    options.client_port = cmd.get<uint16_t>("client_port");
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.rack_id = cmd.get<uint32_t>("rack_id");
    options.with_cxl = true;

    // pool.ref->__NotifyPerf();

    vector<RCHMSMemPool> th_instances;
    vector<MemPoolBase *> instances;
    for (int i = 0; i < cmd.get<int>("thread"); ++i) {
        RCHMSMemPool _p;
        rchms::ClientOptions op = options;
        op.client_port += i;
        _p.ref = rchms::Open(op);
        th_instances.push_back(_p);
    }
    for (int i = 0; i < th_instances.size(); ++i) {
        instances.push_back(&th_instances[0]);
    }

    run_bench({
        .NID = cmd.get<int>("node_id"),
        .NODES = cmd.get<int>("no_node"),
        .IT = cmd.get<size_t>("iteration"),
        .TH = cmd.get<int>("thread"),
        .RA = cmd.get<int>("read_ratio"),
        .PAYLOAD = cmd.get<size_t>("payload_size"),
        .SA = cmd.get<uint64_t>("start_addr"),
        .RANGE = cmd.get<uint64_t>("addr_range"),
        .APC = cmd.get<size_t>("alloc_page_cnt"),
        .ZIPF = 0.99,
        .instances = instances,
    });

    // pool.ref->__DumpStats();

    // pool.ref->__StopPerf();

    return 0;
}