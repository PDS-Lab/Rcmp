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
    cmd.add<int>("thread");
    cmd.add<int>("thread_all", 0, "", false, 0);
    cmd.add<int>("node_id");
    cmd.add<int>("no_node");
    cmd.add<std::string>("redis_server_ip");
    bool ret = cmd.parse(argc, argv);
    if (!ret) {
        DLOG_FATAL("%s", cmd.error_full().c_str());
    }

    rchms::ClientOptions options;
    options.client_ip = cmd.get<std::string>("client_ip");
    options.client_port = cmd.get<uint16_t>("client_port");
    options.cxl_devdax_path = cmd.get<std::string>("cxl_devdax_path");
    options.cxl_memory_size = cmd.get<size_t>("cxl_memory_size");
    options.rack_id = cmd.get<uint32_t>("rack_id");
    options.with_cxl = true;

    // pool.ref->__NotifyPerf();

    int thread = cmd.get<int>("thread");
    RCHMSMemPool instance;
    vector<MemPoolBase *> instances;
    rchms::ClientOptions op = options;
    instance.ref = rchms::Open(op);
    for (int i = 0; i < cmd.get<int>("thread"); ++i) {
        instances.push_back(&instance);
    }

    BenchParam param = {
        .NID = cmd.get<int>("node_id"),
        .NODES = cmd.get<int>("no_node"),
        .IT = cmd.get<size_t>("iteration"),
        // .TH = i,
        .RA = cmd.get<int>("read_ratio"),
        // .PAYLOAD = cmd.get<size_t>("payload_size"),
        // .PAYLOAD = p,
        .SA = 2097152,
        .RANGE = cmd.get<uint64_t>("addr_range"),
        .ZIPF = 0.99,
        .redis_server_ip = cmd.get<string>("redis_server_ip"),
        .instances = instances,
    };

    run_init(param);

    for (size_t payload = 64; payload <= 64; payload *= 2) {
        for (int th = (cmd.get<int>("thread_all") != 0) ? 1 : thread; th <= thread; th *= 2) {
            param.TH = th;
            param.PAYLOAD = payload;
            run_bench(param);
            instance.ref->__ClearStats();
            run_bench(param);
        }
        instance.ref->__DumpStats();
        instance.ref->__ClearStats();
    }

    rchms::Close(instance.ref);

    // pool.ref->__DumpStats();

    // pool.ref->__StopPerf();

    return 0;
}