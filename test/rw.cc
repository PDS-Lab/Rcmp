#include "cmdline.h"
#include "log.hpp"
#include "options.hpp"
#include "rchms.hpp"
#include "stats.hpp"
#include "utils.hpp"

using namespace std;

inline int rdd(int cli_id, int x) {
    long m = 0xc6a4a7935bd1e995L;
    int h = 97 ^ 4;
    int r = cli_id ^ 47;

    x *= m;
    x ^= x >> r;
    x *= m;
    h *= m;
    h ^= x;

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

struct PerfStatistics : public Histogram {
    PerfStatistics() : Histogram(100000, 0, 100000) {}
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

    // __DEBUG_START_PERF();
    // pool->__NotifyPerf();

    const size_t IT = cmd.get<size_t>("iteration");
    const int RA = cmd.get<int>("read_ratio");
    const size_t PAYLOAD = cmd.get<size_t>("payload_size");
    const size_t RANGE = cmd.get<uint64_t>("addr_range");
    const rchms::GAddr SA = cmd.get<uint64_t>("start_addr");
    const size_t APC = cmd.get<size_t>("alloc_page_cnt");

    rchms::GAddr ga = pool->AllocPage(APC);
    DLOG_EXPR(ga, ==, SA);

    PerfStatistics ps;
    PerfStatistics hyr, hyw;

    uint8_t raw[PAYLOAD];

    {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < IT; ++i) {
            int r = rdd(0, i);
            r %= RANGE - PAYLOAD;
            r += SA;
            pool->Write(r, PAYLOAD, raw);
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random write test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / IT);

        double throughput = 1.0 * (IT) / (end_time - start_time);
        DLOG("%d clients total write throughput: %f Kops", 0, throughput * 100);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < IT; ++i) {
            int r = rdd(0, i);
            r %= RANGE - PAYLOAD;
            r += SA;
            pool->Read(r, PAYLOAD, raw);
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random read test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / IT);

        double throughput = 1.0 * (IT) / (end_time - start_time);
        DLOG("%d clients total read throughput: %f Kops", 0, throughput * 100);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    {
        uint64_t start_time = getTimestamp(), end_time;
        uint64_t tv = start_time;

        for (size_t i = 0; i < IT; ++i) {
            uint64_t e;
            int r = rdd(0, i);
            if ((r % 100) < RA) {
                r %= RANGE - PAYLOAD;
                r += SA;
                pool->Read(r, PAYLOAD, raw);
                e = getTimestamp();
                hyr.addValue(e - tv);
            } else {
                r %= RANGE - PAYLOAD;
                r += SA;
                pool->Write(r, PAYLOAD, raw);
                e = getTimestamp();
                hyw.addValue(e - tv);
            }
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random op test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / IT);

        double throughput = 1.0 * IT / (end_time - start_time);
        DLOG("read ratio: %d", RA);
        DLOG("%d clients total op throughput: %f Kops", 0, throughput * 100);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyr.getPercentile(50),
             hyr.getPercentile(99), hyr.getPercentile(99.9), hyr.getPercentile(99.99));

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyw.getPercentile(50),
             hyw.getPercentile(99), hyw.getPercentile(99.9), hyw.getPercentile(99.99));
    }

    // pool->__StopPerf();

    return 0;
}