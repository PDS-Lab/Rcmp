#include "dht.hpp"

#include <unordered_set>
#include <vector>

#include "cmdline.h"
#include "log.hpp"
#include "rcmp.hpp"
#include "stats.hpp"
#include "utils.hpp"

using namespace std;

inline int rdd(int cli_id, int x) { return (((cli_id * 0xaf) ^ (0x45 * x)) ^ 0x89b31); }

template <typename T>
void shuffle(vector<T> &v, int seed) {
    for (int i = v.size() - 1; i >= 0; i--) {
        int r = rdd(seed, i) % (i + 1);
        swap(v[r], v[i]);
    }
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
    cmd.add<int>("read_ratio");
    cmd.add<int>("initor");
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

    HashTableRep h;
    h.pool = pool;

    if (cmd.get<int>("initor") == 1) {
        h.init();
        // return 0;
    } else {
        cin >> h.st;
    }

    // __DEBUG_START_PERF();
    // pool->__NotifyPerf();

    const size_t IT = cmd.get<size_t>("iteration");
    const int RA = cmd.get<int>("read_ratio");

    PerfStatistics ps;
    PerfStatistics hyr, hyw;

    {
        uint64_t start_time = getUsTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < IT; ++i) {
            h.put(rdd(0, i), rdd(0, i));
            uint64_t e = getUsTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getUsTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: %s random write test done. Use time: %ld us. Avg time: %f us\n", 0,
             "HashTable", diff, 1.0 * diff / IT);

        double throughput = 1.0 * (IT) / (end_time - start_time);
        cout << 0 << " clients total add throughput: " << throughput * 1000 << " Kops" << endl;

        {
            printf("p50: %fus, p99: %fus, p999: %fus, p9999: %fus\n", ps.getPercentile(50),
                   ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
        }
    }

    {
        vector<int> rv;
        for (int i = 0; i < IT; i++) {
            rv.push_back(rdd(0, i));
        }
        shuffle(rv, 0);

        uint64_t start_time = getUsTimestamp(), end_time;
        uint64_t tv = start_time;

        for (size_t i = 0; i < IT; ++i) {
            uint64_t e;
            if ((i % 100) < RA) {
                size_t ret = h.get(rv[i]);
                DLOG_EXPR(ret, ==, rv[i]);
                e = getUsTimestamp();
                hyr.addValue(e - tv);
            } else {
                int r = rdd(0, i) * rdd(0, i);
                h.put(r, r);
                e = getUsTimestamp();
                hyw.addValue(e - tv);
            }
            tv = e;
        }

        end_time = getUsTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: %s random op test done. Use time: %ld us. Avg time: %f us\n", 0, "HashTable",
             diff, 1.0 * diff / IT);

        double throughput = 1.0 * IT / (end_time - start_time);
        cout << "read ratio: " << RA << endl;
        cout << IT << " clients total op throughput: " << throughput * 1000 << " Kops" << endl;

        {
            printf("p50: %fus, p99: %fus, p999: %fus, p9999: %fus\n", hyr.getPercentile(50),
                   hyr.getPercentile(99), hyr.getPercentile(99.9), hyr.getPercentile(99.99));

            printf("p50: %fus, p99: %fus, p999: %fus, p9999: %fus\n", hyw.getPercentile(50),
                   hyw.getPercentile(99), hyw.getPercentile(99.9), hyw.getPercentile(99.99));
        }
    }

    // pool->__StopPerf();

    return 0;
}