#include <cstdint>
#include <random>

#include "log.hpp"
#include "stats.hpp"
#include "utils.hpp"

using namespace std;

inline long rdd(long cli_id, long x) {
    long m = 0xc6a4a7935bd1e995L;
    long h = 97 ^ 4;
    long r = cli_id ^ 47;

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
    PerfStatistics() : Histogram(1000000, 0, 1000000) {}
};

struct MemPoolBase {
    using GAddr = uintptr_t;
    constexpr static size_t alloc_unit = 2ul << 20;

    virtual GAddr Alloc(size_t s) = 0;
    virtual void Write(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void Read(GAddr gaddr, size_t s, void *buf) = 0;
};

struct BenchParam {
    size_t IT;              // iteration
    int RA;                 // read ratio
    size_t PAYLOAD;         // payload size
    MemPoolBase::GAddr SA;  // start gaddr
    size_t RANGE;           // gaddr range [SA, SA+RANGE)
    size_t APC;             // alloc page count
    float ZIPF;             // zipf α
};

inline void run_bench(MemPoolBase *pool, BenchParam param) {
    DLOG("start running ...");

    MemPoolBase::GAddr ga = pool->Alloc(param.APC * (MemPoolBase::alloc_unit));
    DLOG_EXPR(ga, ==, param.SA);

    DLOG("start testing ...");

    PerfStatistics ps;
    PerfStatistics hyr, hyw;

    vector<uint8_t> raw(param.PAYLOAD);

    if (1) {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < param.IT; ++i) {
            size_t r = rdd(0, i);
            r %= param.RANGE - param.PAYLOAD;
            r += param.SA;
            pool->Write(r, param.PAYLOAD, raw.data());
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random write test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * (param.IT) / (end_time - start_time);
        DLOG("%d clients total random write throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    if (1) {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < param.IT; ++i) {
            size_t r = rdd(0, i);
            r %= param.RANGE - param.PAYLOAD;
            r += param.SA;
            pool->Read(r, param.PAYLOAD, raw.data());
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random read test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * (param.IT) / (end_time - start_time);
        DLOG("%d clients total random read throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    if (0) {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < param.IT; ++i) {
            size_t r = i * param.PAYLOAD;
            r %= param.RANGE - param.PAYLOAD;
            r += param.SA;
            pool->Write(r, param.PAYLOAD, raw.data());
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: sequential write test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * (param.IT) / (end_time - start_time);
        DLOG("%d clients total sequential write throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    if (0) {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < param.IT; ++i) {
            size_t r = i * param.PAYLOAD;
            r %= param.RANGE - param.PAYLOAD;
            r += param.SA;
            pool->Read(r, param.PAYLOAD, raw.data());
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: sequential read test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * (param.IT) / (end_time - start_time);
        DLOG("%d clients total sequential read throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    if (0) {
        zipf_distribution zipf_distr(param.RANGE / param.PAYLOAD, param.ZIPF);
        mt19937_64 eng;

        vector<size_t> rv(param.IT);
        for (int i = 0; i < param.IT; ++i) {
            rv[i] = (zipf_distr(eng) - 1) * param.PAYLOAD;
        }

        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < param.IT; ++i) {
            size_t r = rv[i];
            r %= param.RANGE - param.PAYLOAD;
            r += param.SA;
            pool->Read(r, param.PAYLOAD, raw.data());
            uint64_t e = getTimestamp();
            ps.addValue(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: zipf read test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * (param.IT) / (end_time - start_time);
        DLOG("%d clients total zipf read throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
             ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    }

    if (0) {
        uint64_t start_time = getTimestamp(), end_time;
        uint64_t tv = start_time;

        for (size_t i = 0; i < param.IT; ++i) {
            uint64_t e;
            size_t r = rdd(0, i);
            if ((r % 100) < param.RA) {
                r %= param.RANGE - param.PAYLOAD;
                r += param.SA;
                pool->Read(r, param.PAYLOAD, raw.data());
                e = getTimestamp();
                hyr.addValue(e - tv);
            } else {
                r %= param.RANGE - param.PAYLOAD;
                r += param.SA;
                pool->Write(r, param.PAYLOAD, raw.data());
                e = getTimestamp();
                hyw.addValue(e - tv);
            }
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: random op test done. Use time: %ld us. Avg time: %f us", 0, diff,
             1.0 * diff / param.IT);

        double throughput = 1.0 * param.IT / (end_time - start_time);
        DLOG("read ratio: %d", param.RA);
        DLOG("%d clients total op throughput: %f Kops", 0, throughput * 1000);

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyr.getPercentile(50),
             hyr.getPercentile(99), hyr.getPercentile(99.9), hyr.getPercentile(99.99));

        DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyw.getPercentile(50),
             hyw.getPercentile(99), hyw.getPercentile(99.9), hyw.getPercentile(99.99));
    }
}
