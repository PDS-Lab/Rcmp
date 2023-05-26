#include <cstdint>
#include <sw/redis++/redis++.h>
#include <random>
#include <string>
#include <thread>

#include "config.hpp"
#include "log.hpp"
#include "stats.hpp"
#include "utils.hpp"

using namespace std;
using namespace sw::redis;

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

inline void redis_sync(Redis &redis, string sync_key, int NID, int NODES) {
    std::string key = sync_key + to_string(NID);
    std::string value = "ok";

    // redis.set(key, value, 30s);
    // for (int i = 0; i < NODES; ++i) {
    //     std::string key = sync_key + to_string(i);
    //     auto val = redis.get(key);
    //     while (val) {
    //         val = redis.get(key);
    //         this_thread::sleep_for(100ms);
    //     }
    // }

    DLOG("%s sync done", sync_key.c_str());
}

struct PerfStatistics : public Histogram {
    PerfStatistics() : Histogram(1000000, 0, 1000000) {}
    PerfStatistics(Histogram &&h) : Histogram(h) {}
};

struct MemPoolBase {
    using GAddr = uintptr_t;
    constexpr static size_t alloc_unit = page_size;

    virtual GAddr Alloc(size_t s) = 0;
    virtual void Write(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void WriteBatch(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void Read(GAddr gaddr, size_t s, void *buf) = 0;
};

struct BenchParam {
    int NID;                // node id
    int NODES;              // node number
    size_t IT;              // iteration(per thread)
    int TH;                 // thread count
    int RA;                 // read ratio
    size_t PAYLOAD;         // payload size
    MemPoolBase::GAddr SA;  // start gaddr
    size_t RANGE;           // gaddr range [SA, SA+RANGE)
    size_t APC;             // alloc page count
    float ZIPF;             // zipf α
    vector<MemPoolBase *> instances;
};

inline void run_bench(BenchParam param) {
    DLOG("start runing ...");

    auto redis = Redis("tcp://192.168.1.52:6379");

    redis_sync(redis, "start", param.NID, param.NODES);

    MemPoolBase *pool = param.instances[0];
    if (param.APC) {
        MemPoolBase::GAddr ga = pool->Alloc(param.APC * (MemPoolBase::alloc_unit));
        DLOG_EXPR(ga, ==, param.SA);
    }

    redis_sync(redis, "alloc", param.NID, param.NODES);

    DLOG("start testing ...");

    vector<uint8_t> raw(param.PAYLOAD);

    if (0) {
        vector<thread> ths;
        vector<uint64_t> diff_times(param.TH, 0);
        vector<PerfStatistics> ps(param.TH);
        for (int tid = 0; tid < param.TH; ++tid) {
            ths.emplace_back([&, tid]() {
                MemPoolBase *pool = param.instances[tid];

                uint64_t start_time = getTimestamp(), end_time;

                uint64_t tv = start_time;
                for (size_t i = 0; i < param.IT; ++i) {
                    size_t r = rdd(tid, i) % param.RANGE;
                    r = align_floor(r, param.PAYLOAD);
                    r += param.SA;
                    pool->Write(r, param.PAYLOAD, raw.data());
                    uint64_t e = getTimestamp();
                    ps[tid].addValue(e - tv);
                    tv = e;
                }

                end_time = getTimestamp();

                long diff = end_time - start_time;

                diff_times[tid] = diff;

                DLOG("client %d tid %d: random write test done. Use time: %ld us. Avg time: %f us",
                     0, tid, diff, 1.0 * diff / param.IT);
            });
        }
        for (auto &th : ths) {
            th.join();
        }
        double total_throughput = 0;
        for (uint64_t diff : diff_times) {
            total_throughput += 1.0 * param.IT / diff;
        }
        DLOG("%d clients total random write throughput: %f Kops", 0, total_throughput * 1000);

        PerfStatistics all_ps = ps[0];
        for (int i = 1; i < ps.size(); ++i) {
            PerfStatistics ps_tmp = all_ps.merge(ps[i]);
            all_ps.~PerfStatistics();
            new (&all_ps) PerfStatistics(ps_tmp);
        }

        DLOG("AVG: %fus, P50: %dus, P99: %dus, P999: %dus, P9999: %dus\n", all_ps.getAverage(),
             all_ps.getPercentile(50), all_ps.getPercentile(99), all_ps.getPercentile(99.9),
             all_ps.getPercentile(99.99));

        redis_sync(redis, "rand_write", param.NID, param.NODES);
    }

    if (0) {
        vector<thread> ths;
        vector<uint64_t> diff_times(param.TH, 0);
        vector<PerfStatistics> ps(param.TH);
        for (int tid = 0; tid < param.TH; ++tid) {
            ths.emplace_back([&, tid]() {
                MemPoolBase *pool = param.instances[tid];

                uint64_t start_time = getTimestamp(), end_time;

                uint64_t tv = start_time;
                for (size_t i = 0; i < param.IT; ++i) {
                    size_t r = rdd(tid, i) % param.RANGE;
                    r = align_floor(r, param.PAYLOAD);
                    r += param.SA;
                    pool->Read(r, param.PAYLOAD, raw.data());
                    uint64_t e = getTimestamp();
                    ps[tid].addValue(e - tv);
                    tv = e;
                }

                end_time = getTimestamp();

                long diff = end_time - start_time;

                diff_times[tid] = diff;

                DLOG("client %d tid %d: random read test done. Use time: %ld us. Avg time: %f us",
                     0, tid, diff, 1.0 * diff / param.IT);
            });
        }
        for (auto &th : ths) {
            th.join();
        }
        double total_throughput = 0;
        for (uint64_t diff : diff_times) {
            total_throughput += 1.0 * param.IT / diff;
        }
        DLOG("%d clients total random read throughput: %f Kops", 0, total_throughput * 1000);

        PerfStatistics all_ps = ps[0];
        for (int i = 1; i < ps.size(); ++i) {
            PerfStatistics ps_tmp = all_ps.merge(ps[i]);
            all_ps.~PerfStatistics();
            new (&all_ps) PerfStatistics(ps_tmp);
        }

        DLOG("AVG: %fus, P50: %dus, P99: %dus, P999: %dus, P9999: %dus\n", all_ps.getAverage(),
             all_ps.getPercentile(50), all_ps.getPercentile(99), all_ps.getPercentile(99.9),
             all_ps.getPercentile(99.99));

        redis_sync(redis, "rand_read", param.NID, param.NODES);
    }

    // if (0) {
    //     uint64_t start_time = getTimestamp(), end_time;

    //     uint64_t tv = start_time;
    //     for (size_t i = 0; i < param.IT; ++i) {
    //         size_t r = i * param.PAYLOAD;
    //         r %= param.RANGE - param.PAYLOAD;
    //         r += param.SA;
    //         pool->Write(r, param.PAYLOAD, raw.data());
    //         uint64_t e = getTimestamp();
    //         ps.addValue(e - tv);
    //         tv = e;
    //     }

    //     end_time = getTimestamp();

    //     long diff = end_time - start_time;

    //     DLOG("cli %d: sequential write test done. Use time: %ld us. Avg time: %f us", 0, diff,
    //          1.0 * diff / param.IT);

    //     double throughput = 1.0 * (param.IT) / (end_time - start_time);
    //     DLOG("%d clients total sequential write throughput: %f Kops", 0, throughput * 1000);

    //     DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
    //          ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    // }

    // if (0) {
    //     uint64_t start_time = getTimestamp(), end_time;

    //     uint64_t tv = start_time;
    //     for (size_t i = 0; i < param.IT; ++i) {
    //         size_t r = i * param.PAYLOAD;
    //         r %= param.RANGE - param.PAYLOAD;
    //         r += param.SA;
    //         pool->Read(r, param.PAYLOAD, raw.data());
    //         uint64_t e = getTimestamp();
    //         ps.addValue(e - tv);
    //         tv = e;
    //     }

    //     end_time = getTimestamp();

    //     long diff = end_time - start_time;

    //     DLOG("cli %d: sequential read test done. Use time: %ld us. Avg time: %f us", 0, diff,
    //          1.0 * diff / param.IT);

    //     double throughput = 1.0 * (param.IT) / (end_time - start_time);
    //     DLOG("%d clients total sequential read throughput: %f Kops", 0, throughput * 1000);

    //     DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", ps.getPercentile(50),
    //          ps.getPercentile(99), ps.getPercentile(99.9), ps.getPercentile(99.99));
    // }

    if (1) {
        vector<thread> ths;
        vector<uint64_t> diff_times(param.TH, 0);
        vector<PerfStatistics> ps(param.TH);
        for (int tid = 0; tid < param.TH; ++tid) {
            ths.emplace_back([&, tid]() {
                MemPoolBase *pool = param.instances[tid];

                zipf_distribution<> zipf_distr(param.RANGE / param.PAYLOAD, param.ZIPF);
                mt19937_64 eng(tid);

                vector<size_t> rv(param.IT);
                for (int i = 0; i < param.IT; ++i) {
                    rv[i] = rdd(tid, zipf_distr(eng)) * param.PAYLOAD;
                }

                uint64_t start_time = getTimestamp(), end_time;

                uint64_t tv = start_time;
                for (size_t i = 0; i < param.IT; ++i) {
                    size_t r = rv[i] % param.RANGE;
                    r = align_floor(r, param.PAYLOAD);
                    r += param.SA;
                    pool->Write(r, param.PAYLOAD, raw.data());
                    uint64_t e = getTimestamp();
                    ps[tid].addValue(e - tv);
                    tv = e;
                }

                end_time = getTimestamp();

                long diff = end_time - start_time;

                diff_times[tid] = diff;

                DLOG("client %d tid %d: zipf write test done. Use time: %ld us. Avg time: %f us", 0,
                     tid, diff, 1.0 * diff / param.IT);
            });
        }
        for (auto &th : ths) {
            th.join();
        }
        double total_throughput = 0;
        for (uint64_t diff : diff_times) {
            total_throughput += 1.0 * param.IT / diff;
        }
        DLOG("%d clients total zipf write throughput: %f Kops", 0, total_throughput * 1000);

        PerfStatistics all_ps = ps[0];
        for (int i = 1; i < ps.size(); ++i) {
            PerfStatistics ps_tmp = all_ps.merge(ps[i]);
            all_ps.~PerfStatistics();
            new (&all_ps) PerfStatistics(ps_tmp);
        }

        DLOG("AVG: %fus, P50: %dus, P99: %dus, P999: %dus, P9999: %dus\n", all_ps.getAverage(),
             all_ps.getPercentile(50), all_ps.getPercentile(99), all_ps.getPercentile(99.9),
             all_ps.getPercentile(99.99));

        redis_sync(redis, "zipf_write", param.NID, param.NODES);
    }

    if (1) {
        vector<thread> ths;
        vector<uint64_t> diff_times(param.TH, 0);
        vector<PerfStatistics> ps(param.TH);
        for (int tid = 0; tid < param.TH; ++tid) {
            ths.emplace_back([&, tid]() {
                MemPoolBase *pool = param.instances[tid];

                zipf_distribution<> zipf_distr(param.RANGE / param.PAYLOAD, param.ZIPF);
                mt19937_64 eng(tid);

                vector<size_t> rv(param.IT);
                for (int i = 0; i < param.IT; ++i) {
                    rv[i] = rdd(tid, zipf_distr(eng)) * param.PAYLOAD;
                }

                uint64_t start_time = getTimestamp(), end_time;

                uint64_t tv = start_time;
                for (size_t i = 0; i < param.IT; ++i) {
                    size_t r = rv[i] % param.RANGE;
                    r = align_floor(r, param.PAYLOAD);
                    r += param.SA;
                    pool->Read(r, param.PAYLOAD, raw.data());
                    uint64_t e = getTimestamp();
                    ps[tid].addValue(e - tv);
                    tv = e;
                }

                end_time = getTimestamp();

                long diff = end_time - start_time;

                diff_times[tid] = diff;

                DLOG("client %d tid %d: zipf read test done. Use time: %ld us. Avg time: %f us", 0,
                     tid, diff, 1.0 * diff / param.IT);
            });
        }
        for (auto &th : ths) {
            th.join();
        }
        double total_throughput = 0;
        for (uint64_t diff : diff_times) {
            total_throughput += 1.0 * param.IT / diff;
        }
        DLOG("%d clients total zipf read throughput: %f Kops", 0, total_throughput * 1000);

        PerfStatistics all_ps = ps[0];
        for (int i = 1; i < ps.size(); ++i) {
            PerfStatistics ps_tmp = all_ps.merge(ps[i]);
            all_ps.~PerfStatistics();
            new (&all_ps) PerfStatistics(ps_tmp);
        }

        DLOG("AVG: %fus, P50: %dus, P99: %dus, P999: %dus, P9999: %dus\n", all_ps.getAverage(),
             all_ps.getPercentile(50), all_ps.getPercentile(99), all_ps.getPercentile(99.9),
             all_ps.getPercentile(99.99));

        redis_sync(redis, "zipf_read", param.NID, param.NODES);
    }

    // if (0) {
    //     uint64_t start_time = getTimestamp(), end_time;
    //     uint64_t tv = start_time;

    //     for (size_t i = 0; i < param.IT; ++i) {
    //         uint64_t e;
    //         size_t r = rdd(0, i);
    //         if ((r % 100) < param.RA) {
    //             r %= param.RANGE - param.PAYLOAD;
    //             r += param.SA;
    //             pool->Read(r, param.PAYLOAD, raw.data());
    //             e = getTimestamp();
    //             hyr.addValue(e - tv);
    //         } else {
    //             r %= param.RANGE - param.PAYLOAD;
    //             r += param.SA;
    //             pool->Write(r, param.PAYLOAD, raw.data());
    //             e = getTimestamp();
    //             hyw.addValue(e - tv);
    //         }
    //         tv = e;
    //     }

    //     end_time = getTimestamp();

    //     long diff = end_time - start_time;

    //     DLOG("cli %d: random op test done. Use time: %ld us. Avg time: %f us", 0, diff,
    //          1.0 * diff / param.IT);

    //     double throughput = 1.0 * param.IT / (end_time - start_time);
    //     DLOG("read ratio: %d", param.RA);
    //     DLOG("%d clients total op throughput: %f Kops", 0, throughput * 1000);

    //     DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyr.getPercentile(50),
    //          hyr.getPercentile(99), hyr.getPercentile(99.9), hyr.getPercentile(99.99));

    //     DLOG("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", hyw.getPercentile(50),
    //          hyw.getPercentile(99), hyw.getPercentile(99.9), hyw.getPercentile(99.99));
    // }

    redis_sync(redis, "test end", param.NID, param.NODES);
}
