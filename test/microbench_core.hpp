#include <pthread.h>
#include <sw/redis++/redis++.h>

#include <cstdint>
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

    redis.set(key, value, 30s);
    for (int i = 0; i < NODES; ++i) {
        std::string key = sync_key + to_string(i);
        auto val = redis.get(key);
        while (!val) {
            val = redis.get(key);
            this_thread::sleep_for(100ms);
        }
    }

    DLOG("%s sync done", sync_key.c_str());
}

inline void redis_del_sync_key(Redis &redis, string sync_key, int NID, int NODES) {
    std::string key = sync_key + to_string(NID);
    redis.del(key);
}

struct PerfStatistics : public Histogram {
    PerfStatistics() : Histogram(2e7, 0, 2e5) {}
    PerfStatistics(Histogram &&h) : Histogram(h) {}
};

struct MemPoolBase {
    using GAddr = uintptr_t;
    constexpr static size_t alloc_unit = page_size;

    virtual GAddr Alloc(size_t s) = 0;
    virtual void Write(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void WriteBatch(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void Read(GAddr gaddr, size_t s, void *buf) = 0;
    virtual void DumpStats() = 0;
};

struct BenchParam {
    int NID;                 // node id
    int NODES;               // node number
    size_t IT;               // iteration(per thread)
    int TH;                  // thread count
    size_t PAYLOAD;          // payload size
    MemPoolBase::GAddr SA;   // start gaddr
    size_t RANGE;            // gaddr range [SA, SA+RANGE)
    float ZIPF;              // zipf α
    string redis_server_ip;  // redis deamon server ip
    vector<MemPoolBase *> instances;
};

enum TestType : int {
    RAND = 1,
    ZIPF = 2,
    SEQ = 4,
    WRITE = 8,
    READ = 16,
};

inline void run_sample(const string &testname, const BenchParam &param, int type, Redis &redis) {
    vector<thread> ths;
    vector<uint64_t> diff_times(param.TH, 0);
    vector<PerfStatistics> ps(param.TH);
    pthread_barrier_t b;
    pthread_barrier_init(&b, nullptr, param.TH + 1);
    for (int tid = 0; tid < param.TH; ++tid) {
        ths.emplace_back([&, tid]() {
            MemPoolBase *pool = param.instances[tid];
            vector<uint8_t> raw(param.PAYLOAD);
            vector<size_t> rv(param.IT);

            if (type & TestType::RAND) {
                mt19937_64 eng(tid);
                for (int i = 0; i < param.IT; ++i) {
                    rv[i] = eng();
                }
            } else if (type & TestType::ZIPF) {
                zipf_distribution<> zipf_distr(param.RANGE / param.PAYLOAD, param.ZIPF);
                mt19937_64 eng(0x9cfa2331b);

                for (int i = 0; i < param.IT; ++i) {
                    mt19937_64 e(zipf_distr(eng));
                    rv[i] = e() * param.PAYLOAD;
                }
            } else if (type & TestType::SEQ) {
                mt19937_64 eng(tid);
                int S = eng();
                for (int i = 0; i < param.IT; ++i) {
                    rv[i] = (S + i) * param.PAYLOAD;
                }
            }

            for (auto &r : rv) {
                r %= param.RANGE;
                r = align_floor(r, param.PAYLOAD);
                r += param.SA;
            }

            pthread_barrier_wait(&b);

            uint64_t start_time = getNsTimestamp(), end_time;

            uint64_t tv = start_time;
            for (size_t i = 0; i < param.IT; ++i) {
                if (type & TestType::WRITE) {
                    pool->Write(rv[i], param.PAYLOAD, raw.data());
                } else if (type & TestType::READ) {
                    pool->Read(rv[i], param.PAYLOAD, raw.data());
                }
                uint64_t e = getNsTimestamp();
                ps[tid].addValue(e - tv);
                tv = e;
            }

            end_time = getNsTimestamp();

            long diff = end_time - start_time;

            diff_times[tid] = diff;

            DLOG("client %d tid %d: %s test done. Use time: %ld us. Avg time: %f us", param.NID,
                 tid, testname.c_str(), diff / 1000, 1.0 * diff / param.IT / 1e3);
        });
    }

    // redis sync is slow, init thread test context in waiting time.
    redis_sync(redis, testname, param.NID, param.NODES);

    pthread_barrier_wait(&b);
    for (auto &th : ths) {
        th.join();
    }
    pthread_barrier_destroy(&b);

    double total_throughput = 0;
    for (uint64_t diff : diff_times) {
        total_throughput += 1.0 * param.IT / diff;
    }
    DLOG("%d clients total %s throughput: %f Mops", param.NID, testname.c_str(),
         total_throughput * 1e3);

    PerfStatistics all_ps = ps[0];
    for (int i = 1; i < ps.size(); ++i) {
        PerfStatistics ps_tmp = all_ps.merge(ps[i]);
        all_ps.~PerfStatistics();
        new (&all_ps) PerfStatistics(ps_tmp);
    }

    DLOG("%d clients %s latnecy: AVG: %fus, P50: %fus, P99: %fus, P999: %fus, P9999: %fus\n",
         param.NID, testname.c_str(), all_ps.getAverage() / 1e3, all_ps.getPercentile(50) / 1e3,
         all_ps.getPercentile(99) / 1e3, all_ps.getPercentile(99.9) / 1e3,
         all_ps.getPercentile(99.99) / 1e3);

    MemPoolBase *pool = param.instances[0];
    pool->DumpStats();
}

inline void run_init(BenchParam param) {
    DLOG("start initing ...");
    MemPoolBase *pool = param.instances[0];
    if (param.NID == 0) {
        MemPoolBase::GAddr ga = pool->Alloc(param.RANGE);
        DLOG_EXPR(ga, ==, param.SA);
    }
    DLOG("initing end ...");
}

inline void run_bench(BenchParam param) {
    DLOG("start runing ...");

    auto redis = Redis("tcp://" + param.redis_server_ip);

    redis_sync(redis, "start", param.NID, param.NODES);

    redis_del_sync_key(redis, "test end", param.NID, param.NODES);

    DLOG("start testing ...");

    // run_sample("random write", param, TestType::WRITE | TestType::RAND, redis);
    // run_sample("random read", param, TestType::READ | TestType::RAND, redis);
    run_sample("zipf write", param, TestType::WRITE | TestType::ZIPF, redis);
    run_sample("zipf read", param, TestType::READ | TestType::ZIPF, redis);

    DLOG("testing end ...");

    redis_del_sync_key(redis, "start", param.NID, param.NODES);

    redis_sync(redis, "test end", param.NID, param.NODES);

    redis_del_sync_key(redis, "random write", param.NID, param.NODES);
    redis_del_sync_key(redis, "random read", param.NID, param.NODES);
    redis_del_sync_key(redis, "zipf write", param.NID, param.NODES);
    redis_del_sync_key(redis, "zipf read", param.NID, param.NODES);
}
