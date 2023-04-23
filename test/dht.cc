#include <unordered_set>
#include <vector>

#include "cmdline.h"
#include "log.hpp"
#include "rchms.hpp"
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

struct HashTableRep {
    struct entry {
        bool valid = false;
        int key;
        int val;
    };

    static constexpr size_t bkt_per_seg_num = 7;
    static constexpr size_t seg_num_second_level = 43;
    static constexpr size_t seg_num = 13103;
    static constexpr size_t max_bkt_num = bkt_per_seg_num * seg_num_second_level * seg_num;
    static constexpr int RETRY_CNT = 3;

    struct segment {
        entry buckets[bkt_per_seg_num];
    };
    struct segment_table_2 {
        // segment
        segment seg_oids[seg_num_second_level];
    };
    struct segment_table {
        // segment_table_2
        segment_table_2 seg_oids[seg_num];
    };

    // <segment_table>
    rchms::GAddr st;
    rchms::PoolContext *pool;

    void init() {
        bool f = false;
        st = pool->AllocPage(div_ceil(sizeof(segment_table), 2ul << 20));
        for (size_t i = 0; i < seg_num; ++i) {
            for (size_t j = 0; j < seg_num_second_level; ++j) {
                for (size_t k = 0; k < bkt_per_seg_num; ++k) {
                    pool->Write(
                        st + (rchms::GAddr) &
                            (((segment_table *)0)->seg_oids[i].seg_oids[j].buckets[k].valid),
                        sizeof(f), &f);
                }
            }
        }

        cout << "create hashtable success" << endl;
    }

    void put(int key, int val) {
        auto fn = [&](size_t sh, rchms::GAddr &entry_addr, entry &res) {
            segment seg;
            pool->Read(st + (rchms::GAddr) & (((segment_table *)0)
                                                  ->seg_oids[sh % seg_num]
                                                  .seg_oids[sh % seg_num_second_level]),
                       sizeof(seg), &seg);

            size_t i = sh % bkt_per_seg_num;
            do {
                if (seg.buckets[i].valid && seg.buckets[i].key == key) {
                    res = seg.buckets[i];
                    entry_addr = st + (rchms::GAddr) & (((segment_table *)0)
                                                            ->seg_oids[sh % seg_num]
                                                            .seg_oids[sh % seg_num_second_level]
                                                            .buckets[i]);
                    return true;
                } else if (!seg.buckets[i].valid) {
                    res = seg.buckets[i];
                    entry_addr = st + (rchms::GAddr) & (((segment_table *)0)
                                                            ->seg_oids[sh % seg_num]
                                                            .seg_oids[sh % seg_num_second_level]
                                                            .buckets[i]);
                    res.valid = true;
                    res.key = key;
                    res.val = val;
                    return true;
                }
                i = (i + 1) % bkt_per_seg_num;
            } while (i != sh % bkt_per_seg_num);

            return false;
        };

        size_t sh = key;
        size_t sh_end;
        unordered_set<size_t> find_shed;
        rchms::GAddr entry_addr;
        entry res_buf;
        // 重hash RETRY_CNT次
        for (int retry_hash = RETRY_CNT; retry_hash != 0; --retry_hash) {
            sh = rdd(0, sh);
            if (fn(sh, entry_addr, res_buf)) {
                goto commit;
            }
            find_shed.insert(sh);
        }

        // 线性探测segment
        sh_end = sh;
        for (sh = (sh + 1) % seg_num; sh != sh_end; sh = (sh + 1) % seg_num) {
            if (find_shed.find(sh) != find_shed.end()) {
                continue;
            }
            if (fn(sh, entry_addr, res_buf)) {
                goto commit;
            }
        }

    commit:
        pool->Write(entry_addr, sizeof(entry), &res_buf);
    }

    int get(int key) {
        int ret = -1;

        auto fn = [&](size_t sh) {
            segment seg;
            pool->Read(st + (rchms::GAddr) & (((segment_table *)0)
                                                  ->seg_oids[sh % seg_num]
                                                  .seg_oids[sh % seg_num_second_level]),
                       sizeof(seg), &seg);

            size_t i = sh % bkt_per_seg_num;
            do {
                if (seg.buckets[i].valid && seg.buckets[i].key == key) {
                    ret = seg.buckets[i].val;
                    return 0;
                } else if (!seg.buckets[i].valid) {
                    return 1;
                }
                i = (i + 1) % bkt_per_seg_num;
            } while (i != sh % bkt_per_seg_num);

            return 2;
        };

        size_t sh = key;
        size_t sh_end;
        unordered_set<size_t> find_shed;
        // 重hash RETRY_CNT次
        for (int retry_hash = RETRY_CNT; retry_hash != 0; --retry_hash) {
            sh = rdd(0, sh);
            if (fn(sh) != 2) {
                return ret;
            }
            find_shed.insert(sh);
        }

        // 线性探测segment
        sh_end = sh;
        for (sh = (sh + 1) % seg_num; sh != sh_end; sh = (sh + 1) % seg_num) {
            if (find_shed.find(sh) != find_shed.end()) {
                continue;
            }
            if (fn(sh) != 2) {
                return ret;
            }
        }

        return -1;
    }
};

struct PerfStatistics {
    std::vector<uint32_t> hist;
    uint64_t cnt;

    PerfStatistics() : hist(100000, 0), cnt(0) {}

    void add(int x) {
        ++cnt;
        if (x >= 100000) return;
        ++hist[x];
    }

    void percentile(int &p50, int &p99, int &p999, int &p9999) {
        uint64_t S = 0;
        bool f50 = false, f99 = false, f999 = false, f9999 = false;
        for (int i = 0; i < hist.size(); ++i) {
            S += hist[i];
            if (!f50 && S * 100 >= cnt * 50) {
                f50 = true;
                p50 = i;
            }
            if (!f99 && S * 100 >= cnt * 99) {
                f99 = true;
                p99 = i;
            }
            if (!f999 && S * 1000 >= cnt * 999) {
                f999 = true;
                p999 = i;
            }
            if (!f9999 && S * 10000 >= cnt * 9999) {
                f9999 = true;
                p9999 = i;
            }
        }
    }

    void clear() {
        hist.assign(100000, 0);
        cnt = 0;
    }
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

    __DEBUG_START_PERF();

    HashTableRep h;
    h.pool = pool;

    h.init();

    const size_t IT = cmd.get<size_t>("iteration");;
    const int RA = cmd.get<int>("read_ratio");

    PerfStatistics ps;
    PerfStatistics hyr, hyw;

    {
        uint64_t start_time = getTimestamp(), end_time;

        uint64_t tv = start_time;
        for (size_t i = 0; i < IT; ++i) {
            h.put(rdd(0, i), rdd(0, i));
            uint64_t e = getTimestamp();
            ps.add(e - tv);
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: %s random write test done. Use time: %ld us. Avg time: %f us\n", 0,
             "HashTable", diff, 1.0 * diff / IT);

        double throughput = 1.0 * (IT) / (end_time - start_time);
        cout << 0 << " clients total add throughput: " << throughput * 1000 << " Kops" << endl;

        {
            int p50, p99, p999, p9999;
            ps.percentile(p50, p99, p999, p9999);
            printf("p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", p50, p99, p999, p9999);
        }
    }

    {
        vector<int> rv;
        for (int i = 0; i < IT; i++) {
            rv.push_back(rdd(0, i));
        }
        shuffle(rv, 0);

        uint64_t start_time = getTimestamp(), end_time;
        uint64_t tv = start_time;

        for (size_t i = 0; i < IT; ++i) {
            uint64_t e;
            if ((i % 100) < RA) {
                size_t ret = h.get(rv[i]);
                DLOG_EXPR(ret, ==, rv[i]);
                e = getTimestamp();
                hyr.add(e - tv);
            } else {
                int r = rdd(0, i) * rdd(0, i);
                h.put(r, r);
                e = getTimestamp();
                hyw.add(e - tv);
            }
            tv = e;
        }

        end_time = getTimestamp();

        long diff = end_time - start_time;

        DLOG("cli %d: %s random op test done. Use time: %ld us. Avg time: %f us\n", 0, "HashTable",
             diff, 1.0 * diff / IT);

        double throughput = 1.0 * IT / (end_time - start_time);
        cout << "read ratio: " << RA << endl;
        cout << IT << " clients total op throughput: " << throughput * 1000 << " Kops" << endl;

        {
            int p50, p99, p999, p9999;
            hyr.percentile(p50, p99, p999, p9999);
            printf("read p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", p50, p99, p999, p9999);

            hyw.percentile(p50, p99, p999, p9999);
            printf("write p50: %dus, p99: %dus, p999: %dus, p9999: %dus\n", p50, p99, p999, p9999);
        }
    }

    return 0;
}