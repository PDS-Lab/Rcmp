#pragma once

#include <unordered_set>
#include <vector>

#include "rchms.hpp"

using namespace std;

struct HashTableRep {
    template <typename D>
    static constexpr D div_ceil(D x, uint64_t div) {
        return (x + div - 1) / div;
    }

    static int rdd(int cli_id, int x) { return (((cli_id * 0xaf) ^ (0x45 * x)) ^ 0x89b31); }

    static constexpr size_t bkt_per_seg_num = 7;
    static constexpr size_t seg_num_second_level = 43;
    static constexpr size_t seg_num = 110001;
    static constexpr size_t max_bkt_num = bkt_per_seg_num * seg_num_second_level * seg_num;
    static constexpr int RETRY_CNT = 3;

    struct entry {
        bool valid = false;
        int key;
        int val;
    };

    struct segment {
        entry buckets[bkt_per_seg_num];
    };
    struct segment_table_2 {
        segment seg[seg_num_second_level];
    };
    struct segment_table {
        segment_table_2 seg2[seg_num];
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
                            (((segment_table *)0)->seg2[i].seg[j].buckets[k].valid),
                        sizeof(f), &f);
                }
            }
        }

        printf("create hashtable success. gaddr: %lu\n", st);
    }

    void put(int key, int val) {
        auto fn = [&](size_t sh, rchms::GAddr &entry_addr, entry &res) {
            segment seg;
            pool->Read(st + (rchms::GAddr) & (((segment_table *)0)
                                                  ->seg2[sh % seg_num]
                                                  .seg[sh % seg_num_second_level]),
                       sizeof(seg), &seg);

            size_t i = sh % bkt_per_seg_num;
            do {
                if (seg.buckets[i].valid && seg.buckets[i].key == key) {
                    res = seg.buckets[i];
                    entry_addr = st + (rchms::GAddr) & (((segment_table *)0)
                                                            ->seg2[sh % seg_num]
                                                            .seg[sh % seg_num_second_level]
                                                            .buckets[i]);
                    res.val = val;
                    return true;
                } else if (!seg.buckets[i].valid) {
                    res = seg.buckets[i];
                    entry_addr = st + (rchms::GAddr) & (((segment_table *)0)
                                                            ->seg2[sh % seg_num]
                                                            .seg[sh % seg_num_second_level]
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
                                                  ->seg2[sh % seg_num]
                                                  .seg[sh % seg_num_second_level]),
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