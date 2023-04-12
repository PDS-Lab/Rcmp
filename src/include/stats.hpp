#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>
#include <vector>

class Histogram {
   public:
    Histogram(int numBuckets, double minValue, double maxValue);

    void addValue(double value);
    void clear();
    int getBucketCount() const;
    double getBucketValue(int bucket) const;
    int getBucketCount(int bucket) const;
    int getTotalCount() const;
    int getPercentile(double percentile) const;

   private:
    int getBucket(double value) const;

    const int m_numBuckets;
    const double m_minValue;
    const double m_maxValue;
    const double m_bucketWidth;
    std::vector<int> m_buckets;
};

class FreqStats {
   public:
    FreqStats(size_t max_recent_record);

    void add(uint64_t t);
    void clear();
    size_t freq() const;
    uint64_t start() const;
    uint64_t last() const;
    void dump(size_t &cnt, std::vector<uint64_t> &time_v);

   private:
    size_t m_cnt;
    size_t m_max_recent_record;
    std::deque<uint64_t> m_time_q;
    uint64_t m_last_time;
    uint64_t m_start_time;
};