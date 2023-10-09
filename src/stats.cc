#include "stats.hpp"

#include <algorithm>
#include <vector>

Histogram::Histogram(int numBuckets, double minValue, double maxValue)
    : m_numBuckets(numBuckets),
      m_minValue(minValue),
      m_maxValue(maxValue),
      m_bucketWidth((maxValue - minValue) / numBuckets),
      m_buckets(numBuckets, 0) {}

void Histogram::addValue(double value) {
    if (value < m_minValue || value > m_maxValue) {
        return;
    }

    int bucket = getBucket(value);
    ++m_buckets[bucket];
}

void Histogram::clear() { std::fill(m_buckets.begin(), m_buckets.end(), 0); }

int Histogram::getBucketCount() const { return m_numBuckets; }

double Histogram::getBucketValue(int bucket) const {
    if (bucket < 0 || bucket >= m_numBuckets) {
        // Invalid bucket index
        return -1;
    }

    return m_minValue + m_bucketWidth * bucket;
}

int Histogram::getBucketCount(int bucket) const {
    if (bucket < 0 || bucket >= m_numBuckets) {
        // Invalid bucket index
        return -1;
    }

    return m_buckets[bucket];
}

int Histogram::getTotalCount() const {
    int totalCount = 0;
    for (int count : m_buckets) {
        totalCount += count;
    }
    return totalCount;
}

double Histogram::getPercentile(double percentile) const {
    return getBucketValue(getPercentileBucket(percentile));
}

int Histogram::getPercentileBucket(double percentile) const {
    if (percentile < 0 || percentile > 100) {
        // Invalid percentile value
        return -1;
    }

    int totalCount = getTotalCount();
    int countSoFar = 0;
    for (int i = 0; i < m_numBuckets; ++i) {
        countSoFar += m_buckets[i];
        if (countSoFar / (double)totalCount * 100 >= percentile) {
            return i;
        }
    }

    // Should not reach here
    return -1;
}

double Histogram::getAverage() const {
    double S = 0;
    for (int i = 0; i < m_numBuckets; ++i) {
        S += getBucketValue(i) * getBucketCount(i);
    }
    return S / getTotalCount();
}

int Histogram::getBucket(double value) const { return (value - m_minValue) / m_bucketWidth; }

Histogram Histogram::merge(Histogram &other) {
    Histogram nh(std::max(m_numBuckets, other.m_numBuckets), std::min(m_minValue, other.m_maxValue),
                 std::max(m_maxValue, other.m_maxValue));
    for (int i = 0; i < m_numBuckets; ++i) {
        nh.m_buckets[nh.getBucket(getBucketValue(i))] += getBucketCount(i);
    }
    for (int i = 0; i < other.m_numBuckets; ++i) {
        nh.m_buckets[nh.getBucket(other.getBucketValue(i))] += other.getBucketCount(i);
    }
    return nh;
}

Mutex FreqStats::m_exp_decays_lck;
std::vector<float> FreqStats::m_exp_decays;

FreqStats::FreqStats(size_t max_recent_record, float lambda, uint64_t restart_interval)
    : m_max_recent_record(max_recent_record),
      m_restart_interval(restart_interval),
      m_cnt(0),
      m_lambda(lambda) {
    std::unique_lock<Mutex> lck(m_exp_decays_lck);
    for (int i = m_exp_decays.size(); i < m_restart_interval; ++i) {
        m_exp_decays.push_back(exp(-m_lambda * i));
    }
}

size_t FreqStats::add(uint64_t t) {
    size_t pre;
    uint64_t delta_t = t - m_last_time;
    if (delta_t >= m_restart_interval) {
        pre = 1;
        // m_cnt.store(1, std::memory_order::memory_order_relaxed);
        m_cnt++;
        m_start_time = t;
    } else {
        // pre =
        //     m_cnt.fetch_add(1, std::memory_order::memory_order_relaxed) * m_exp_decays[delta_t] +
        //     1;
        pre = m_cnt * m_exp_decays[delta_t] + 1;
        m_cnt++;
    }
    m_last_time = t;
    return pre;
}

void FreqStats::clear() {
    // m_cnt.store(0, std::memory_order::memory_order_relaxed);
    m_cnt = 0;
    m_time_q.clear();
}

size_t FreqStats::freq(uint64_t t) const {
    uint64_t delta_t = t - m_last_time;
    if (delta_t >= m_restart_interval) {
        return 0;
    } else {
        // return m_cnt.load(std::memory_order::memory_order_relaxed) * m_exp_decays[delta_t];
        return m_cnt * m_exp_decays[delta_t];
    }
}
uint64_t FreqStats::start() const { return m_start_time; }
uint64_t FreqStats::last() const { return m_last_time; }

void FreqStats::dump(size_t &cnt, std::vector<uint64_t> &time_v) {
    // cnt = m_cnt.load(std::memory_order::memory_order_relaxed);
    cnt = m_cnt;
    time_v.assign(m_time_q.begin(), m_time_q.end());
}