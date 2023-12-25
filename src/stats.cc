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

FreqStats::Heatness FreqStats::add_wr(uint64_t t) {
    m_wr_heat = m_wr_heat + Heatness::one(t);
    return m_wr_heat;
}

FreqStats::Heatness FreqStats::add_rd(uint64_t t) {
    m_rd_heat = m_rd_heat + Heatness::one(t);
    return m_rd_heat;
}

void FreqStats::init_exp_decays(float half_life_us) {
    float m_lambda = 0.693147180559945309417232121458176568 /* ln 2*/ / half_life_us;

    for (int i = m_exp_decays.size(); i < half_life_us * 4; ++i) {
        m_exp_decays.push_back(exp(-m_lambda * i));
    }
}

void FreqStats::clear() {
    m_wr_heat.clear();
    m_wr_heat.clear();
}

FreqStats::Heatness FreqStats::Heatness::one(uint64_t t) {
    Heatness h;
    h.last_time = t;
    h.last_heat = 1;
    return h;
}

FreqStats::Heatness FreqStats::Heatness::heat(uint64_t t) const {
    Heatness h;
    h.last_time = t;
    uint64_t delta = t - last_time;
    if (t >= FreqStats::m_exp_decays.size()) {
        h.last_heat = last_heat * std::pow(FreqStats::m_exp_decays[1], delta);
    } else {
        h.last_heat = last_heat * FreqStats::m_exp_decays[delta];
    }
    return h;
}

void FreqStats::Heatness::clear() {
    last_time = 0;
    last_heat = 0;
}

FreqStats::Heatness FreqStats::Heatness::operator+(const FreqStats::Heatness &b) const {
    FreqStats::Heatness h;
    h.last_time = std::max(last_time, b.last_time);
    h.last_heat = heat(h.last_time).last_heat + b.heat(h.last_time).last_heat;
    return h;
}