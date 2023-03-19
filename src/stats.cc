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

int Histogram::getPercentile(double percentile) const {
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

int Histogram::getBucket(double value) const { return (value - m_minValue) / m_bucketWidth; }