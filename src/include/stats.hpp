#pragma once

#include <algorithm>
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