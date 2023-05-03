#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>
#include <random>
#include <vector>
#include <atomic>

class Histogram {
   public:
    Histogram(int numBuckets, double minValue, double maxValue);
    ~Histogram() = default;

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
    ~FreqStats() = default;

    void add(uint64_t t);
    void clear();
    size_t freq() const;
    uint64_t start() const;
    uint64_t last() const;
    void dump(size_t &cnt, std::vector<uint64_t> &time_v);

   private:
    std::atomic<size_t> m_cnt;
    size_t m_max_recent_record;
    std::deque<uint64_t> m_time_q;
    uint64_t m_last_time;
    uint64_t m_start_time;
};

/**
 * @brief Generates random number according zipfian distribution.
 * It is defined as: P(X=k)= C / k^q, 1 <= k <= n
 */
class zipf_distribution {
public:
  zipf_distribution(uint32_t n, double q = 1.0) : n_(n), q_(q) {
    std::vector<double> pdf(n);
    for (uint32_t i = 0; i < n; ++i) {
      pdf[i] = std::pow((double)i + 1, -q);
    }
    dist_ = std::discrete_distribution<uint32_t>(pdf.begin(), pdf.end());
  }

  template <typename Generator> uint32_t operator()(Generator &g) {
    return dist_(g) + 1;
  }

  uint32_t min() { return 1; }
  uint32_t max() { return n_; }

private:
  uint32_t n_;
  double q_;
  std::discrete_distribution<uint32_t> dist_;
};