#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <random>
#include <vector>

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
    double getAverage() const;

    Histogram merge(Histogram &other);

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
    FreqStats(size_t max_recent_record, float lambda, uint64_t restart_interval);
    ~FreqStats() = default;

    size_t add(uint64_t t);
    void clear();
    size_t freq(uint64_t t) const;
    uint64_t start() const;
    uint64_t last() const;
    void dump(size_t &cnt, std::vector<uint64_t> &time_v);

   private:
    std::atomic<size_t> m_cnt;
    size_t m_max_recent_record;
    std::deque<uint64_t> m_time_q;
    uint64_t m_last_time;
    uint64_t m_start_time;
    uint64_t m_restart_interval;
    float m_lambda;
    std::vector<float> m_exp_decays;
};

/**
 * @brief Generates random number according zipfian distribution.
 * It is defined as: P(X=k)= C / k^q, 1 <= k <= n
 */
template <typename IntType = int>
class zipf_distribution {
   public:
    typedef IntType result_type;

    zipf_distribution(IntType max, double theta) : max_(max), theta_(theta), dist_(0.0, 1.0) {
        c_ = std::pow(max_, -theta_) / zeta(theta_, max_);
        q_ = std::pow(2.0, -theta_);
        h_ = harmonic(max_);
        v_ = dist_(gen_);
    }

    /**
     * @brief 返回zipf分布随机数[0, max)
     *
     * @tparam Generator
     * @param g
     * @return IntType
     */
    template <typename Generator>
    IntType operator()(Generator &g) {
        while (true) {
            double u = dist_(g) - 0.5;
            double y = std::floor(std::pow(max_ + 0.5, v_ - u) - 0.5);
            if (y < 1 || y > max_) continue;
            double k = std::floor(y);
            v_ = dist_(g);
            if (v_ >= q_ * std::pow(k + 1, theta_) / (h_ + k)) continue;
            return static_cast<IntType>(k) - 1;
        }
    }

   private:
    IntType max_;
    double theta_;
    double c_;
    double q_;
    double h_;
    double v_;
    std::mt19937 gen_;
    std::uniform_real_distribution<double> dist_;

    static double zeta(double theta, IntType n) {
        double sum = 0.0;
        for (IntType i = 1; i <= n; ++i) sum += std::pow(i, -theta);
        return sum;
    }

    double harmonic(IntType n) const { return c_ * zeta(theta_, n); }
};