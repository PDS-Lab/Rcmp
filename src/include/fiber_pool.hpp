#pragma once

#include <boost/fiber/all.hpp>
#include <functional>
#include <queue>

class FiberPool {
   public:
    ~FiberPool() { EraseAllFiber(); }

    size_t size() const { return fibers_.size(); }

    void AddFiber(size_t n) {
        std::unique_lock<boost::fibers::mutex> lock(mutex_);
        for (std::size_t i = 0; i < n; ++i) {
            fibers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<boost::fibers::mutex> lock(mutex_);
                        cond_.wait(lock, [this] { return !tasks_.empty() || stop_; });
                        if (stop_ && tasks_.empty()) {
                            return;
                        }
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    void EraseAllFiber() {
        {
            std::unique_lock<boost::fibers::mutex> lock(mutex_);
            stop_ = true;
        }
        cond_.notify_all();
        for (auto& fiber : fibers_) {
            fiber.join();
        }
        fibers_.clear();
    }

    template <typename F>
    void EnqueueTask(F&& f) {
        {
            std::unique_lock<boost::fibers::mutex> lock(mutex_);
            tasks_.emplace(std::forward<F>(f));
        }
        cond_.notify_one();
    }

   private:
    std::vector<boost::fibers::fiber> fibers_;
    std::queue<std::function<void()>> tasks_;
    boost::fibers::mutex mutex_;
    boost::fibers::condition_variable cond_;
    volatile bool stop_ = false;
};
