//          Copyright Nat Goodspeed 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <algorithm>  // std::find_if()
#include <boost/fiber/all.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <queue>
#include <thread>

#include "log.hpp"

//[priority_props
class priority_props : public boost::fibers::fiber_properties {
   public:
    priority_props(boost::fibers::context* ctx);

    int get_priority() const;

    // Call this method to alter priority, because we must notify
    // priority_scheduler of any change.
    void set_priority(int p);

    void set_low_priority();
    void set_high_priority();

    // The fiber name of course is solely for purposes of this example
    // program; it has nothing to do with implementing scheduler priority.
    // This is a public data member -- not requiring set/get access methods --
    // because we need not inform the scheduler of any change.
    std::string name; /*< A property that does not affect the scheduler does
                          not need access methods. >*/
   private:
    int priority_;
};
//]

//[priority_scheduler
class priority_scheduler : public boost::fibers::algo::algorithm_with_properties<priority_props> {
   private:
    typedef boost::fibers::scheduler::ready_queue_type /*< See [link ready_queue_t]. >*/ rqueue_t;

    rqueue_t rqueue_high_;
    rqueue_t rqueue_low_;
    std::mutex mtx_{};
    std::condition_variable cnd_{};
    bool flag_{false};

   public:
    priority_scheduler();

    // For a subclass of algorithm_with_properties<>, it's important to
    // override the correct awakened() overload.
    /*<< You must override the [member_link algorithm_with_properties..awakened]
         method. This is how your scheduler receives notification of a
         fiber that has become ready to run. >>*/
    virtual void awakened(boost::fibers::context* ctx, priority_props& props) noexcept;

    /*<< You must override the [member_link algorithm_with_properties..pick_next]
         method. This is how your scheduler actually advises the fiber manager
         of the next fiber to run. >>*/
    virtual boost::fibers::context* pick_next() noexcept;

    /*<< You must override [member_link algorithm_with_properties..has_ready_fibers]
      to inform the fiber manager of the state of your ready queue. >>*/
    virtual bool has_ready_fibers() const noexcept;

    /*<< Overriding [member_link algorithm_with_properties..property_change]
         is optional. This override handles the case in which the running
         fiber changes the priority of another ready fiber: a fiber already in
         our queue. In that case, move the updated fiber within the queue. >>*/
    virtual void property_change(boost::fibers::context* ctx, priority_props& props) noexcept;

    void suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept;

    void notify() noexcept;
};

class FiberPool {
   private:
    struct WorkerFiberTaskQueue {
        std::queue<std::function<void()>> fiber_tasks_;
        boost::fibers::mutex fiber_mutex_;
        boost::fibers::condition_variable fiber_cond_;
    };

   public:
    ~FiberPool();

    size_t FiberSize() const;

    void AddFiber(size_t n);

    void AddFiber(WorkerFiberTaskQueue& my_queue, size_t n);

    void EraseAll();

    template <typename F>
    void EnqueueTask(F&& f) {
        {
            std::unique_lock<boost::fibers::mutex> lock(fr_queue_.fiber_mutex_);
            fr_queue_.fiber_tasks_.emplace(std::forward<F>(f));
        }
        fr_queue_.fiber_cond_.notify_one();
    }

   private:
    std::vector<boost::fibers::fiber> fibers_;
    volatile bool fiber_stop_ = false;
    volatile bool stop_ = false;
    WorkerFiberTaskQueue fr_queue_;
};
