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
    priority_props(boost::fibers::context* ctx)
        : fiber_properties(ctx), /*< Your subclass constructor must accept a
                                  [^[class_link context]*] and pass it to
                                  the `fiber_properties` constructor. >*/
          priority_(0) {}

    int get_priority() const {
        return priority_; /*< Provide read access methods at your own discretion. >*/
    }

    // Call this method to alter priority, because we must notify
    // priority_scheduler of any change.
    void set_priority(int p) { /*<
           It's important to call `notify()` on any
           change in a property that can affect the
           scheduler's behavior. Therefore, such
           modifications should only be performed
           through an access method. >*/
        // Of course, it's only worth reshuffling the queue and all if we're
        // actually changing the priority.
        if (p != priority_) {
            priority_ = p;
            notify();
        }
    }

    void set_low_priority() { set_priority(1); }
    void set_high_priority() { set_priority(100); }

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

    rqueue_t rqueue_;
    std::mutex mtx_{};
    std::condition_variable cnd_{};
    bool flag_{false};

   public:
    priority_scheduler() : rqueue_() {}

    // For a subclass of algorithm_with_properties<>, it's important to
    // override the correct awakened() overload.
    /*<< You must override the [member_link algorithm_with_properties..awakened]
         method. This is how your scheduler receives notification of a
         fiber that has become ready to run. >>*/
    virtual void awakened(boost::fibers::context* ctx, priority_props& props) noexcept {
        int ctx_priority = props.get_priority(); /*< `props` is the instance of
                                                   priority_props associated
                                                   with the passed fiber `ctx`. >*/
        // With this scheduler, fibers with higher priority values are
        // preferred over fibers with lower priority values. But fibers with
        // equal priority values are processed in round-robin fashion. So when
        // we're handed a new context*, put it at the end of the fibers
        // with that same priority. In other words: search for the first fiber
        // in the queue with LOWER priority, and insert before that one.
        rqueue_t::iterator i(std::find_if(rqueue_.begin(), rqueue_.end(),
                                          [ctx_priority, this](boost::fibers::context& c) {
                                              return properties(&c).get_priority() < ctx_priority;
                                          }));
        // Now, whether or not we found a fiber with lower priority,
        // insert this new fiber here.
        rqueue_.insert(i, *ctx);
    }

    /*<< You must override the [member_link algorithm_with_properties..pick_next]
         method. This is how your scheduler actually advises the fiber manager
         of the next fiber to run. >>*/
    virtual boost::fibers::context* pick_next() noexcept {
        // if ready queue is empty, just tell caller
        if (rqueue_.empty()) {
            return nullptr;
        }
        boost::fibers::context* ctx(&rqueue_.front());
        rqueue_.pop_front();
        //<-
        //->
        return ctx;
    }

    /*<< You must override [member_link algorithm_with_properties..has_ready_fibers]
      to inform the fiber manager of the state of your ready queue. >>*/
    virtual bool has_ready_fibers() const noexcept { return !rqueue_.empty(); }

    /*<< Overriding [member_link algorithm_with_properties..property_change]
         is optional. This override handles the case in which the running
         fiber changes the priority of another ready fiber: a fiber already in
         our queue. In that case, move the updated fiber within the queue. >>*/
    virtual void property_change(boost::fibers::context* ctx, priority_props& props) noexcept {
        // Although our priority_props class defines multiple properties, only
        // one of them (priority) actually calls notify() when changed. The
        // point of a property_change() override is to reshuffle the ready
        // queue according to the updated priority value.

        // 'ctx' might not be in our queue at all, if caller is changing the
        // priority of (say) the running fiber. If it's not there, no need to
        // move it: we'll handle it next time it hits awakened().
        if (!ctx->ready_is_linked()) { /*<
          Your `property_change()` override must be able to
          handle the case in which the passed `ctx` is not in
          your ready queue. It might be running, or it might be
          blocked. >*/
                                       //<-
            // hopefully user will distinguish this case by noticing that
            // the fiber with which we were called does not appear in the
            // ready queue at all
            //->
            return;
        }

        // Found ctx: unlink it
        ctx->ready_unlink();

        // Here we know that ctx was in our ready queue, but we've unlinked
        // it. We happen to have a method that will (re-)add a context* to the
        // right place in the ready queue.
        awakened(ctx, props);
    }

    void suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept {
        if ((std::chrono::steady_clock::time_point::max)() == time_point) {
            std::unique_lock<std::mutex> lk(mtx_);
            cnd_.wait(lk, [this]() { return flag_; });
            flag_ = false;
        } else {
            std::unique_lock<std::mutex> lk(mtx_);
            cnd_.wait_until(lk, time_point, [this]() { return flag_; });
            flag_ = false;
        }
    }

    void notify() noexcept {
        std::unique_lock<std::mutex> lk(mtx_);
        flag_ = true;
        lk.unlock();
        cnd_.notify_all();
    }
};

class FiberPool {
   private:
    struct WorkerFiberTaskQueue {
        std::queue<std::function<void()>> fiber_tasks_;
        boost::fibers::mutex fiber_mutex_;
        boost::fibers::condition_variable fiber_cond_;
    };

   public:
    ~FiberPool() { EraseAll(); }

    size_t FiberSize() const { return fibers_.size(); }

    void AddFiber(size_t n) { AddFiber(fr_queue_, n); }

    void AddFiber(WorkerFiberTaskQueue& my_queue, size_t n) {
        std::unique_lock<boost::fibers::mutex> lock(my_queue.fiber_mutex_);
        for (std::size_t i = 0; i < n; ++i) {
            auto fiber = boost::fibers::fiber([this, &my_queue] {
                while (!fiber_stop_) {
                    std::function<void()> task;
                    {
                        std::unique_lock<boost::fibers::mutex> lock(my_queue.fiber_mutex_);
                        my_queue.fiber_cond_.wait(lock, [this, &my_queue] {
                            return !my_queue.fiber_tasks_.empty() || fiber_stop_;
                        });
                        if (fiber_stop_) return;
                        if (my_queue.fiber_tasks_.empty()) continue;
                        task = std::move(my_queue.fiber_tasks_.front());
                        my_queue.fiber_tasks_.pop();
                    }
                    task();
                }
            });
            fiber.properties<priority_props>().set_low_priority();
            fibers_.emplace_back(std::move(fiber));
        }
    }

    void EraseAll() {
        {
            {
                std::unique_lock<boost::fibers::mutex> lock(fr_queue_.fiber_mutex_);
                fiber_stop_ = true;
            }
            fr_queue_.fiber_cond_.notify_all();
        }

        for (auto& fiber : fibers_) {
            fiber.join();
        }
        fibers_.clear();
    }

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
