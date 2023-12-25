//          Copyright Nat Goodspeed 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "fiber_pool.hpp"

//[priority_props
priority_props::priority_props(boost::fibers::context* ctx)
    : fiber_properties(ctx), /*< Your subclass constructor must accept a
                              [^[class_link context]*] and pass it to
                              the `fiber_properties` constructor. >*/
      priority_(1) {}

int priority_props::get_priority() const {
    return priority_; /*< Provide read access methods at your own discretion. >*/
}

// Call this method to alter priority, because we must notify
// priority_scheduler of any change.
void priority_props::set_priority(int p) { /*<
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

void priority_props::set_low_priority() { set_priority(1); }
void priority_props::set_high_priority() { set_priority(100); }
//]

//[priority_scheduler

priority_scheduler::priority_scheduler() : rqueue_high_(), rqueue_low_() {}

// For a subclass of algorithm_with_properties<>, it's important to
// override the correct awakened() overload.
/*<< You must override the [member_link algorithm_with_properties..awakened]
     method. This is how your scheduler receives notification of a
     fiber that has become ready to run. >>*/
void priority_scheduler::awakened(boost::fibers::context* ctx, priority_props& props) noexcept {
    int ctx_priority = props.get_priority(); /*< `props` is the instance of
                                               priority_props associated
                                               with the passed fiber `ctx`. >*/
    // With this scheduler, fibers with higher priority values are
    // preferred over fibers with lower priority values. But fibers with
    // equal priority values are processed in round-robin fashion. So when
    // we're handed a new context*, put it at the end of the fibers
    // with that same priority. In other words: search for the first fiber
    // in the queue with LOWER priority, and insert before that one.
    if (ctx_priority == 1) {
        rqueue_low_.push_back(*ctx);
    } else {
        rqueue_high_.push_back(*ctx);
    }
}

/*<< You must override the [member_link algorithm_with_properties..pick_next]
     method. This is how your scheduler actually advises the fiber manager
     of the next fiber to run. >>*/
boost::fibers::context* priority_scheduler::pick_next() noexcept {
    boost::fibers::context* ctx;
    if (!rqueue_high_.empty()) {
        ctx = &rqueue_high_.front();
        rqueue_high_.pop_front();
    } else if (!rqueue_low_.empty()) {
        ctx = &rqueue_low_.front();
        rqueue_low_.pop_front();
    } else {
        ctx = nullptr;
    }
    return ctx;
}

/*<< You must override [member_link algorithm_with_properties..has_ready_fibers]
  to inform the fiber manager of the state of your ready queue. >>*/
bool priority_scheduler::has_ready_fibers() const noexcept {
    return !rqueue_high_.empty() || !rqueue_low_.empty();
}

/*<< Overriding [member_link algorithm_with_properties..property_change]
     is optional. This override handles the case in which the running
     fiber changes the priority of another ready fiber: a fiber already in
     our queue. In that case, move the updated fiber within the queue. >>*/
void priority_scheduler::property_change(boost::fibers::context* ctx,
                                         priority_props& props) noexcept {
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

void priority_scheduler::suspend_until(
    std::chrono::steady_clock::time_point const& time_point) noexcept {
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

void priority_scheduler::notify() noexcept {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        flag_ = true;
    }
    cnd_.notify_all();
}

FiberPool::~FiberPool() { EraseAll(); }

size_t FiberPool::FiberSize() const { return fibers_.size(); }

void FiberPool::AddFiber(size_t n) { AddFiber(fr_queue_, n); }

void FiberPool::AddFiber(WorkerFiberTaskQueue& my_queue, size_t n) {
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

void FiberPool::EraseAll() {
    {
        std::unique_lock<boost::fibers::mutex> lock(fr_queue_.fiber_mutex_);
        fiber_stop_ = true;
    }
    fr_queue_.fiber_cond_.notify_all();

    for (auto& fiber : fibers_) {
        fiber.join();
    }
    fibers_.clear();
}
