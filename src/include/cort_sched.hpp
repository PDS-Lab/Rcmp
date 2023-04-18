#pragma once

#include <boost/coroutine2/all.hpp>
#include <boost/coroutine2/coroutine.hpp>
#include <functional>
#include <list>

using coro_t = boost::coroutines2::coroutine<void>;

namespace this_cort {

void yield();
void reset_resume_cond(std::function<bool()> fn);

}

struct CortScheduler;

struct CortTask {
    enum State : uint8_t {
        READY,
        SUSPEND,
        DONE,
    };

    CortTask(CortScheduler *sched);

    void reset_resume_cond(std::function<bool()> cond_fn);

    void resume();

    State state;
    CortScheduler *sched;
    coro_t::pull_type *pull;
    coro_t::push_type *push;
    std::function<void()> fn;
    std::function<bool()> resume_condition;
};

struct CortScheduler {
    CortScheduler(int prealloc_cort);

    void addTask(std::function<void()> fn);
    void runOnce();

    std::list<CortTask> active_cort_list;
    std::list<CortTask> wait_cort_list;

    void addCort();
    void resumeCort(std::list<CortTask>::iterator &it);
};