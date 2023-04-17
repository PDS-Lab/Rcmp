#pragma once

#include <boost/coroutine2/all.hpp>
#include <boost/coroutine2/coroutine.hpp>
#include <functional>
#include <list>
#include <vector>

using coro_t = boost::coroutines2::coroutine<void>;

namespace this_cort {

void yield();

}

struct CortScheduler;

struct CortTask {
    CortTask(std::function<void()> &&fn);

    void reset_resume_cond(std::function<bool()> &&cond_fn);

    void resume();

    bool valid() const;
    bool isPinnable() const;

    CortTask &operator=(CortTask &&other);

    size_t pinnable_idx;
    bool cut_down;
    std::function<bool()> resume_condition;
    coro_t::pull_type cort_source;
    CortScheduler *sched;
    coro_t::push_type *sink;
};

struct CortScheduler {
    void addPinnableTask(std::function<void()> &&fn);
    void addTask(std::function<void()> &&fn);
    void run();

    std::vector<std::function<void()>> pinnable_func_list;
    std::list<CortTask> active_cort_list;
};