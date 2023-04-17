#include "cort_sched.hpp"

#include <vector>

bool truly_cond_fn() { return true; }

static thread_local coro_t::push_type *t_sink = nullptr;

void this_cort::yield() { (*t_sink)(); }

CortTask::CortTask(std::function<void()> &&fn)
    : cort_source(std::move(fn)),
      resume_condition(truly_cond_fn),
      pinnable_idx(-1ul),
      cut_down(false),
      sink(nullptr) {}

void CortTask::reset_resume_cond(std::function<bool()> &&cond_fn) {
    resume_condition = std::move(cond_fn);
}

void CortTask::resume() {
    if (resume_condition()) {
        resume_condition = truly_cond_fn;
        t_sink = sink;
        cort_source();
        sink = t_sink;
    }
}

bool CortTask::valid() const { return !cort_source; }

bool CortTask::isPinnable() const { return pinnable_idx != -1ul; }

CortTask &CortTask::operator=(CortTask &&other) {
    cort_source = std::move(other.cort_source);
    std::swap(resume_condition, other.resume_condition);
    std::swap(pinnable_idx, other.pinnable_idx);
    std::swap(cut_down, other.cut_down);
    std::swap(sink, other.sink);

    return *this;
}

void CortScheduler::addPinnableTask(std::function<void()> &&fn) {
    pinnable_func_list.emplace_back(fn);
    addTask(std::move(fn));
}

void CortScheduler::addTask(std::function<void()> &&fn) {
    active_cort_list.emplace_back([fn](coro_t::push_type &sink) {
        t_sink = &sink;
        fn();
    });
}

void CortScheduler::run() {
    while (!active_cort_list.empty()) {
        for (auto it = active_cort_list.begin(); it != active_cort_list.end(); ++it) {
            auto &cort = *it;

            cort.resume();

            if (cort.isPinnable() && !cort.cut_down) {
                if (cort.valid()) {
                    // TODO: reuse list node
                    it = --active_cort_list.erase(it);
                    addTask(std::move(pinnable_func_list[cort.pinnable_idx]));
                } else {
                    addTask(std::move(pinnable_func_list[cort.pinnable_idx]));
                }
            } else if (cort.valid() && (cort.cut_down || (!cort.cut_down && !cort.isPinnable()))) {
                it = --active_cort_list.erase(it);
            }
        }
    }
}