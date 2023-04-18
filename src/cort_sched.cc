#include "cort_sched.hpp"

bool truly_cond_fn() { return true; }

static thread_local CortTask *current_cort = nullptr;

void this_cort::yield() { current_cort->push->operator()(); }

void this_cort::reset_resume_cond(std::function<bool()> fn) { current_cort->reset_resume_cond(fn); }

CortTask::CortTask(CortScheduler *sched)
    : pull(nullptr), resume_condition(truly_cond_fn), push(nullptr), sched(sched) {}

void CortTask::reset_resume_cond(std::function<bool()> cond_fn) { resume_condition = cond_fn; }

void CortTask::resume() {
    if (resume_condition()) {
        current_cort = this;
        resume_condition = truly_cond_fn;
        pull->operator()();
    }
}

void CortScheduler::addTask(std::function<void()> fn) {
    auto wait_it = wait_cort_list.begin();
    if (wait_it == wait_cort_list.end()) {
        addCort();
        wait_it = wait_cort_list.begin();
    }

    wait_it->fn = fn;

    active_cort_list.splice(active_cort_list.end(), wait_cort_list, wait_it);

    resumeCort(wait_it);
}

void CortScheduler::addCort() {
    wait_cort_list.emplace_back(this);
    auto &cort = wait_cort_list.back();
    cort.state = CortTask::READY;
    cort.pull = new coro_t::pull_type([&cort](coro_t::push_type &push) {
        push();
        cort.push = &push;
        while (true) {
            cort.state = CortTask::SUSPEND;
            cort.fn();
            cort.state = CortTask::DONE;
            cort.fn = nullptr;
            this_cort::yield();
        }
    });
}

CortScheduler::CortScheduler(int prealloc_cort) {
    for (int i = 0; i < prealloc_cort; ++i) {
        addCort();
    }
}

void CortScheduler::resumeCort(std::list<CortTask>::iterator &it) {
    auto &cort = *it;

    cort.resume();

    if (cort.state == CortTask::DONE) {
        cort.state = CortTask::READY;
        auto it_tmp = it;
        it_tmp--;
        wait_cort_list.splice(wait_cort_list.begin(), active_cort_list, it);
        it = it_tmp;
    }
}

void CortScheduler::runOnce() {
    for (auto it = active_cort_list.begin(); it != active_cort_list.end(); ++it) {
        resumeCort(it);
    }
}