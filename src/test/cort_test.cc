#include <cstdio>

#include "cort_sched.hpp"
#include "log.hpp"

bool latch = false;

CortScheduler sched(4);

void event_loop() {
    static int loop_cnt = 0;
    getchar();
    if ((++loop_cnt) % 2 == 0) {
        // get event
        if (loop_cnt % 12 == 0) {
            sched.Spawn([]() {
                DLOG("unlatch");
                latch = false;
            });
        } else if (loop_cnt % 4 == 0) {
            sched.Spawn([]() {
                if (!latch) {
                    latch = true;
                } else {
                    this_cort::ResetResumeCond([]() { return !latch; });
                    this_cort::yield();
                    latch = true;
                }
                DLOG("latch");
            });
        } else if (loop_cnt % 2 == 0) {
            sched.Spawn([=]() { DLOG("%d", loop_cnt); });
        }
    }
}

int main() {
    while (1) {
        event_loop();
        sched.RunOnce();
    }
}