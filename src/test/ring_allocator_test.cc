#include <unistd.h>
#include <iostream>

#include "allocator.hpp"
#include "utils.hpp"

using namespace std;

int main() {
    RingAllocator<512> bb;

    vector<thread> vs;
    for (int i = 0; i < 8; i++) {
        vs.emplace_back([&, i]() {
            int a = 0;
            while (a < 2000000) {
                int s = (rand() % 10) + 4;
                int* p = nullptr;
                while (p == nullptr) {
                    p = (int*)bb.alloc(s);
                }
                *p = -1;
                // s = (rand() % 5) + 1;
                // usleep(s);
                bb.free(p);

                if ((++a) % 1000000 == 0) {
                    printf("%d %d\n", i, a);
                }
            }
        });
    }

    uint64_t _s = getTimestamp();

    for (auto& th : vs) {
        th.join();
    }

    cout << getTimestamp() - _s << endl;
    return 0;
}