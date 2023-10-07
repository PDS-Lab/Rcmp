#include <unistd.h>

#include <iostream>
#include <random>

#include "allocator.hpp"
#include "utils.hpp"

using namespace std;

int main() {
    RingArena<512, 4> bb;

    const size_t IT = 2000000;

    vector<thread> vs;
    for (int i = 0; i < 8; i++) {
        vs.emplace_back([&, i]() {
            size_t a = 0;
            mt19937 rng(i);
            while (a < IT) {
                int s = (rng() % 10) + 4;
                int* p = nullptr;
                while (p == nullptr) {
                    p = (int*)bb.allocate(s);
                }
                *p = -1;
                bb.deallocate(p, s);

                if ((++a) % 1000000 == 0) {
                    printf("%d %lu\n", i, a);
                }
            }
        });
    }

    uint64_t _s = getUsTimestamp();

    for (auto& th : vs) {
        th.join();
    }

    cout << 1.0 * vs.size() * IT / (getUsTimestamp() - _s) << " Mops" << endl;
    return 0;
}