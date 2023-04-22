#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

#include "concurrent_queue.hpp"

using namespace std;

using CQ =
    ConcurrentQueue<int, 4096, ConcurrentQueueProducerMode::MP, ConcurrentQueueConsumerMode::MC>;

int main() {
    vector<thread> vs;
    CQ q;

    const int PT = 8;
    const int CT = 8;
    const int IT = 1000000;

    for (int i = 0; i < PT; ++i) {
        vs.emplace_back([&q]() {
            for (int j = 0; j < IT; ++j) {
                while (!q.forceEnqueue(j))
                    ;
            }
        });
    }

    bool over = false;
    vector<uint64_t> sum(CT, 0);
    for (int i = 0; i < CT; ++i) {
        vs.emplace_back([i, &q, &over, &sum]() {
            int n;
            while (!over) {
                if (q.tryDequeue(&n)) sum[i] += n;
            }
            while (q.tryDequeue(&n)) {
                sum[i] += n;
            }
        });
    }

    uint64_t _s = getTimestamp();

    for (int i = 0; i < PT; ++i) {
        vs[i].join();
    }

    over = true;

    for (int i = PT; i < PT + CT; ++i) {
        vs[i].join();
    }

    uint64_t S = 0;
    for (auto n : sum) {
        S += n;
    }

    assert(S == 1ul * PT * (0 + IT - 1) * IT / 2);

    cout << getTimestamp() - _s << endl;

    return 0;
}