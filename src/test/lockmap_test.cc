#include "lockmap.hpp"
#include <cassert>
#include <iostream>
#include <shared_mutex>
#include <thread>
#include <vector>
#include "utils.hpp"

using namespace std;

LockResourceManager<int, SharedMutex> lr;

int main() {
    vector<int> a(100, 0);
    vector<int> b(100, 0);
    bool f = true;
    vector<thread> ths;
    for (int i = 0; i < 0; ++i) {
        ths.emplace_back([&](){
            for (int t = 0; t < 100000; ++t) {
                int r = rand() % 100;
                lr.lock(r);

                int a_ = a[r], b_ = b[r];
                a[r] = b_ + 1;
                b[r] = a_ + 1;

                lr.unlock(r);
            }
        });
    }
    for (int i = 0; i < 2; ++i) {
        ths.emplace_back([&, i](){
            int t = 0;

            while (f) {
                int r = rand() % 100;
                lr.lock_shared(r);

                assert(a[r] == b[r]);

                lr.unlock_shared(r);

                if ((++t % 10000) == 0) {
                    cout << "t " << i << " " << t << endl;
                }

                this_thread::yield();
            }
        });
    }

    for (int i = 0; i < 0; ++i) {
        ths[i].join();
    }

    sleep(1);
    f = false;

    for (int i = 0; i < 2; ++i) {
        ths[i].join();
    }

    return 0;
}