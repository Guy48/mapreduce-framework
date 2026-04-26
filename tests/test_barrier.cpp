#include "Barrier.h"

#include <atomic>
#include <cassert>
#include <pthread.h>
#include <vector>

struct BarrierTestContext {
    Barrier *barrier;
    std::atomic<int> *phase1;
    std::atomic<int> *phase2;
};

void *worker(void *arg)
{
    BarrierTestContext *ctx = static_cast<BarrierTestContext *>(arg);
    ctx->phase1->fetch_add(1, std::memory_order_relaxed);
    ctx->barrier->barrier();
    ctx->phase2->fetch_add(1, std::memory_order_relaxed);
    ctx->barrier->barrier();
    return nullptr;
}

int main()
{
    constexpr int threadCount = 6;
    Barrier barrier(threadCount);
    std::atomic<int> phase1{0};
    std::atomic<int> phase2{0};

    std::vector<pthread_t> threads(threadCount);
    std::vector<BarrierTestContext> contexts(threadCount);

    for (int i = 0; i < threadCount; ++i) {
        contexts[static_cast<std::size_t>(i)] = BarrierTestContext{&barrier, &phase1, &phase2};
        int rc = pthread_create(&threads[static_cast<std::size_t>(i)], nullptr, worker, &contexts[static_cast<std::size_t>(i)]);
        assert(rc == 0);
    }

    for (pthread_t &thread : threads) {
        pthread_join(thread, nullptr);
    }

    assert(phase1.load(std::memory_order_relaxed) == threadCount);
    assert(phase2.load(std::memory_order_relaxed) == threadCount);
    return 0;
}
