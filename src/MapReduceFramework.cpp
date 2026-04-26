#include "MapReduceFramework.h"
#include "Barrier.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <stdexcept>
#include <vector>

namespace {

struct Job;

struct ThreadContext {
    Job *job;
    int threadIndex;
    IntermediateVec *localPairs;
};

struct Job {
    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
    const int requestedThreads;

    std::atomic<stage_t> stage;
    std::atomic<std::size_t> nextInputIndex;
    std::atomic<std::size_t> mapCompleted;
    std::atomic<std::size_t> emittedPairs;
    std::atomic<std::size_t> shuffledPairs;
    std::atomic<std::size_t> nextReduceIndex;
    std::atomic<std::size_t> reduceCompleted;
    std::atomic<int> activeThreads;
    std::atomic<bool> joined;

    pthread_mutex_t taskMutex;
    pthread_mutex_t outputMutex;
    pthread_mutex_t stateMutex;

    Barrier barrier;
    std::vector<pthread_t> threads;
    std::vector<ThreadContext> contexts;
    std::vector<IntermediateVec> localIntermediates;
    std::vector<IntermediateVec> shuffledGroups;

    Job(const MapReduceClient &clientRef,
        const InputVec &inputRef,
        OutputVec &outputRef,
        int threadCount)
        : client(clientRef)
        , inputVec(inputRef)
        , outputVec(outputRef)
        , requestedThreads(threadCount)
        , stage(UNDEFINED_STAGE)
        , nextInputIndex(0)
        , mapCompleted(0)
        , emittedPairs(0)
        , shuffledPairs(0)
        , nextReduceIndex(0)
        , reduceCompleted(0)
        , activeThreads(threadCount)
        , joined(false)
        , barrier(threadCount)
        , threads(static_cast<std::size_t>(threadCount))
        , contexts(static_cast<std::size_t>(threadCount))
        , localIntermediates(static_cast<std::size_t>(threadCount))
    {
        if (pthread_mutex_init(&taskMutex, nullptr) != 0) {
            throw std::runtime_error("failed to initialize task mutex");
        }
        if (pthread_mutex_init(&outputMutex, nullptr) != 0) {
            pthread_mutex_destroy(&taskMutex);
            throw std::runtime_error("failed to initialize output mutex");
        }
        if (pthread_mutex_init(&stateMutex, nullptr) != 0) {
            pthread_mutex_destroy(&taskMutex);
            pthread_mutex_destroy(&outputMutex);
            throw std::runtime_error("failed to initialize state mutex");
        }

        for (int i = 0; i < threadCount; ++i) {
            contexts[static_cast<std::size_t>(i)] = ThreadContext{
                this,
                i,
                &localIntermediates[static_cast<std::size_t>(i)]
            };
        }
    }

    ~Job()
    {
        pthread_mutex_destroy(&taskMutex);
        pthread_mutex_destroy(&outputMutex);
        pthread_mutex_destroy(&stateMutex);
    }
};

void throwSystemError(const char *message)
{
    std::cerr << "system error: " << message << std::endl;
}

std::size_t getTotalForStage(const Job &job, stage_t stage)
{
    switch (stage) {
    case UNDEFINED_STAGE:
        return 1;
    case MAP_STAGE:
        return job.inputVec.size();
    case SHUFFLE_STAGE:
        return job.emittedPairs.load(std::memory_order_relaxed);
    case REDUCE_STAGE:
        return job.shuffledGroups.size();
    default:
        return 1;
    }
}

std::size_t getProcessedForStage(const Job &job, stage_t stage)
{
    switch (stage) {
    case UNDEFINED_STAGE:
        return 0;
    case MAP_STAGE:
        return job.mapCompleted.load(std::memory_order_relaxed);
    case SHUFFLE_STAGE:
        return job.shuffledPairs.load(std::memory_order_relaxed);
    case REDUCE_STAGE:
        return job.reduceCompleted.load(std::memory_order_relaxed);
    default:
        return 0;
    }
}

bool keysEqual(const K2 *lhs, const K2 *rhs)
{
    return !(*lhs < *rhs) && !(*rhs < *lhs);
}

ThreadContext *getLargestKeyContext(Job &job)
{
    ThreadContext *largestContext = nullptr;
    const K2 *largestKey = nullptr;

    for (std::size_t i = 0; i < job.localIntermediates.size(); ++i) {
        IntermediateVec &vec = job.localIntermediates[i];
        if (vec.empty()) {
            continue;
        }

        const K2 *candidateKey = vec.back().first;
        if (largestKey == nullptr || *largestKey < *candidateKey) {
            largestKey = candidateKey;
            largestContext = &job.contexts[i];
        }
    }

    return largestContext;
}

void shuffle(Job &job)
{
    while (job.shuffledPairs.load(std::memory_order_relaxed) <
           job.emittedPairs.load(std::memory_order_relaxed)) {
        ThreadContext *largestContext = getLargestKeyContext(job);
        if (largestContext == nullptr) {
            break;
        }

        IntermediateVec group;
        group.push_back(largestContext->localPairs->back());
        largestContext->localPairs->pop_back();
        job.shuffledPairs.fetch_add(1, std::memory_order_relaxed);

        const K2 *groupKey = group.back().first;

        for (std::size_t i = 0; i < job.localIntermediates.size(); ++i) {
            IntermediateVec &vec = job.localIntermediates[i];
            while (!vec.empty() && keysEqual(groupKey, vec.back().first)) {
                group.push_back(vec.back());
                vec.pop_back();
                job.shuffledPairs.fetch_add(1, std::memory_order_relaxed);
            }
        }

        job.shuffledGroups.push_back(std::move(group));
    }
}

void mapPhase(Job &job, ThreadContext &context)
{
    while (true) {
        std::size_t inputIndex = 0;

        if (pthread_mutex_lock(&job.taskMutex) != 0) {
            throw std::runtime_error("failed to lock task mutex");
        }

        if (job.nextInputIndex.load(std::memory_order_relaxed) < job.inputVec.size()) {
            inputIndex = job.nextInputIndex.fetch_add(1, std::memory_order_relaxed);
        } else {
            if (pthread_mutex_unlock(&job.taskMutex) != 0) {
                throw std::runtime_error("failed to unlock task mutex");
            }
            break;
        }

        if (pthread_mutex_unlock(&job.taskMutex) != 0) {
            throw std::runtime_error("failed to unlock task mutex");
        }

        const InputPair &input = job.inputVec[inputIndex];
        job.client.map(input.first, input.second, &context);
        job.mapCompleted.fetch_add(1, std::memory_order_relaxed);
    }
}

void reducePhase(Job &job, ThreadContext &context)
{
    while (true) {
        std::size_t groupIndex = 0;

        if (pthread_mutex_lock(&job.taskMutex) != 0) {
            throw std::runtime_error("failed to lock task mutex");
        }

        if (job.nextReduceIndex.load(std::memory_order_relaxed) < job.shuffledGroups.size()) {
            groupIndex = job.nextReduceIndex.fetch_add(1, std::memory_order_relaxed);
        } else {
            if (pthread_mutex_unlock(&job.taskMutex) != 0) {
                throw std::runtime_error("failed to unlock task mutex");
            }
            break;
        }

        if (pthread_mutex_unlock(&job.taskMutex) != 0) {
            throw std::runtime_error("failed to unlock task mutex");
        }

        job.client.reduce(&job.shuffledGroups[groupIndex], &context);
        job.reduceCompleted.fetch_add(1, std::memory_order_relaxed);
    }
}

void *threadEntry(void *arg)
{
    ThreadContext *context = static_cast<ThreadContext *>(arg);
    Job &job = *context->job;

    try {
        if (job.stage.load(std::memory_order_relaxed) != MAP_STAGE) {
            job.stage.store(MAP_STAGE, std::memory_order_relaxed);
        }

        mapPhase(job, *context);
        std::sort(context->localPairs->begin(), context->localPairs->end(),
                  [](const IntermediatePair &lhs, const IntermediatePair &rhs) {
                      return *(lhs.first) < *(rhs.first);
                  });

        job.barrier.barrier();

        if (context->threadIndex == 0) {
            job.stage.store(SHUFFLE_STAGE, std::memory_order_relaxed);
            shuffle(job);
            job.stage.store(REDUCE_STAGE, std::memory_order_relaxed);
        }

        job.barrier.barrier();

        reducePhase(job, *context);
    } catch (const std::exception &e) {
        throwSystemError(e.what());
        pthread_exit(nullptr);
    } catch (...) {
        throwSystemError("unexpected exception in worker thread");
        pthread_exit(nullptr);
    }

    return nullptr;
}

void destroyJob(Job *job)
{
    if (job == nullptr) {
        return;
    }
    delete job;
}

} // namespace

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *threadContext = static_cast<ThreadContext *>(context);
    threadContext->localPairs->push_back({key, value});
    threadContext->job->emittedPairs.fetch_add(1, std::memory_order_relaxed);
}

void emit3(K3 *key, V3 *value, void *context)
{
    ThreadContext *threadContext = static_cast<ThreadContext *>(context);

    if (pthread_mutex_lock(&threadContext->job->outputMutex) != 0) {
        throw std::runtime_error("failed to lock output mutex");
    }

    threadContext->job->outputVec.push_back({key, value});

    if (pthread_mutex_unlock(&threadContext->job->outputMutex) != 0) {
        throw std::runtime_error("failed to unlock output mutex");
    }
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    if (multiThreadLevel <= 0) {
        return nullptr;
    }

    Job *job = nullptr;
    try {
        job = new Job(client, inputVec, outputVec, multiThreadLevel);
    } catch (const std::exception &e) {
        throwSystemError(e.what());
        return nullptr;
    }

    int startedThreads = 0;
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(&job->threads[static_cast<std::size_t>(i)],
                           nullptr,
                           threadEntry,
                           &job->contexts[static_cast<std::size_t>(i)]) != 0) {
            job->activeThreads.store(startedThreads, std::memory_order_relaxed);
            job->barrier.setParticipantCount(startedThreads);

            for (int j = 0; j < startedThreads; ++j) {
                pthread_join(job->threads[static_cast<std::size_t>(j)], nullptr);
            }

            destroyJob(job);
            throwSystemError("failed to create worker thread");
            return nullptr;
        }
        ++startedThreads;
    }

    return static_cast<JobHandle>(job);
}

void waitForJob(JobHandle jobHandle)
{
    if (jobHandle == nullptr) {
        return;
    }

    Job *job = static_cast<Job *>(jobHandle);
    bool expected = false;
    if (!job->joined.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }

    const int threadCount = job->activeThreads.load(std::memory_order_relaxed);
    for (int i = 0; i < threadCount; ++i) {
        pthread_join(job->threads[static_cast<std::size_t>(i)], nullptr);
    }
}

void getJobState(JobHandle jobHandle, JobState *state)
{
    if (jobHandle == nullptr || state == nullptr) {
        return;
    }

    Job *job = static_cast<Job *>(jobHandle);

    if (pthread_mutex_lock(&job->stateMutex) != 0) {
        throwSystemError("failed to lock state mutex");
        return;
    }

    const stage_t stage = job->stage.load(std::memory_order_relaxed);
    const std::size_t total = getTotalForStage(*job, stage);
    const std::size_t processed = getProcessedForStage(*job, stage);

    state->stage = stage;
    state->percentage = (total == 0)
        ? 0.0f
        : static_cast<float>((processed * 100) / total);

    if (state->percentage > 100.0f) {
        state->percentage = 100.0f;
    }

    if (pthread_mutex_unlock(&job->stateMutex) != 0) {
        throwSystemError("failed to unlock state mutex");
    }
}

void closeJobHandle(JobHandle jobHandle)
{
    if (jobHandle == nullptr) {
        return;
    }

    waitForJob(jobHandle);
    destroyJob(static_cast<Job *>(jobHandle));
}
