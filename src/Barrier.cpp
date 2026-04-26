#include "Barrier.h"

#include <cstdio>
#include <cstdlib>

static void fail(const char *msg)
{
    std::fprintf(stderr, "%s\n", msg);
    std::exit(EXIT_FAILURE);
}

Barrier::Barrier(int participantCount)
    : count(0)
    , participantCount(participantCount)
    , generation(0)
{
    if (pthread_mutex_init(&mutex, nullptr) != 0) {
        fail("[Barrier] pthread_mutex_init failed");
    }
    if (pthread_cond_init(&cond, nullptr) != 0) {
        pthread_mutex_destroy(&mutex);
        fail("[Barrier] pthread_cond_init failed");
    }
}

Barrier::~Barrier()
{
    if (pthread_mutex_destroy(&mutex) != 0) {
        fail("[Barrier] pthread_mutex_destroy failed");
    }
    if (pthread_cond_destroy(&cond) != 0) {
        fail("[Barrier] pthread_cond_destroy failed");
    }
}

void Barrier::setParticipantCount(int newParticipantCount)
{
    if (pthread_mutex_lock(&mutex) != 0) {
        fail("[Barrier] pthread_mutex_lock failed");
    }

    participantCount = newParticipantCount;
    if (count >= participantCount) {
        count = 0;
        ++generation;
        if (pthread_cond_broadcast(&cond) != 0) {
            pthread_mutex_unlock(&mutex);
            fail("[Barrier] pthread_cond_broadcast failed");
        }
    }

    if (pthread_mutex_unlock(&mutex) != 0) {
        fail("[Barrier] pthread_mutex_unlock failed");
    }
}

void Barrier::barrier()
{
    if (pthread_mutex_lock(&mutex) != 0) {
        fail("[Barrier] pthread_mutex_lock failed");
    }

    const int myGeneration = generation;
    ++count;

    if (count == participantCount) {
        count = 0;
        ++generation;
        if (pthread_cond_broadcast(&cond) != 0) {
            pthread_mutex_unlock(&mutex);
            fail("[Barrier] pthread_cond_broadcast failed");
        }
    } else {
        while (myGeneration == generation) {
            if (pthread_cond_wait(&cond, &mutex) != 0) {
                pthread_mutex_unlock(&mutex);
                fail("[Barrier] pthread_cond_wait failed");
            }
        }
    }

    if (pthread_mutex_unlock(&mutex) != 0) {
        fail("[Barrier] pthread_mutex_unlock failed");
    }
}
