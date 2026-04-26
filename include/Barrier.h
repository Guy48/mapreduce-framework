#ifndef BARRIER_H
#define BARRIER_H

#include <pthread.h>

// Reusable pthread-based barrier.
class Barrier {
public:
    explicit Barrier(int participantCount);
    ~Barrier();

    Barrier(const Barrier &) = delete;
    Barrier &operator=(const Barrier &) = delete;

    void barrier();
    void setParticipantCount(int participantCount);

private:
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int participantCount;
    int generation;
};

#endif // BARRIER_H
