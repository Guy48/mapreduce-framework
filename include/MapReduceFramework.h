#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

using JobHandle = void *;

enum stage_t {
    UNDEFINED_STAGE = 0,
    MAP_STAGE = 1,
    SHUFFLE_STAGE = 2,
    REDUCE_STAGE = 3
};

struct JobState {
    stage_t stage;
    float percentage;
};

void emit2(K2 *key, V2 *value, void *context);
void emit3(K3 *key, V3 *value, void *context);

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel);

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState *state);
void closeJobHandle(JobHandle job);

#endif // MAPREDUCEFRAMEWORK_H
