#include "MapReduceFramework.h"

#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

class TextValue : public V1 {
public:
    explicit TextValue(std::string text)
        : text(std::move(text))
    {
    }

    std::string text;
};

class CharKey : public K2, public K3 {
public:
    explicit CharKey(char value)
        : value(value)
    {
    }

    bool operator<(const K2 &other) const override
    {
        return value < static_cast<const CharKey &>(other).value;
    }

    bool operator<(const K3 &other) const override
    {
        return value < static_cast<const CharKey &>(other).value;
    }

    char value;
};

class CountValue : public V2, public V3 {
public:
    explicit CountValue(int count)
        : count(count)
    {
    }

    int count;
};

class SlowClient : public MapReduceClient {
public:
    void map(const K1 *, const V1 *value, void *context) const override
    {
        const auto *text = static_cast<const TextValue *>(value);
        for (unsigned char c : text->text) {
            emit2(new CharKey(static_cast<char>(c)), new CountValue(1), context);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
    }

    void reduce(const IntermediateVec *pairs, void *context) const override
    {
        const char key = static_cast<const CharKey *>(pairs->at(0).first)->value;
        int total = 0;
        for (const IntermediatePair &pair : *pairs) {
            total += static_cast<const CountValue *>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        emit3(new CharKey(key), new CountValue(total), context);
    }
};

int main()
{
    SlowClient client;
    InputVec input;
    OutputVec output;

    TextValue a("aaaa");
    TextValue b("bbbb");
    TextValue c("cccc");
    TextValue d("dddd");

    input.push_back({nullptr, &a});
    input.push_back({nullptr, &b});
    input.push_back({nullptr, &c});
    input.push_back({nullptr, &d});

    JobHandle job = startMapReduceJob(client, input, output, 4);
    assert(job != nullptr);

    JobState state{};
    bool sawMap = false;
    bool sawReduce = false;

    for (;;) {
        getJobState(job, &state);
        if (state.stage == MAP_STAGE) {
            sawMap = true;
        } else if (state.stage == REDUCE_STAGE) {
            sawReduce = true;
            if (state.percentage >= 100.0f) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    waitForJob(job);
    closeJobHandle(job);

    assert(sawMap);
    assert(sawReduce);

    for (const OutputPair &pair : output) {
        delete pair.first;
        delete pair.second;
    }

    return 0;
}
