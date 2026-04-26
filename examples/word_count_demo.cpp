#include "MapReduceFramework.h"

#include <cstdio>
#include <string>
#include <utility>
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

class WordCountClient : public MapReduceClient {
public:
    void map(const K1 *, const V1 *value, void *context) const override
    {
        const auto *text = static_cast<const TextValue *>(value);
        int counts[256] = {0};

        for (unsigned char c : text->text) {
            ++counts[c];
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0) {
                continue;
            }

            emit2(new CharKey(static_cast<char>(i)),
                  new CountValue(counts[i]),
                  context);
        }
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

        emit3(new CharKey(key), new CountValue(total), context);
    }
};

int main()
{
    WordCountClient client;
    InputVec input;
    OutputVec output;

    TextValue one("This is a small demo");
    TextValue two("MapReduce on Linux with pthreads");
    TextValue three("Clean project layout");

    input.push_back({nullptr, &one});
    input.push_back({nullptr, &two});
    input.push_back({nullptr, &three});

    JobHandle job = startMapReduceJob(client, input, output, 4);
    waitForJob(job);
    closeJobHandle(job);

    for (const OutputPair &pair : output) {
        const auto *key = static_cast<const CharKey *>(pair.first);
        const auto *value = static_cast<const CountValue *>(pair.second);
        std::printf("%c: %d\n", key->value, value->count);
        delete pair.first;
        delete pair.second;
    }

    return 0;
}
