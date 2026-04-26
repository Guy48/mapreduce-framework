#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <utility>
#include <vector>

class K1 {
public:
    virtual ~K1() = default;
    virtual bool operator<(const K1 &other) const = 0;
};

class V1 {
public:
    virtual ~V1() = default;
};

class K2 {
public:
    virtual ~K2() = default;
    virtual bool operator<(const K2 &other) const = 0;
};

class V2 {
public:
    virtual ~V2() = default;
};

class K3 {
public:
    virtual ~K3() = default;
    virtual bool operator<(const K3 &other) const = 0;
};

class V3 {
public:
    virtual ~V3() = default;
};

using InputPair = std::pair<K1 *, V1 *>;
using IntermediatePair = std::pair<K2 *, V2 *>;
using OutputPair = std::pair<K3 *, V3 *>;

using InputVec = std::vector<InputPair>;
using IntermediateVec = std::vector<IntermediatePair>;
using OutputVec = std::vector<OutputPair>;

class MapReduceClient {
public:
    virtual ~MapReduceClient() = default;

    virtual void map(const K1 *key, const V1 *value, void *context) const = 0;
    virtual void reduce(const IntermediateVec *pairs, void *context) const = 0;
};

#endif // MAPREDUCECLIENT_H
