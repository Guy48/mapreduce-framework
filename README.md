# MapReduce Framework

A small MapReduce framework implemented in C++ with pthreads on Linux.

## Features

- pthread-based worker execution
- reusable barrier implementation
- map / shuffle / reduce pipeline
- progress reporting through `getJobState`
- static library build via CMake
- example application and test scenarios

## Project layout

```text
include/   Public headers
src/       Library implementation
examples/  Small demo application
tests/     Test executables
```

## Build

```bash
cmake -S . -B build
cmake --build build
```

## Run tests

```bash
ctest --test-dir build --output-on-failure
```

## Example

```bash
./build/word_count_demo
```

## API

The public API is exposed through `include/MapReduceFramework.h` and `include/MapReduceClient.h`.

## Notes

The project is intentionally Linux-oriented and uses `pthread` primitives directly.
