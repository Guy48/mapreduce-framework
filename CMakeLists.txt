cmake_minimum_required(VERSION 3.16)
project(MapReduceFramework LANGUAGES CXX)

option(BUILD_EXAMPLES "Build example applications" ON)
option(BUILD_TESTS "Build tests" ON)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(MapReduceFramework STATIC
    src/Barrier.cpp
    src/MapReduceFramework.cpp
)

target_include_directories(MapReduceFramework
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(MapReduceFramework
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
)

find_package(Threads REQUIRED)
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

if(BUILD_EXAMPLES)
    add_executable(word_count_demo examples/word_count_demo.cpp)
    target_link_libraries(word_count_demo PRIVATE MapReduceFramework)
endif()

if(BUILD_TESTS)
    include(CTest)
    enable_testing()

    add_executable(test_barrier tests/test_barrier.cpp)
    target_link_libraries(test_barrier PRIVATE MapReduceFramework)
    add_test(NAME barrier COMMAND test_barrier)

    add_executable(test_word_count tests/test_word_count.cpp)
    target_link_libraries(test_word_count PRIVATE MapReduceFramework)
    add_test(NAME word_count COMMAND test_word_count)

    add_executable(test_job_state tests/test_job_state.cpp)
    target_link_libraries(test_job_state PRIVATE MapReduceFramework)
    add_test(NAME job_state COMMAND test_job_state)
endif()
