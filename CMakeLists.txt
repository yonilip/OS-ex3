cmake_minimum_required(VERSION 3.3)
project(OS_ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES MapReduceFrameWork.h MapReduceClient.h MapReduceFrameWork.cpp main.cpp)
add_executable(OS_ex3 ${SOURCE_FILES})