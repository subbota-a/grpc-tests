cmake_minimum_required(VERSION 3.23)
project(grpc_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

include(cmake-conan/conan.cmake)
conan_cmake_run(
    CONANFILE conanfile.txt
    BASIC_SETUP CMAKE_TARGETS
    BUILD missing
)
include(${CMAKE_BINARY_DIR}/conan_paths.cmake)

add_subdirectory(test)
