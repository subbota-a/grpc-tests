cmake_minimum_required(VERSION 3.23)
cmake_policy(SET CMP0091 NEW)
project(grpc_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

include(cmake-conan/conan.cmake)

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(_conan_settings compiler.runtime=MTd)
else()
    set(_conan_settings compiler.runtime=MT)
endif()

# It requires conan 1.50 or later
conan_cmake_run(
    CONANFILE conanfile.txt
    BASIC_SETUP CMAKE_TARGETS
    BUILD missing
    SETTINGS ${_conan_settings}
)
include(${CMAKE_BINARY_DIR}/conan_paths.cmake)

add_subdirectory(test)
