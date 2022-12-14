cmake_minimum_required(VERSION 3.23)
cmake_policy(SET CMP0091 NEW)
project(grpc_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

include(cmake-conan/conan.cmake)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
    get_property(isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
    if (isMultiConfig)
        message("Generator is multi config")
        conan_cmake_run(
            CONANFILE conanfile.txt
            BASIC_SETUP CMAKE_TARGETS
            BUILD missing
            SETTINGS compiler.runtime=MTd build_type=Debug
        )
#        conan_cmake_run(
#            CONANFILE conanfile.txt
#            BASIC_SETUP CMAKE_TARGETS
#            BUILD missing
#            SETTINGS compiler.runtime=MT build_type=RelWithDebInfo
#        )
    else()
        message("Generator is single config for ${CMAKE_BUILD_TYPE}")
        if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
            set(_conan_settings compiler.runtime=MTd)
        else ()
            set(_conan_settings compiler.runtime=MT)
        endif ()
        conan_cmake_run(
            CONANFILE conanfile.txt
            BASIC_SETUP CMAKE_TARGETS
            BUILD missing
            SETTINGS ${_conan_settings}
        )
    endif()
endif ()

include(${CMAKE_BINARY_DIR}/conan_paths.cmake)

add_subdirectory(test)
