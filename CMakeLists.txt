cmake_minimum_required(VERSION 3.23)
cmake_policy(SET CMP0091 NEW)
project(grpc_tests)

if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/0.18.1/conan.cmake"
        "${CMAKE_BINARY_DIR}/conan.cmake"
        TLS_VERIFY ON)
endif()

include(${CMAKE_BINARY_DIR}/conan.cmake)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " /MTd")
    string(APPEND CMAKE_CXX_FLAGS_RELWITHDEBINFO  " /MT")
endif()

if (CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_CONFIGURATION_TYPES Debug RelWithDebInfo)
    foreach(_build_type ${CMAKE_CONFIGURATION_TYPES})
        conan_cmake_autodetect(_settings BUILD_TYPE ${_build_type})

        conan_cmake_install(
            PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}
            BUILD missing
            SETTINGS ${_settings}
        )
    endforeach()

else()
        message("Generator is single config for ${CMAKE_BUILD_TYPE}")
        conan_cmake_autodetect(_settings BUILD_TYPE ${CMAKE_BUILD_TYPE})
        conan_cmake_install(
            PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}
            BUILD missing
            SETTINGS ${_settings}
        )
endif ()

include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

add_subdirectory(test)
