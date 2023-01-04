cmake_minimum_required(VERSION 3.23)
cmake_policy(SET CMP0091 NEW)
project(grpc_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

include(cmake-conan/conan.cmake)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
    if ("${CMAKE_GENERATOR}" STREQUAL "Visual Studio 16 2019")
        message("Visual Studio generator")
        set(CMAKE_CXX_FLAGS_DEBUG "/MTd")
        set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "/MT")

        conan_cmake_autodetect(debug_settings BUILD_TYPE Debug)
        conan_cmake_autodetect(release_settings BUILD_TYPE RelWithDebInfo)

        conan_cmake_install(
            PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}
            BUILD missing
            SETTINGS ${debug_settings}
        )

        conan_cmake_install(
            PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}
            BUILD missing
            SETTINGS ${release_settings}
        )
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
