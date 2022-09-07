find_package(gRPC REQUIRED)
find_package(GTest REQUIRED)
find_package(Protobuf REQUIRED)
find_package(magic_enum REQUIRED)

add_executable(grpc-tests
    client_stream.cpp
    ${gen_sources}
    ${gen_headers}
    test.proto
    main.cpp server_stream.cpp common.h common.cpp)
target_link_libraries(grpc-tests
    PUBLIC protobuf::libprotobuf gRPC::grpc++ GTest::gtest magic_enum::magic_enum
    )
set(_grpc_cpp_plugin $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
set(_generated_dir ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${_generated_dir})
target_include_directories(grpc-tests
    PRIVATE ${_generated_dir})

if (${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
    set_property(TARGET grpc-tests
        PROPERTY MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    target_compile_definitions(grpc-tests PUBLIC _WIN32_WINNT=0x0A00)
elseif(${CMAKE_CXX_COMPILER_ID} STREQUAL GNU)
    target_compile_options(grpc-tests PUBLIC -fsanitize=address -g)
    target_link_options(grpc-tests  PUBLIC -fsanitize=address)
else()
    message(WARNING "Unknown compiler ${CMAKE_CXX_COMPILER_ID}")
endif ()

protobuf_generate(
    TARGET grpc-tests
    LANGUAGE cpp
    PROTOC_OUT_DIR ${_generated_dir}
)

protobuf_generate(
    TARGET grpc-tests
    LANGUAGE grpc
    PLUGIN "protoc-gen-grpc=${_grpc_cpp_plugin}"
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PROTOC_OUT_DIR ${_generated_dir}
)
include(GoogleTest)
gtest_discover_tests(grpc-tests)
