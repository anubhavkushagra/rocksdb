cmake_minimum_required(VERSION 3.10)
project(KVStore CXX)
set(CMAKE_CXX_STANDARD 17)

# 1. Find Dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
find_package(Threads REQUIRED)
find_library(ROCKSDB_LIB rocksdb REQUIRED)
find_package(OpenSSL REQUIRED)

# 2. Proto Generation
find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

set(PROTO_SRC "${CMAKE_CURRENT_SOURCE_DIR}/proto/kv.proto")
set(GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GEN_DIR})

add_custom_command(
    OUTPUT "${GEN_DIR}/kv.pb.cc" "${GEN_DIR}/kv.pb.h" "${GEN_DIR}/kv.grpc.pb.cc" "${GEN_DIR}/kv.grpc.pb.h"
    COMMAND ${PROTOC_EXECUTABLE} --cpp_out=${GEN_DIR} --grpc_out=${GEN_DIR} --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE} -I${CMAKE_CURRENT_SOURCE_DIR}/proto ${PROTO_SRC}
    DEPENDS ${PROTO_SRC}
)

include_directories(${GEN_DIR} ${GRPC_INCLUDE_DIRS} ${PROTOBUF_INCLUDE_DIRS})
# Add the include folder we just created
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
# 3. Server
add_executable(kv_server 
    server/server.cpp 
    "${GEN_DIR}/kv.pb.cc" 
    "${GEN_DIR}/kv.grpc.pb.cc"
)
target_link_libraries(kv_server ${ROCKSDB_LIB} ${GRPC_LIBRARIES} ${PROTOBUF_LIBRARIES} Threads::Threads z bz2 snappy lz4 zstd)

# 4. Client (Pointing to your external directory)
add_executable(kv_client 
    kv_load_client/load_client.cpp 
    "${GEN_DIR}/kv.pb.cc" 
    "${GEN_DIR}/kv.grpc.pb.cc"
)
target_link_libraries(kv_client ${GRPC_LIBRARIES} ${PROTOBUF_LIBRARIES} Threads::Threads)

# 5. Conflict Test
add_executable(conflict_test
    kv_load_client/conflict_test.cpp
    "${GEN_DIR}/kv.pb.cc"
    "${GEN_DIR}/kv.grpc.pb.cc"
)
target_link_libraries(conflict_test ${GRPC_LIBRARIES} ${PROTOBUF_LIBRARIES} Threads::Threads)


# Update these two lines to include OpenSSL at the end
target_link_libraries(kv_server grpc++ grpc protobuf rocksdb OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(kv_client grpc++ grpc protobuf OpenSSL::SSL OpenSSL::Crypto)

# 6. Redis Server
add_executable(redis_server
    server/redis_server.cpp
    "${GEN_DIR}/kv.pb.cc"
    "${GEN_DIR}/kv.grpc.pb.cc"
)
target_link_libraries(redis_server ${GRPC_LIBRARIES} ${PROTOBUF_LIBRARIES} Threads::Threads hiredis)
target_link_libraries(redis_server grpc++ grpc protobuf OpenSSL::SSL OpenSSL::Crypto)