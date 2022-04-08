cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0015)
  cmake_policy(SET CMP0015 NEW)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(CMAKE_VERSION VERSION_LESS "3.1")
  add_compile_options(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
endif()

project(postgres_scanner C CXX)

find_package(Threads REQUIRED)
add_subdirectory(postgres)

# skip building extensions on mingw
if(WIN32 AND NOT MSVC)
  return()
endif()

include_directories(duckdb/src/include)
include_directories(duckdb/third_party/fmt/include)

include_directories(postgres/postgres/src/include)
include_directories(postgres/postgres/src/interfaces/libpq)

if(WIN32)
  include_directories(
    postgres/postgres/src/include/port/win32 postgres/postgres/src/port
    postgres/postgres/src/include/port/win32_msvc)
endif()

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -pedantic -fsanitize=address -fno-sanitize=vptr)
  link_directories(duckdb/build/debug/src)
else()
  link_directories(duckdb/build/release/src)
endif()

add_library(postgres_scanner SHARED postgres_scanner.cpp)
set_target_properties(postgres_scanner PROPERTIES PREFIX "")
target_link_libraries(postgres_scanner duckdb duckdb_libpq Threads::Threads)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(postgres_scanner -fsanitize=address)
endif()

set_target_properties(postgres_scanner PROPERTIES SUFFIX ".duckdb_extension")

if(MSVC)
  set_target_properties(
    postgres_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                ${CMAKE_BINARY_DIR})
  set_target_properties(
    postgres_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                                ${CMAKE_BINARY_DIR})
endif()

if(WIN32)
  target_link_libraries(postgres_scanner wsock32 ws2_32 wldap32 secur32)

  set_target_properties(postgres_scanner PROPERTIES LINK_FLAGS_RELEASE
                                                    "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
endif()

#
# add_executable(concurrency_test concurrency_test.cpp)
# target_link_libraries(concurrency_test duckdb duckdb_libpq Threads::Threads)
#
# if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
# target_link_libraries(concurrency_test -fsanitize=address) endif()
