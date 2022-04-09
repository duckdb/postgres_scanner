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

set(TARGET_NAME postgres_scanner)

project(${TARGET_NAME} C CXX)

option(
  OSX_BUILD_UNIVERSAL
  "Build both architectures on OSX and create a single binary containing both."
  FALSE)
if(OSX_BUILD_UNIVERSAL)
  if(NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  set(CMAKE_OSX_ARCHITECTURES
      "x86_64;arm64"
      CACHE STRING "Build architectures for Mac OS X" FORCE)
endif()

find_package(Threads REQUIRED)
add_subdirectory(postgres)

# skip building extensions on mingw
if(WIN32 AND NOT MSVC)
  return()
endif()

include_directories(${DUCKDB_INCLUDE_FOLDER})

include_directories(postgres/postgres/src/include)
include_directories(postgres/postgres/src/interfaces/libpq)

if(WIN32)
  include_directories(
    postgres/postgres/src/include/port/win32 postgres/postgres/src/port
    postgres/postgres/src/include/port/win32_msvc)
endif()

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -pedantic -fsanitize=address -fno-sanitize=vptr)
endif()

link_directories(${DUCKDB_LIBRARY_FOLDER})

add_library(${TARGET_NAME} SHARED postgres_scanner.cpp)
set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")
target_link_libraries(${TARGET_NAME} duckdb_libpq Threads::Threads)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(${TARGET_NAME} -fsanitize=address)
endif()

set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS
                                                  "-undefined dynamic_lookup")
endif()

if(WIN32)
  target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
endif()

if(MSVC)
  set_target_properties(${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                                  ${CMAKE_BINARY_DIR})
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                              ${CMAKE_BINARY_DIR})
endif()

if(WIN32)
  target_link_libraries(${TARGET_NAME} wsock32 ws2_32 wldap32 secur32)

  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_RELEASE
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
