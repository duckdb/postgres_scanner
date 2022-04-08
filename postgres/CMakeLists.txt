cmake_minimum_required(VERSION 2.8.12)

project(duckdb_libpq CXX C)
if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

add_definitions(-DFRONTEND=1 -D_GNU_SOURCE=1)

include_directories(include postgres/src/include postgres/src/backend)

if(WIN32)
  include_directories(postgres/src/include/port/win32 postgres/src/port
                      postgres/src/include/port/win32_msvc)
endif()

set(LIBPG_SOURCES
    postgres/src/common/base64.c
    postgres/src/common/cryptohash.c
    postgres/src/common/encnames.c
    postgres/src/common/hmac.c
    postgres/src/common/ip.c
    postgres/src/common/link-canary.c
    postgres/src/common/md5.c
    postgres/src/common/md5_common.c
    postgres/src/common/saslprep.c
    postgres/src/common/scram-common.c
    postgres/src/common/sha1.c
    postgres/src/common/sha2.c
    postgres/src/common/string.c
    postgres/src/common/unicode_norm.c
    postgres/src/common/wchar.c
    postgres/src/interfaces/libpq/fe-auth-scram.c
    postgres/src/interfaces/libpq/fe-auth.c
    postgres/src/interfaces/libpq/fe-connect.c
    postgres/src/interfaces/libpq/fe-exec.c
    postgres/src/interfaces/libpq/fe-lobj.c
    postgres/src/interfaces/libpq/fe-misc.c
    postgres/src/interfaces/libpq/fe-print.c
    postgres/src/interfaces/libpq/fe-protocol3.c
    postgres/src/interfaces/libpq/fe-secure.c
    postgres/src/interfaces/libpq/fe-trace.c
    postgres/src/interfaces/libpq/legacy-pqsignal.c
    postgres/src/interfaces/libpq/libpq-events.c
    postgres/src/interfaces/libpq/pqexpbuffer.c
    postgres/src/port/chklocale.c
    postgres/src/port/explicit_bzero.c
    postgres/src/port/inet_net_ntop.c
    postgres/src/port/noblock.c
    postgres/src/port/pg_strong_random.c
    postgres/src/port/pgstrcasecmp.c
    postgres/src/port/snprintf.c
    postgres/src/port/strerror.c
    postgres/src/port/thread.c)

if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux" OR WIN32)
  set(LIBPG_SOURCES ${LIBPG_SOURCES} postgres/src/port/strlcpy.c
                    postgres/src/port/getpeereid.c)
endif()

if(WIN32)
  set(LIBPG_SOURCES
      ${LIBPG_SOURCES}
      postgres/src/interfaces/libpq/pthread-win32.c
      postgres/src/interfaces/libpq/win32.c
      postgres/src/port/getaddrinfo.c
      postgres/src/port/gettimeofday.c
      postgres/src/port/inet_aton.c
      postgres/src/port/open.c
      postgres/src/port/pgsleep.c
      postgres/src/port/win32error.c
      postgres/src/port/win32setlocale.c
      postgres/src/port/win32stat.c)

endif()

function(PREPEND var prefix)
  set(listVar "")
  foreach(f ${ARGN})
    list(APPEND listVar "${prefix}/${f}")
  endforeach(f)
  set(${var}
      "${listVar}"
      PARENT_SCOPE)
endfunction(PREPEND)

prepend(LIBPG_SOURCES_FULLPATH ${CMAKE_CURRENT_SOURCE_DIR} ${LIBPG_SOURCES})

message(${CMAKE_SOURCE_DIR})
add_custom_command(
  OUTPUT ${LIBPG_SOURCES_FULLPATH}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/postgres
  COMMAND sh configure)

add_library(duckdb_libpq STATIC ${LIBPG_SOURCES_FULLPATH})

set_property(TARGET duckdb_libpq PROPERTY C_STANDARD 99)

set_target_properties(duckdb_libpq PROPERTIES POSITION_INDEPENDENT_CODE ON)
