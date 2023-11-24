cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME postgres_scanner)
project(${TARGET_NAME})

add_definitions(-DFRONTEND=1 -D_GNU_SOURCE=1)
add_definitions(-DUSE_OPENSSL=1 -DHAVE_BIO_GET_DATA=1 -DHAVE_BIO_METH_NEW=1)
set(OPENSSL_USE_STATIC_LIBS TRUE)

find_package(OpenSSL REQUIRED)

if(EMSCRIPTEN)
  add_definitions(-DSTRERROR_R_INT=1)
endif()

if(NOT MSVC)
  set(POSTGRES_SCANNER_EXTRA_CFLAGS
      "-Wno-pedantic -Wno-sign-compare -Wno-unused-variable")
endif()

set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

set(CMAKE_C_FLAGS_DEBUG
    "${CMAKE_C_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS} ${POSTGRES_SCANNER_EXTRA_CFLAGS}"
)
set(CMAKE_C_FLAGS_RELEASE
    "${CMAKE_C_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

include_directories(include postgres/src/include postgres/src/backend
                    postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})

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
    postgres/src/interfaces/libpq/fe-secure-openssl.c
    postgres/src/interfaces/libpq/fe-secure-common.c
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
      postgres/src/port/system.c
      postgres/src/port/win32error.c
      postgres/src/port/win32ntdll.c
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

add_subdirectory(src)

prepend(LIBPG_SOURCES_FULLPATH ${CMAKE_CURRENT_SOURCE_DIR} ${LIBPG_SOURCES})

if(EXISTS "postgres")

else()
  execute_process(COMMAND sh pgconfigure ${LIBPG_SOURCES_FULLPATH}
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
add_custom_command(
  OUTPUT ${LIBPG_SOURCES_FULLPATH}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND sh pgconfigure)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES}
                         ${LIBPG_SOURCES_FULLPATH})

target_include_directories(
  ${TARGET_NAME}_loadable_extension
  PRIVATE include postgres/src/include postgres/src/backend
          postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})

if(WIN32)
  target_include_directories(
    ${TARGET_NAME}_loadable_extension
    PRIVATE postgres/src/include/port/win32 postgres/src/port
            postgres/src/include/port/win32_msvc)
endif()

target_link_libraries(${TARGET_NAME}_loadable_extension ${OPENSSL_LIBRARIES})
set_property(TARGET ${TARGET_NAME}_loadable_extension PROPERTY C_STANDARD 99)

if(WIN32)
  target_link_libraries(${TARGET_NAME}_loadable_extension wsock32 ws2_32
                        wldap32 secur32 crypt32)
endif()
