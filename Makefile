.PHONY: all clean format debug release duckdb_debug duckdb_release update
all: release
GEN=ninja

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

DUCKDB_ROOT_DIRECTORY=duckdb
ifneq ("${DUCKDB_ROOT}a", "a")
	DUCKDB_ROOT_DIRECTORY:=${DUCKDB_ROOT}
endif

clean:
	rm -rf build
	rm -rf duckdb/build
	rm -rf postgres/postgres

duckdb_debug:
	cd ${DUCKDB_ROOT_DIRECTORY} && \
	BUILD_TPCDS=1 BUILD_TPCH=1 make debug

duckdb_release:
	cd ${DUCKDB_ROOT_DIRECTORY} && \
	BUILD_TPCDS=1 BUILD_TPCH=1 make release

debug: duckdb_debug
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_INCLUDE_FOLDER=${DUCKDB_ROOT_DIRECTORY}/src/include -DDUCKDB_LIBRARY_FOLDER=${DUCKDB_ROOT_DIRECTORY}/build/debug/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
	cmake --build .

release: duckdb_release
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDUCKDB_INCLUDE_FOLDER=${DUCKDB_ROOT_DIRECTORY}/src/include -DDUCKDB_LIBRARY_FOLDER=${DUCKDB_ROOT_DIRECTORY}/build/release/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
	cmake --build .

test: release
	${DUCKDB_ROOT_DIRECTORY}/build/release/test/unittest --test-dir . "[postgres_scanner]"

format:
	cp ${DUCKDB_ROOT_DIRECTORY}/.clang-format .
	clang-format --sort-includes=0 -style=file -i postgres_scanner.cpp
	clang-format --sort-includes=0 -style=file -i concurrency_test.cpp
	cmake-format -i CMakeLists.txt
	cmake-format -i postgres/CMakeLists.txt
	rm .clang-format


update:
	git submodule update --remote --merge
