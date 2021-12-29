.PHONY: all clean format debug release duckdb_debug duckdb_release
all: release
GEN=ninja
clean:
	rm -rf build
	rm -rf duckdb/build

duckdb_debug:
	cd duckdb && \
	BUILD_TPCDS=1 BUILD_TPCH=1 make debug

duckdb_release:
	cd duckdb && \
	BUILD_TPCDS=1 BUILD_TPCH=1 make release

debug: duckdb_debug
	mkdir -p build/debug && \
	cd build/debug && \
	cmake  -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release: duckdb_release
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

test: debug
	./duckdb/build/debug/test/unittest --test-dir . "[postgres_scanner]"

format:
	clang-format --sort-includes=0 -style=file -i postgres_scanner.cpp
	clang-format --sort-includes=0 -style=file -i concurrency_test.cpp
	cmake-format -i CMakeLists.txt