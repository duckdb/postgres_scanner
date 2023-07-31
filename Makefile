.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_BUILD_UNIVERSAL_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_EXTENSIONS="tpch;tpcds" ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${TOOLCHAIN_FLAGS}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=\
-DDUCKDB_EXTENSION_NAMES="postgres_scanner" \
-DDUCKDB_EXTENSION_POSTGRES_SCANNER_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_POSTGRES_SCANNER_SHOULD_LINK=0 \
-DDUCKDB_EXTENSION_POSTGRES_SCANNER_LOAD_TESTS=0
#-DDUCKDB_EXTENSION_POSTGRES_SCANNER_TEST_PATH="$(PROJ_DIR)test" \
#-DDUCKDB_EXTENSION_POSTGRES_SCANNER_INCLUDE_PATH="$(PROJ_DIR)src/include"

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb && make clean
	rm -rf postgres

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

# Main tests
test: test_release

test_release: release
	./build/release/test/unittest --test-dir . "[postgres_scanner]"

test_debug: debug
	./build/debug/test/unittest --test-dir . "[postgres_scanner]"

format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge
