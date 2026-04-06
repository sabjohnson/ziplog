# Compiler settings
CXX = g++
#CXXFLAGS=-std=c++17 -Wall -Wextra -g -fsanitize=address -fno-omit-frame-pointer
CXXFLAGS=-std=c++17 -Wall -Wextra -g

INCLUDES = -I. -Iapi -Iinclude -Iimpl -Ithird_party

# Directories
OBJ_DIR = obj
JSON_HEADER = third_party/json.hpp

# Source files
API_SOURCES = api/config.cpp api/message.cpp api/network_utils.cpp

SRC_SOURCES = \
	impl/main.cpp \
	impl/client/client.cpp \
	impl/proxy/proxy.cpp \
	impl/server/server.cpp \
	impl/subscriber/subscriber.cpp \
	impl/zipper/zipper.cpp \
	impl/zipper/reconfig_manager.cpp \
	impl/zipper/slot_allocator.cpp

ALL_SOURCES = $(API_SOURCES) $(SRC_SOURCES)
OBJECTS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(ALL_SOURCES))
TEST_OBJECTS = $(filter-out $(OBJ_DIR)/impl/main.o, $(OBJECTS)) # https://www.gnu.org/software/make/manual/html_node/Text-Functions.html
TARGET = ziplog

COMMON_HEADERS = \
	api/common.h \
	api/types.h \
	api/config.h \
	api/message.h \
	api/network_utils.h \
	api/address.h \
	api/node_config.h \
	include/base_node.h \
	include/circular_buffer.h \
	include/connection_pool.h \
	include/logger.h \

# Test information
GTEST_DIR = third_party/gtest_install
TEST_LIBS = $(GTEST_DIR)/lib/libgtest.a $(GTEST_DIR)/lib/libgtest_main.a -lpthread
INCLUDES = -I. -Iapi -Iinclude -Iimpl -Ithird_party -I$(GTEST_DIR)/include
TEST_RUNNER = test_runner

# Link flags - ADD -fsanitize=address here too
LDFLAGS = -fsanitize=address -lpthread

# Targets --------------------------------------------------

# Build main executable
$(TARGET): $(OBJECTS) | check_json
	$(CXX) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

# Compile source files - create directory structure in obj/
# (| = order-only prereq: ensure json.hpp exists but don't rebuild if check_json runs)
$(OBJ_DIR)/%.o: %.cpp $(COMMON_HEADERS) | check_json
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Tests ----------------------------------------------------
TEST_SOURCES = $(wildcard tests/*.cpp)
TEST_OBJS = $(patsubst tests/%.cpp,$(OBJ_DIR)/tests/%.o,$(TEST_SOURCES))

$(OBJ_DIR)/tests/%.o: tests/%.cpp $(COMMON_HEADERS) | check_json check_gtest
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test_build: $(TEST_OBJECTS) $(TEST_OBJS) | check_json check_gtest
	$(CXX) $(CXXFLAGS) $(TEST_OBJS) $(TEST_OBJECTS) -o $(TEST_RUNNER) $(TEST_LIBS)

# Build and run test target
test: test_build
	./$(TEST_RUNNER)

# Dependencies ---------------------------------------------

# Check if JSON header exists, download if not
check_json:
	@if [ ! -f $(JSON_HEADER) ]; then \
		echo "Downloading nlohmann/json single header..."; \
		mkdir -p third_party; \
		curl -L https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp -o $(JSON_HEADER); \
		echo "Downloaded $(JSON_HEADER)"; \
	fi

# Manual download target
download_json:
	mkdir -p third_party
	curl -L https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp -o $(JSON_HEADER)

check_gtest:
	@if [ ! -f $(GTEST_DIR)/lib/libgtest.a ]; then \
		echo "Building GoogleTest..."; \
		mkdir -p third_party/googletest && \
		cd third_party/googletest && \
		wget -q https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz && \
		tar -xzf v1.14.0.tar.gz && \
		cd googletest-1.14.0 && \
		cmake -DCMAKE_INSTALL_PREFIX=$(CURDIR)/third_party/gtest_install . && \
		make && \
		make install && \
		echo "GoogleTest installed."; \
	fi

# Clean ---------------------------------------------------------

# Just remove obj directory and executable
clean:
	rm -rf $(OBJ_DIR) $(TARGET) $(TEST_RUNNER)

# Clean everything including downloaded dependencies
external_clean: clean
	rm -rf third_party/

.PHONY: clean external_clean test test_build check_json download_json check_gtest
