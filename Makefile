# Compiler settings
CXX = g++
#CXXFLAGS = -std=c++17 -Wall -Wextra -g
CXXFLAGS=-std=c++17 -Wall -Wextra -g -fsanitize=address -fno-omit-frame-pointer

INCLUDES = -I. -Iapi -Iinclude -Ithird_party

# Directories
OBJ_DIR = obj
JSON_HEADER = third_party/json.hpp

# Source files
API_SOURCES = api/config.cpp api/message.cpp api/network_utils.cpp
SRC_SOURCES = impl/zipper.cpp impl/client.cpp impl/proxy.cpp impl/server.cpp impl/subscriber.cpp impl/main.cpp

ALL_SOURCES = $(API_SOURCES) $(SRC_SOURCES)
OBJECTS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(ALL_SOURCES))
TEST_OBJECTS = $(filter-out $(OBJ_DIR)/impl/main.o, $(OBJECTS)) # https://www.gnu.org/software/make/manual/html_node/Text-Functions.html
TARGET = ziplog

COMMON_HEADERS = api/common.h api/types.h api/config.h api/message.h api/network_utils.h api/address.h api/node_config.h include/base_node.h

# Test information
TEST_LIBS = -lgtest -lgtest_main -lpthread
TEST_RUNNER = test_runner

# Build main executable
$(TARGET): $(OBJECTS) | check_json
	$(CXX) $(OBJECTS) -o $(TARGET)

# Compile source files - create directory structure in obj/
# (| = order-only prereq: ensure json.hpp exists but don't rebuild if check_json runs)
$(OBJ_DIR)/%.o: %.cpp $(COMMON_HEADERS) | check_json
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

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

# Clean - just remove obj directory and executable
clean:
	rm -rf $(OBJ_DIR) $(TARGET)

# Clean everything including downloaded dependencies
external_clean: clean
	rm -rf third_party/

# Build test target
# Your current (WRONG):
$(OBJ_DIR)/tests/%.o: tests/%.cpp $(COMMON_HEADERS) | check_json
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

TEST_SOURCES = $(wildcard tests/*.cpp)
TEST_OBJS = $(patsubst tests/%.cpp,$(OBJ_DIR)/tests/%.o,$(TEST_SOURCES))

test_build: $(TEST_OBJECTS) $(TEST_OBJS) | check_json
	$(CXX) $(CXXFLAGS) $(TEST_OBJS) $(TEST_OBJECTS) -o $(TEST_RUNNER) -lpthread -lgtest -lgtest_main

# Build and run test target
test: test_build
	./test_runner

.PHONY: clean external_clean test test_build check_json download_json