# Compiler settings
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -g
INCLUDES = -I. -Iapi -Iinclude -Ithird_party

# Directories
OBJ_DIR = obj
JSON_HEADER = third_party/json.hpp

# Source files
API_SOURCES = api/config.cpp api/message.cpp api/network_utils.cpp
SRC_SOURCES = impl/zipper.cpp impl/proxy.cpp impl/server.cpp impl/subscriber.cpp impl/main.cpp

ALL_SOURCES = $(API_SOURCES) $(SRC_SOURCES)
OBJECTS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(ALL_SOURCES))
TEST_OBJECTS = $(filter-out $(OBJ_DIR)/impl/main.o, $(OBJECTS)) # https://www.gnu.org/software/make/manual/html_node/Text-Functions.html
TARGET = ziplog

COMMON_HEADERS = api/common.h api/types.h api/config.h api/message.h api/network_utils.h include/base_node.h

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
test_build: $(TEST_OBJECTS) | check_json
	$(CXX) $(CXXFLAGS) $(INCLUDES) tests/*.cpp $(TEST_OBJECTS) -o test_runner

# Build and run test target
test: $(TEST_OBJECTS) | check_json
	$(CXX) $(CXXFLAGS) $(INCLUDES) tests/*.cpp $(TEST_OBJECTS) -o test_runner
	./test_runner

.PHONY: clean external_clean test test_build check_json download_json