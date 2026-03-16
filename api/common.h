#pragma once

// libraries
#include <string>
#include <vector>
#include <set>
#include <unordered_map>

#include <thread>
#include <mutex>
#include <atomic>

#include <chrono>
#include <cstdint>
#include <utility>

#include <iostream>
#include <optional>

#include <arpa/inet.h> // htonl/ntohl
#include <cstring>     // memcpy
#include <unistd.h>

// commonly used types in implementation files
using std::cerr;
using std::cout;
using std::endl;
using std::pair;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;

// std items
using std::atomic;
using std::lock_guard;
using std::mutex;
using std::nullopt;
using std::optional;
using std::thread;

// chrono aliases
using namespace std::chrono;
using namespace std::chrono_literals; // for 100ms, 1s, etc.

namespace ziplog
{

    // constants
    static constexpr uint32_t MAX_MESSAGE_SIZE = 65535; // 2 ^ 16... 2 bytes read in for message header on tcp connection
    static constexpr uint64_t EPOCH_DURATION = 1000;    // keep this at a multiple of 10
    static constexpr uint64_t MAX_EPOCH_HISTORY = 10;
}