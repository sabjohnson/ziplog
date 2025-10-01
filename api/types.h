#pragma once
#include "common.h"

namespace ziplog {
namespace api {

    // type aliases
    using MessageType = uint32_t;
    using ShardId = uint32_t;
    using NodeId = uint32_t;
    using SequenceNumber = uint64_t;
    using Timestamp = uint64_t;
    using Command = string;

    /*
        @return: Timestamp representing current time.
    */
    inline Timestamp now_ms() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    /*
        @param tp: Time point to be converted to timestamp.
        @return: Timestamp representing current time.
    */
    inline Timestamp timestamp_to_ms(const system_clock::time_point& tp) {
        return duration_cast<milliseconds>(tp.time_since_epoch()).count();
    }

    // validation

    /*
        @brief: Takes node id and determines if it is in valid range for config.
        @param id: Id to be validated.
        @param max_size: Non-inclusive boundary for node id.
        @param node_type: Type of node being validated.
        @throws: std::invalid_argument if id is out of range.
    */
    template<typename T>
    void validate_node_id(T id, size_t max_size, const string& node_type) {
        if (id < 0 || static_cast<size_t>(id) >= max_size) {
            throw std::invalid_argument(node_type + " id " + std::to_string(id) + " out of range [0, " + std::to_string(max_size) + ")");
        }
    }
}}