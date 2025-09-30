#pragma once
#include <string>
#include <cstdint>
#include <cassert>
#include <vector>
#include <optional>

using std::string;
using std::vector;

namespace ziplog {
namespace api {

    // Constants
    constexpr uint32_t MAX_MESSAGE_SIZE = 65535; // 2 ^ 16... 2 bytes read in for message header on tcp connection

    // Message type of all servers
    enum messageType : uint32_t {
        APPEND,         // proxy to server OR server to subscriber
        ACK,            // subscriber to server OR server to proxy
        ZIP_REQUEST,    // proxy to zipper
        ZIP_RESPONSE,   // zipper to proxy
    };

    struct message {
        messageType type;
        uint32_t shard_id;
        uint32_t sender_id;         // index of address in config

        uint64_t seq_or_count;      // log index or num of requests
        string data;                // represents commands

        vector<uint64_t> ordering_values;   // timestamps or sequence numbers (ZIP_REQ/RESP)

        vector<uint8_t> serialize() const;  // returns empty vector on failure (message is too large)
        static std::optional<message> deserialize(const vector<uint8_t>& buffer);   // return std::nullopt on failure

        // Accessors to make intent clear (non-defensive assuming benign failures)
        uint64_t get_sequence_number() const {
            assert(type == APPEND || type == ACK);
            return seq_or_count;
        }

        void set_sequence_number(uint64_t seq) {
            assert(type == APPEND || type == ACK);
            seq_or_count = seq;
        }

        uint64_t get_num_requests() const {
            assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
            return seq_or_count;
        }

        void set_num_requests(uint64_t count) {
            assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
            seq_or_count = count;
        }

        const vector<uint64_t>& get_timestamps() const {
            assert(type == ZIP_REQUEST);
            return ordering_values;
        }

        void set_timestamps(const vector<uint64_t>& timestamps) {
            assert(type == ZIP_REQUEST);
            ordering_values = timestamps;
        }

        const vector<uint64_t>& get_assigned_sequences() const {
            assert(type == ZIP_RESPONSE);
            return ordering_values;
        }

        void set_assigned_sequences(const vector<uint64_t>& sequences) {
            assert(type == ZIP_RESPONSE);
            ordering_values = sequences;
        }
    };

}}