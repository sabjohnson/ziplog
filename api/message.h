#pragma once
#include "types.h"
#include <cassert>

namespace ziplog {
namespace api {

    // Message type for all servers
    enum MessageTypes : uint32_t {
        APPEND,         // client to proxy, proxy to server OR server to subscriber
        SKIP,
        SUCCESS,
        FAILURE,
        ACK,            // subscriber to server OR server to proxy
        ZIP_REQUEST,    // proxy to zipper
        ZIP_RESPONSE,   // zipper to proxy
        REPORT,
        FREEZE,
        RECONFIGURATION,
    };

    struct Message {
        MessageType type;
        ShardId shard_id;
        NodeId sender_id;                           // index of address in config
        SequenceNumber seq_or_count;                // log index (APPEND) or num of slots (ZIP_REQUEST)
        Command data;                               // data being appended in log (APPEND). may be a batch
        vector<SequenceNumber> ordering_values;     // timestamps (ZIP_REQUEST) or sequence numbers (ZIP_RESPONSE)

        // Serialization methods
        vector<uint8_t> serialize() const;  // returns empty vector on failure (message is too large)
        static std::optional<Message> deserialize(const vector<uint8_t>& buffer);   // return std::nullopt on failure

        // Accessors to make intent clear (non-defensive assuming benign failures)
        SequenceNumber get_sequence_number() const {
            assert(type == APPEND || type == SKIP || type == ACK);
            return seq_or_count;
        }

        void set_sequence_number(SequenceNumber seq) {
            assert(type == APPEND || type == SKIP || type == ACK);
            seq_or_count = seq;
        }

        NodeId get_failed_proxy() const {
            if (type == REPORT) {
                return static_cast<NodeId>(seq_or_count);
            } else if (type == FREEZE) {
                return static_cast<NodeId>(ordering_values.front());
            }
            assert(false);
        }

        void set_failed_proxy(NodeId id) {
            if (type == REPORT) {
                seq_or_count = static_cast<SequenceNumber>(id);
            } else if (type == FREEZE) {
                ordering_values = {static_cast<SequenceNumber>(id)};
            } else {
                assert(false);
            }
        }

        void set_round(SequenceNumber round) {
            assert(type == FREEZE);
            seq_or_count = round;
        }

        SequenceNumber get_round() {
            assert(type == FREEZE);
            return seq_or_count;
        }

        SequenceNumber get_num_requests() const {
            assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
            return seq_or_count;
        }

        void set_num_requests(SequenceNumber count) {
            assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
            seq_or_count = count;
        }

        const vector<SequenceNumber>& get_assigned_sequences() const {
            assert(type == ZIP_RESPONSE);
            return ordering_values;
        }

        void set_assigned_sequences(const vector<SequenceNumber>& sequences) {
            assert(type == ZIP_RESPONSE);
            ordering_values = sequences;
        }
    };
}}