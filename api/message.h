#pragma once
#include <string>
#include <cstdint>
#include <vector>
#include <optional>

using std::string;
using std::vector;

namespace ziplog {
namespace api {

    const uint32_t MAX_MESSAGE_SIZE = 65535; // 2 ^ 16... 2 bytes read in for message header on tcp connection
    
    // enumerate client rssponse messages
    enum messageType {
        APPEND,        // client > server
        APPEND_ACK,    // server > client
        //READ,
        //READ_ACK,
        FORWARD_TO_SUB,        // server > subscriber
        FORWARD_TO_SUB_ACK,    // subscriber > server
    };

    struct message {
        // id of node sending this message
        int sender_id;
        // from messageType enum
        messageType type;
        // log entry number
        uint32_t sequence_number;
        // command
        string data;

        vector<uint8_t> serialize() const;  // returns empty vector on failure (message is too large)
        static std::optional<message> deserialize(const vector<uint8_t>& buffer);
    };
}}