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
    using Command = vector<uint8_t>;

    inline Command string_to_command(const string& s) {
        return Command(s.begin(), s.end());
    }

    inline string command_to_string(const Command& cmd) {
        return string(cmd.begin(), cmd.end());
    }

    class CommandBatch {
    private:
        Command buffer_;

    public:
        void add_command(const Command& cmd) {
            uint32_t len = htonl(cmd.size());
            buffer_.insert(buffer_.end(), reinterpret_cast<const uint8_t*>(&len), reinterpret_cast<const uint8_t*>(&len) + 4);
            buffer_.insert(buffer_.end(), cmd.begin(), cmd.end());
        }

        Command serialize() const {
            return buffer_;
        }

        static vector<Command> deserialize(const Command& data) {
            vector<Command> commands;
            size_t offset = 0;

            while (offset < data.size()) {
                // read length
                if (offset + 4 > data.size()) break;

                uint32_t net_len;
                memcpy(&net_len, data.data() + offset, 4);
                uint32_t len = ntohl(net_len);
                offset += 4;

                if (offset + len > data.size()) break;

                // read command and append command to vector
                commands.emplace_back(data.begin() + offset, data.begin() + offset + len);
                offset += len;
            }

            return commands;
        }

        inline Command string_to_command(const string& s) {
            return Command(s.begin(), s.end());
        }
    };

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