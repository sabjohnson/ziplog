#include "network_utils.h"
#include "message.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <iostream>

namespace ziplog {
namespace api {

    bool NetworkUtils::sendBytes(int socket, const void* data, size_t len) {
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        size_t sent = 0;
        
        while (sent < len) {
            ssize_t result = send(socket, ptr + sent, len - sent, 0);
            if (result <= 0) {
                return false;
            }
            sent += result;
        }
        return true;
    }
    
    bool NetworkUtils::recvBytes(int socket, void* data, size_t len) {
        uint8_t* ptr = static_cast<uint8_t*>(data);
        size_t received = 0;
        
        while (received < len) {
            ssize_t result = recv(socket, ptr + received, len - received, 0);
            if (result <= 0) {
                return false;
            }
            received += result;
        }
        return true;
    }
    
    
    bool NetworkUtils::sendMessage(int socket, const message& msg) {
        // attempt to serialize message
        auto serialized = msg.serialize();
        if (serialized.empty()) {
            std::cerr << "Rejecting send of oversized message" << std::endl;
            return false;
        }
        
        // send size of serialized message
        uint16_t msg_len = htons(static_cast<uint16_t>(serialized.size()));
        if (!sendBytes(socket, &msg_len, 2)) {
            return false;
        }
        // send message
        return sendBytes(socket, serialized.data(), serialized.size());
    }
    
    bool NetworkUtils::recvMessage(int socket, message& msg) {
        // read 2-byte length prefix
        uint16_t msg_len;
        if (!recvBytes(socket, &msg_len, 2)) {
            return false;
        }
        
        msg_len = ntohs(msg_len);
        if (msg_len > MAX_MESSAGE_SIZE) {
            std::cerr << "Rejecting recv of oversized message" << std::endl;
            return false;
        }
        
        // Read message data
        std::vector<uint8_t> buffer(msg_len);
        if (!recvBytes(socket, buffer.data(), msg_len)) {
            return false;
        }
        
        // attempt to deserialize message
        auto opt_msg = message::deserialize(buffer);
        if (!opt_msg.has_value()) {
            std::cerr << "Failed to deserialize message" << std::endl;
            return false;
        }
        msg = opt_msg.value();
        return true;
    }
}}