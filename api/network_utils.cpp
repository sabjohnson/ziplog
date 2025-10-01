#include "network_utils.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

namespace ziplog {
namespace api {

    /*
    -------------------------------------------------------------------------------------------
    Request and Response Pattern
    -------------------------------------------------------------------------------------------
    */

    // creates connector socket and attempt to send a message ('max_retries'x)
    bool NetworkUtils::send_message_to_address(const string& ip, int port, const Message& msg, int timeout_ms, int max_retries) {
        for (int attempt = 0; attempt < max_retries; attempt++) {
            // create connector socket (outgoing connection)
            int sockfd = create_connector_socket(timeout_ms);
            if (sockfd < 0) continue;

            // attempt to connect to address of recipient
            if (connect_to_address(sockfd, ip, port)) {
                bool success = send_message_and_wait_for_ack(sockfd, msg);
                close(sockfd);
                if (success) return true;
            } else {
                close(sockfd);
            }

        }
        return false;
    }

    // sends a message on a connect and wait for a response
    bool NetworkUtils::send_message_and_wait_for_ack(int connector_socket, const Message& msg) {
        if (!send_message(connector_socket, msg)) {
            return false;
        }

        Message ack_response;
        if (!recv_message(connector_socket, ack_response)) {
            return false;
        }

        return (ack_response.type == ACK && ack_response.seq_or_count == msg.seq_or_count);
    }

    // send request to zipper
    bool NetworkUtils::request_from_zipper(const string& ip, int port, const Message& msg, Message& resp, int timeout_ms, int max_retries) {
        for (int attempt = 0; attempt < max_retries; attempt++) {
            // create connector socket (outgoing connection)
            int sockfd = create_connector_socket(timeout_ms);
            if (sockfd < 0) continue;

            // attempt to connect to address of recipient
            if (connect_to_address(sockfd, ip, port)) {
                if (!send_message(sockfd, msg)) {
                    close(sockfd);
                    return false;
                }
                if (!recv_message(sockfd, resp)) {
                    close(sockfd);
                    return false;
                }
                close(sockfd);
                return true;
            } else {
                close(sockfd);
            }
        }
        return false;
    }

    /*
    -------------------------------------------------------------------------------------------
    Sending/Reading Bytes over a Connection
    -------------------------------------------------------------------------------------------
    */

    // serializes messages, send size header and bytes of struct over a socket
    bool NetworkUtils::send_message(int socket, const Message& msg) {
        // attempt to serialize message
        auto serialized = msg.serialize();
        if (serialized.empty()) {
            std::cerr << "Rejecting send of oversized message" << std::endl;
            return false;
        }

        // send size of serialized message
        uint16_t msg_len = htons(static_cast<uint16_t>(serialized.size()));
        if (!send_bytes(socket, &msg_len, 2)) {
            return false;
        }
        // send message
        return send_bytes(socket, serialized.data(), serialized.size());
    }

    // sending specified number of bytes from a pointer to a sockett
    bool NetworkUtils::send_bytes(int socket, const void* data, size_t len) {
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

    // reads size header and struct from a connection and deserializes into a message struct
    bool NetworkUtils::recv_message(int socket, Message& msg) {
        // read 2-byte length prefix
        uint16_t msg_len;
        if (!recv_bytes(socket, &msg_len, 2)) {
            return false;
        }

        msg_len = ntohs(msg_len);
        if (msg_len > MAX_MESSAGE_SIZE) {
            std::cerr << "Rejecting recv of oversized message" << std::endl;
            return false;
        }

        // read message data
        vector<uint8_t> buffer(msg_len);
        if (!recv_bytes(socket, buffer.data(), msg_len)) {
            return false;
        }

        // attempt to deserialize message
        auto opt_msg = Message::deserialize(buffer);
        if (!opt_msg) {
            std::cerr << "Failed to deserialize message" << std::endl;
            return false;
        }

        msg = *opt_msg;
        return true;
    }

    // reads specified number of bytes from a socket and writes to a pointer
    bool NetworkUtils::recv_bytes(int socket, void* data, size_t len) {
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

    /*
    -------------------------------------------------------------------------------------------
    Socket Creation and Configuration
    -------------------------------------------------------------------------------------------
    */

    // useful for clients in ziplog architecture
    int NetworkUtils::create_connector_socket(int timeout_ms) {
        // bind to socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);   // creates socket
        if (sockfd < 0) return -1;

        // set receive timeout
        if (timeout_ms > 0) {
            struct timeval timeout;
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_usec = (timeout_ms % 1000) * 1000;
            if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
                close(sockfd);
                return -1;
            }
        }

        return sockfd;
    }

    // for outgoing connections to an ip address
    bool NetworkUtils::connect_to_address(int connector_socket, const string& ip, int port) {
        // set up recipient server addr
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr);

        // return success (connect < 0 signifies failure)
        return connect(connector_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) >= 0;
    }

    int NetworkUtils::create_listening_socket(const string& ip, int port, bool reuse_addr) {
        // create socket server
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) return -1;

        if (reuse_addr) {
            // set sock option to allow re-use (https://pubs.opengroup.org/onlinepubs/009695099/functions/setsockopt.html)
            int enabled = 1;
            if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
                close(sockfd);
                return -1;
            }
        }

        // bind socket to address
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(sockfd);
            return -1;
        }

        // listen for connections (https://man7.org/linux/man-pages/man2/listen.2.html)
        if (listen(sockfd, 10) < 0) {
            close(sockfd);
            return -1;
        }

        return sockfd;
    }

}}