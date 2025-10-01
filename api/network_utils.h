#pragma once
#include "message.h"

namespace ziplog {
namespace api {

    class NetworkUtils {
    public:
        /*
        -------------------------------------------------------------------------------------------
        Request and Response Pattern
        -------------------------------------------------------------------------------------------
        */

        /*
            @brief: Creates connector socket and calls sendMessageAndWaitForAck(msg) at most max_retries times.
            @param ip: ipv4 address of recipient
            @param port: port of recipient
            @param msg: message being sent to recipient
            @param timeout: timeout specification
            @param max_retries: max number of retries
            @return: true on communication success, false otherwise
        */
        static bool send_message_to_address(const string& ip, int port, const Message& msg, int timeout_ms, int max_retries);

        /*
            @brief: Send/receive once with timeout. Checks that the message/acknowledgment sequence number match
            @param socket: 'connector socket' file descriptor
            @param msg: message being sent on socket
            @return: true on success in sending/receiving to/from recipient, false otherwise
        */
        static bool send_message_and_wait_for_ack(int socket, const Message& msg);

        /*
            fill in later
        */
        static bool request_from_zipper(const string& ip, int port, const Message& msg, Message& resp, int timeout_ms, int max_retries);

        /*
        -------------------------------------------------------------------------------------------
        Sending/Reading Bytes over a Connection
        -------------------------------------------------------------------------------------------
        */

        /*
            @brief: Serializes message and writes it to a socket
            @param socket: file descriptor being written to
            @param msg: message to serialize and send
            @return: true on success in writing size header and serialized struct to file descriptor
        */
        static bool send_message(int socket, const Message& msg);

        /*
             @param: file descriptor to write to, bytes of serialized data, the size of serialized data
             @return: boolean of success of writing all bytes to file descriptor
        */
        static bool send_bytes(int socket, const void* data, size_t len);

        /*
             @param: file descriptor to read from, message struct that has been deserialized
             @return: boolean of success of reading size header and deserializing message struct from file descriptor
        */
        static bool recv_message(int socket, Message& msg);

        /*
             @param: file descriptor to read from, buffer to write the read bytes to, the number of bytes to read in
             @return: boolean of success of reading all bytes from file descriptor
        */
        static bool recv_bytes(int socket, void* data, size_t len);

        /*
        -------------------------------------------------------------------------------------------
        Socket Creation and Configuration
        -------------------------------------------------------------------------------------------
        */

        /*
            @param: number of milliseconds to wait to receive on a connection before timing out
            @return: file descriptor for socket for outgoing connections
        */
        static int create_connector_socket(int timeout_ms = 0);

        /*
            @param: ipv4 address, port and a flag marking if the socket should be re-usable
            @return: file descriptor for socket for incoming connections
        */
        static int create_listening_socket(const string& ip, int port, bool reuse_addr = true);

        /*
            @param: 'connector socket' file descriptor, ipv4 address and port of outgoing connection
            @return: boolean of success in connecting to recipient
        */
        static bool connect_to_address(int socket, const string& ip, int port);

    };
}}