#pragma once
#include "message.h"
#include <cstdint>
#include <string>

using std::string;

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
            @input ip: ipv4 address of recipient
            @input port: port of recipient
            @input msg: message being sent to recipient
            @input timeout: timeout specification
            @input max_retries: max number of retries
            @return: true on communication success, false otherwise
        */
        static bool sendMessageToAddress(const string& ip, int port, const message& msg, int timeout_ms, int max_retries);

        /*
            @brief: Send/receive once with timeout. Checks that the message/acknowledgment sequence number match
            @input socket: 'connector socket' file descriptor
            @input msg: message being sent on socket
            @return: true on success in sending/receiving to/from recipient, false otherwise
        */
        static bool sendMessageAndWaitForAck(int socket, const message& msg);

        /*
            fill in later
        */
        static bool requestFromZipper(const string& ip, int port, const message& msg, message& resp, int timeout_ms, int max_retries);

        /*
        -------------------------------------------------------------------------------------------
        Sending/Reading Bytes over a Connection
        -------------------------------------------------------------------------------------------
        */

        /*
            @brief: Serializes message and writes it to a socket
            @input socket: file descriptor being written to
            @input msg: message to serialize and send
            @return: true on success in writing size header and serialized struct to file descriptor
        */
        static bool sendMessage(int socket, const message& msg);

        /*
         input: file descriptor to write to, bytes of serialized data, the size of serialized data
         return: boolean of success of writing all bytes to file descriptor
        */
        static bool sendBytes(int socket, const void* data, size_t len);

        /*
         input: file descriptor to read from, message struct that has been deserialized
         return: boolean of success of reading size header and deserializing message struct from file descriptor
        */
        static bool recvMessage(int socket, message& msg);

        /*
         input: file descriptor to read from, buffer to write the read bytes to, the number of bytes to read in
         return: boolean of success of reading all bytes from file descriptor
        */
        static bool recvBytes(int socket, void* data, size_t len);

        /*
        -------------------------------------------------------------------------------------------
        Socket Creation and Configuration
        -------------------------------------------------------------------------------------------
        */

        /*
        input: number of milliseconds to wait to receive on a connection before timing out
        return: file descriptor for socket for outgoing connections
        */
        static int createConnectorSocket(int timeout_ms = 0);

        /*
        input: ipv4 address, port and a flag marking if the socket should be re-usable
        return: file descriptor for socket for incoming connections
        */
        static int createListeningSocket(const string& ip, int port, bool reuse_addr = true);

        /*
        input: 'connector socket' file descriptor, ipv4 address and port of outgoing connection
        return: boolean of success in connecting to recipient
        */
        static bool connectToAddress(int socket, const string& ip, int port);

    };
}}