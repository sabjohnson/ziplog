#pragma once
#include "message.h"
#include <vector>
#include <cstdint>

namespace ziplog {
namespace api {

    class NetworkUtils {
    public:
        /*
         * input: file descriptor to write to, bytes of serialized data, the size of serialized data
         * return: boolean of success of writing all bytes to file descriptor
        */
        static bool sendBytes(int socket, const void* data, size_t len);
        /*
         * input: file descriptor to read from, buffer to write the read bytes to, the number of bytes to read in
         * return: boolean of success of reading all bytes from file descriptor
        */
        static bool recvBytes(int socket, void* data, size_t len);
        /*
         * input: file descriptor to write to, message struct to serialize
         * return: boolean of success of writing size header and serialized struct to file descriptor
        */
        static bool sendMessage(int socket, const message& msg);
        /*
         * input: file descriptor to read from, message struct that has been deserialized
         * return: boolean of success of reading size header and deserializing message struct from file descriptor
        */
        static bool recvMessage(int socket, message& msg);
    };

}}