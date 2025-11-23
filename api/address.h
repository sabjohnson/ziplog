#pragma once
#include <string>
#include <functional>

using std::string;

namespace ziplog {
namespace api {

    struct Address;
    Address parse_address(const string& addr);

    struct Address {
        string ip;
        int port;

        // constructors
        Address() : ip(""), port(0) {}
        Address(const string& ip_, int port_) : ip(ip_), port(port_) {}

        // helper to create from "ip:port" string
        static Address from_string(const string& addr_str) {
            return parse_address(addr_str);
        }

        // convert back to string for logging/hashing
        string to_string() const {
            return ip + ":" + std::to_string(port);
        }

        // equality for maps/sets
        bool operator==(const Address& other) const {
            return ip == other.ip && port == other.port;
        }
    };
}}

// hash function for using Address as map key (https://www.geeksforgeeks.org/cpp/stdhash-class-in-c-stl/)
namespace std {
    template<>
    struct hash<ziplog::api::Address> {
        size_t operator()(const ziplog::api::Address& addr) const {
            return hash<string>()(addr.ip) ^ hash<int>()(addr.port);
        }
    };
}