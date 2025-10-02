#pragma once
#include "types.h"
#include <stdexcept>

namespace ziplog {
namespace api {

    class ConfigParseError : public std::runtime_error {
    public:
        ConfigParseError(const string& msg) : std::runtime_error(msg) {}
    };

    struct ZiplogConfig {
        int f;
        int max_retries;
        int timeout_ms;
        pair<string, int> zipper;
        vector<pair<string, int>> proxies;
        vector<pair<string, int>> servers;
        vector<pair<string, int>> subscribers;

        // helper methods
        size_t quorum() const {
            return f + 1;
        }

        size_t num_proxies() const {
            return proxies.size();
        }

        size_t num_servers() const {
            return servers.size();
        }

        size_t num_subscribers() const {
            return subscribers.size();
        }


        bool isValidProxy(NodeId id) {
            if (id >= static_cast<NodeId>(num_proxies())) return false;
            return true;
        }

        bool isValidServer(NodeId id) {
            if (id >= static_cast<NodeId>(num_servers())) return false;
            return true;
        }

        bool isValidSubscriber(NodeId id) {
            if (id >= static_cast<NodeId>(num_subscribers())) return false;
            return true;
        }
    };

    /*
        @brief: Validates string representing ipv4 address ("127.0.0.1:8001") and returns host and port.
        @param addr: String representing ipv4 address to be parsed/validated.
        @return: Pair of string and int if valid input.
        @throws: ConfigParseError on unsuccessful parsing (look at comments in config.cpp for more).
    */
    pair<string, int> parse_address(const string& addr);

    /*
        @brief: Takes file path of file containing JSON object and parses the file to create a ZiplogConfig.
        @param filename: Path of file.
        @return: ZiplogConfig object.
        @throws: ConfigParseError if file cannot be read or JSON structure is invalid.
    */
    ZiplogConfig parse_config(const string& filename);

    /*
        @brief: Takes raw string representing JSON object and parses into a ZiplogConfig.
        @param json_str: String JSON object representing ziplog config.
        @return: ZiplogConfig object.
        @throws: ConfigParseError if JSON structure is invalid.
    */
    ZiplogConfig parse_config_JSON(const string& json_str);
}}