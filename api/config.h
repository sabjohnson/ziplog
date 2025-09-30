#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <utility>
#include <stdexcept>

using std::vector;
using std::string;
using std::pair;

namespace ziplog {
namespace api {

    static constexpr uint64_t EPOCH_DURATION_MS = 10000;  // keep this at a multiple of 10


    class ConfigParseError : public std::runtime_error {
    public:
        ConfigParseError(const string& msg) : std::runtime_error(msg) {}
    };

    struct ziplogConfig {
        int f;
        int max_retries;
        int timeout_ms;
        pair<string, int> zipper;
        vector<pair<string, int>> proxies;
        vector<pair<string, int>> servers;
        vector<pair<string, int>> subscribers;
    };

    /*
        @brief: Validates string representing ipv4 address ("127.0.0.1:8001") and returns host and port.
        @param addr: String representing ipv4 address to be parsed/validated.
        @return: Pair of string and int if valid input.
        @throws: ConfigParseError on unsuccessful parsing (look at comments in config.cpp for more).
    */
    pair<string, int> parseAddress(const string& addr);

    /*
        @brief: Takes file path of file containing JSON object and parses the file to create a ziplogConfig.
        @param filename: Path of file.
        @return: ziplogConfig object.
        @throws: ConfigParseError if file cannot be read or JSON structure is invalid.
    */
    ziplogConfig parseConfig(const string& filename);

    /*
        @brief: Takes raw string representing JSON object and parses into a ziplogConfig.
        @param json_str: String JSON object representing ziplog config.
        @return: ziplogConfig object.
        @throws: ConfigParseError if JSON structure is invalid.
    */
    ziplogConfig parseConfigJSON(const string& json_str);
}}