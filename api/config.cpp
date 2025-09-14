#include "config.h"
#include <third_party/json.hpp>
#include <set>
#include <regex>
#include <fstream>
#include <sstream>

using json = nlohmann::json;
using std::regex;

namespace ziplog {
namespace api {
    /*
        Parses "IP:PORT" address strings. Accepts IPv4 (0.0.0.0-255.255.255.255)
        and "localhost". Ports must be in dynamic range 49152-65535.
        Examples: "127.0.0.1:50001", "localhost:55000", "192.168.1.1:51234"
        @throws ConfigParseError for invalid format, IP, or port range
    */
    pair<string, int> parseAddress(const string& addr) {
        // find presence of colon
        size_t colonPos = addr.find_last_of(':');
        if (colonPos == string::npos) throw ConfigParseError("Config Address is not a valid IP address, missing colon");

        // split on semicolon
        string ipAddr = addr.substr(0, colonPos);
        string portStr = addr.substr(colonPos + 1);
        int port;

        if (ipAddr.empty() || portStr.empty()) throw ConfigParseError("Config Address or Port is empty");

        // validate addr is ipv4 within range 0-255
        regex ipPattern(
            R"(^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)"
        );
        if (ipAddr == "localhost") ipAddr = "127.0.0.1";
        if (!regex_match(ipAddr, ipPattern)) throw ConfigParseError("Invalid IP address: " + ipAddr);

        // validate port number is within range 1-65535
        try {
            port = stoi(portStr);
        } catch (...) {
            throw ConfigParseError("Invalid port: " + portStr);
        }
        if (port < 1 || port > 65535) throw ConfigParseError("Port " + portStr + " not in range (1-65535)");

        // return parsed tcp information
        return {ipAddr, port};
    }

    ziplogConfig parseConfigJSON(const string& json_str) {
        ziplogConfig config;

        try {
            json j = json::parse(json_str);

            // store 'f'
            if (!j.contains("f") || !j["f"].is_number_integer()) {
                throw ConfigParseError("Missing or invalid 'f' field");
            }
            config.f = j["f"];
            if (config.f < 0) throw ConfigParseError("'f' field must be >= 0");

            // helper to extract and validate arrays
            auto extractArray = [&](const string& field) -> vector<string> {
                if (!j.contains(field) || !j[field].is_array()) {
                    throw ConfigParseError("Missing or invalid '" + field + "' field");
                }

                vector<string> result;
                std::set<string> seen;
                for (const auto& item : j[field]) {
                    if (!item.is_string()) {
                        throw ConfigParseError("Non-string item in " + field + " array");
                    }
                    // verify valid address
                    string addr = item;
                    if (seen.count(addr)) throw ConfigParseError("Duplicate address");
                    seen.insert(addr);
                    parseAddress(addr);
                    result.push_back(addr);
                }
                return result;
            };

            // update config (does not check for duplicates or 2f + 1 servers
            config.clients = extractArray("clients");
            config.servers = extractArray("servers");
            config.subscribers = extractArray("subscribers");

            // if (config.clients.size() < 1) throw ConfigParseError("Missing clients");
            // if (config.subscribers.size() < 1) throw ConfigParseError("Missing subscribers");
            if (config.servers.size() < static_cast<size_t>(2 * config.f + 1)) {
                throw ConfigParseError("Insufficient servers: need at least " +
                                      std::to_string(2 * config.f + 1) +
                                      " servers for f=" + std::to_string(config.f));
            }
        } catch (const ConfigParseError& e) {
            // re-throw the specific error
            throw;
        } catch (const json::parse_error& e) {
            throw ConfigParseError("JSON syntax error at byte " + std::to_string(e.byte) + ": " + e.what());
        } catch (const json::exception& e) {
            throw ConfigParseError("JSON processing error: " + string(e.what()));
        } catch (const std::exception& e) {
            throw ConfigParseError("Unexpected error during config parsing: " + string(e.what()));
        }
        return config;
    }

    ziplogConfig parseConfig(const string& filename) {
        std::ifstream file(filename);    // https://cplusplus.com/reference/fstream/ifstream/ifstream/
        if (!file.is_open()) throw ConfigParseError("Can't open file: " + filename);

        std::stringstream buffer;
        buffer << file.rdbuf();         // https://cplusplus.com/reference/ios/ios/rdbuf/
        file.close();
        return parseConfigJSON(buffer.str());
    }
}}