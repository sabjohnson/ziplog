#include <vector>
#include <string>
#include <utility>
#include <stdexcept>

using std::vector;
using std::string;
using std::pair;

namespace ziplog {
namespace api {

    class ConfigParseError : public std::runtime_error {
    public:
        ConfigParseError(const string& msg) : std::runtime_error(msg) {}
    };

    struct ziplogConfig {
    int f;
    vector<string> clients;
    vector<string> servers;
    vector<string> subscribers;
    };

    // helper to split "127.0.0.1:8001" into host and port
    pair<string, int> parseAddress(const string& addr);

    /*
        input: JSON filename
        return: ziplogConfig object
        throws: ConfigParseError if file cannot be read or JSON is invalid
    */
    ziplogConfig parseConfig(const string& filename);

    /*
        input: string in format of JSON object
        return: ziplogConfig object
        throws: ConfigParseError if JSON is invalid
    */
    ziplogConfig parseConfigJSON(const string& json_str);
}}