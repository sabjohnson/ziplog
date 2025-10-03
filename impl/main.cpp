#include "common.h"
#include "config.h"
#include "zipper.h"
#include "client.h"
#include "proxy.h"
#include "server.h"
#include "subscriber.h"

#define SUCCESS 0
#define ERROR -1

using namespace ziplog::api;
using namespace ziplog::impl;

int main(int argc, char* argv[]) {
    if (argc != 3 && argc != 4) {
        std::cerr << "Usage: <mode> <config_file> [id]" << std::endl;
        std::cerr << "Modes:" << std::endl;
        std::cerr << "  zipper              - no ID required" << std::endl;
        std::cerr << "  client <proxy_id>   - ID required" << std::endl;
        std::cerr << "  proxy <id>          - ID required" << std::endl;
        std::cerr << "  server <id>         - ID required" << std::endl;
        std::cerr << "  subscriber <id>     - ID required" << std::endl;
        std::cerr << "\nExamples:" << std::endl;
        std::cerr << "  " << argv[0] << " zipper config/servers.json" << std::endl;
        std::cerr << "  " << argv[0] << " proxy config/servers2.json 0" << std::endl;
        return ERROR;
    }

    try {
        string mode = argv[1];
        string config_file = argv[2];

        optional<NodeId> id;
        if (argc == 4) {
            id = static_cast<NodeId>(std::stoi(argv[3]));
        }

        ZiplogConfig config = parse_config(config_file);

        if (mode == "zipper") {
            Zipper zipper(config);
            // keep alive until user presses Enter
            std::cout << "Press Enter to shutdown..." << std::endl;
            std::cin.get();
        } else if (mode == "client") {
            if (!id.has_value()) {
                std::cerr << "Client mode requires an proxy ID to contact" << std::endl;
                return ERROR;
            }
            Client client(config, *id);

            // read from stdin and send appends
            std::cout << "Type string value to send APPENDs and hit Enter. Enter 'quit' to shutdown..." << std::endl;
            string line;
            while (std::getline(std::cin, line)) {
                if (line == "quit") break;
                bool success = client.append(line);
                std::cout << (success ? "Sent successfully" : "Send failed") << std::endl;
            }
        } else if (mode == "proxy") {
            if (!id.has_value()) {
                std::cerr << "Proxy mode requires an ID" << std::endl;
                return ERROR;
            }
            Proxy proxy(*id, config);
            
            // keep alive until user presses Enter
            std::cout << "Press Enter to shutdown..." << std::endl;
            std::cin.get();
        } else if (mode == "server") {
            if (!id.has_value()) {
                std::cerr << "Server mode requires an ID" << std::endl;
                return ERROR;
            }
            Server server(*id, config);
            
            // keep alive until user presses Enter
            std::cout << "Press Enter to shutdown..." << std::endl;
            std::cin.get();
        } else if (mode == "subscriber") {
            if (!id.has_value()) {
                std::cerr << "Subscriber mode requires an ID" << std::endl;
                return ERROR;
            }
            Subscriber subscriber(*id, config);
            
            // keep alive until user presses Enter
            std::cout << "Press Enter to shutdown..." << std::endl;
            std::cin.get();
        } else {
            std::cerr << "Unsupported mode: " << mode << std::endl;
            return ERROR;
        }
    } catch (const ConfigParseError& e) {
        std::cerr << "Config Error: " << e.what() << std::endl;
        return ERROR;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return ERROR;
    }
    return SUCCESS;
}
