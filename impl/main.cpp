#include "config.h"
#include "client.h"
#include "server.h"
#include "subscriber.h"
#include <iostream>

#define SUCCESS 0
#define ERROR -1

using namespace ziplog::api;
using namespace ziplog::impl;

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " client|server|subscriber config_file ID_number" << std::endl;
        std::cerr << "Example: " << argv[0] << " client config/servers.json 0" << std::endl;
        return ERROR;
    }

    try {
        string mode = argv[1];
        string configFile = argv[2];
        int id = std::stoi(argv[3]);

        ziplogConfig config = parseConfig(configFile);

        if (mode == "client") {
            //std::cout << "Client mode with ID " << id << std::endl;
            Client client(id, config);
            
            // read from stdin and send appends
            std::cout << "Type string value to send APPENDs and hit Enter. Enter 'quit' to shutdown..." << std::endl;
            string line;
            while (std::getline(std::cin, line)) {
                if (line == "quit") break;
                bool success = client.append(line);
                std::cout << (success ? "Sent successfully\n" : "Send failed\n") << std::endl;
            }
        } else if (mode == "server") {
            //std::cout << "Server mode with ID " << id << std::endl;
            Server server(id, config);
            
            // keep alive until user presses Enter
            std::cout << "Press Enter to shutdown..." << std::endl;
            std::cin.get();
        } else if (mode == "subscriber") {
            //std::cout << "Subscriber mode with ID " << id << std::endl;
            Subscriber subscriber(id, config);
            
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
