#include "config.h"
//#include "client.h"
//#include "server.h"
//#include "subscriber.h"
#include <iostream>
#include <string>

#define SUCCESS 0
#define ERROR -1

using namespace ziplog::api;


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
            std::cout << "Client mode with ID " << id << std::endl;
        } else if (mode == "server") {
            std::cout << "Server mode with ID " << id << std::endl;
        } else if (mode == "subscriber") {
            std::cout << "Subscriber mode with ID " << id << std::endl;
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
