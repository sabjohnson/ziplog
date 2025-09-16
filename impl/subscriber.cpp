#include "subscriber.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <cstring>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(int subscriber_id, ziplogConfig &cfg) {
        if (subscriber_id < 0 || static_cast<size_t>(subscriber_id) >= cfg.subscribers.size()) {
            throw std::invalid_argument("Id " + std::to_string(subscriber_id) + " not in range of " + std::to_string(cfg.subscribers.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        id = subscriber_id;
        config = cfg;
        auto [ip, p] = cfg.subscribers[subscriber_id];
        ipAddress = ip;
        port = p;
        isRunning = false;
        subscriber_sock = -1;
        running_thread = std::thread(&Subscriber::run, this);
    }
    
    void Subscriber::run() {
        if (isRunning) {
            std::cerr << "Subscriber " << std::to_string(id) << " already running" << std::endl;
            return;
        }
        
        // create socket
        subscriber_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (subscriber_sock < 0) {
            std::cerr << "Subscriber " << std::to_string(id) << " failed to create server socket" << std::endl;
            return;
        }
        
        // set sock option to allow re-use (https://pubs.opengroup.org/onlinepubs/009695099/functions/setsockopt.html)
        int enabled = 1;
        if (setsockopt(subscriber_sock, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
            std::cerr << "Subscriber " << std::to_string(id) << " failed to set server socket options" << std::endl;
            return;
        }
        
        // bind socket to address
        struct sockaddr_in subscriber_addr;
        memset(&subscriber_addr, 0, sizeof(subscriber_addr));
        subscriber_addr.sin_family = AF_INET;
        subscriber_addr.sin_port = htons(port);
        inet_pton(AF_INET, ipAddress.c_str(), &subscriber_addr.sin_addr);
        
        if (bind(subscriber_sock, (struct sockaddr*)&subscriber_addr, sizeof(subscriber_addr)) < 0) {
            std::cerr << "Failed to bind socket to " << ipAddress << ":" << port << std::endl;
            close(subscriber_sock);
            return;
        }
        
        // listen for connections (https://man7.org/linux/man-pages/man2/listen.2.html)
        if (listen(subscriber_sock, 10) < 0) {
            std::cerr << "Failed to listen on socket" << std::endl;
            close(subscriber_sock);
            return;
        }
        
        // handle connections
        std::cout << "Subscriber " << id << " listening on " << ipAddress << ":" << port << std::endl;
        isRunning = true;
        while (isRunning) {
            struct sockaddr_in server_addr;
            socklen_t server_len = sizeof (server_addr);
            
            // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
            int server_socket = accept(subscriber_sock, (struct sockaddr*)&server_addr, &server_len);
            if (server_socket < 0) {
                if (isRunning) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }
            
            // handle server connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
            std::thread server_thread(&Subscriber::handleServer, this, server_socket);
            server_thread.detach();
        }
        
        // close socket
        close(subscriber_sock);
    }
    
    void Subscriber::handleServer(int server_sock) {
        while (isRunning) {
            message msg;
            // read message
            if (!NetworkUtils::recvMessage(server_sock, msg)) {
                //std::cerr << "Failed to receive message from server" << std::endl;
                break;
            }
            std::cout << "Subscriber " << id << " received message from server " << msg.sender_id
                      << " (seq: " << msg.sequence_number << ", type: " << msg.type << ")" << std::endl;
            
            // process message
            if (msg.type == APPEND) {
                processForQuorum(msg);
            }
            
            // send response (default to ack for now)
            message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id;
            ack_msg.sequence_number = msg.sequence_number;
            ack_msg.data = "gotchu!";
            
            if (!NetworkUtils::sendMessage(server_sock, ack_msg)) {
                std::cerr << "Failed to send ACK to server" << std::endl;
                // break here?
            } else {
                //std::cout << "Subscriber " << id << " sent ACK for message " << msg.sequence_number << std::endl;
            }
        }
        close(server_sock);
    }
    
    void Subscriber::processForQuorum(const ziplog::api::message &msg) {
        std::lock_guard<std::mutex> lock(mutex);
        
        // check if applied
        if (applied.count(msg.sequence_number)) {
            return;
        }
        pending[msg.sequence_number].insert(msg.sender_id);
        
        // check for quorum
        if (pending[msg.sequence_number].size() > static_cast<size_t>(config.f)) {
            applyOperation(msg);
            applied.insert(msg.sequence_number);
            pending.erase(msg.sequence_number);
            //std::cout << "Subscriber " << id << " applied message " << msg.sequence_number
            //      << " after receiving from " << (config.f + 1) << " servers" << std::endl;
        } else {
            std::cout << "Subscriber " << id << " buffering message " << msg.sequence_number
                  << " (" << pending[msg.sequence_number].size() << "/" << (config.f + 1) << ")" << std::endl;
        }
    }
    
    void Subscriber::applyOperation(const message &msg) {
        log.push_back(msg.data);
        std::cout << "Server " << id << " APPLIED message " << msg.sequence_number << " (log size: " << log.size() << ")" << std::endl;
    }
    
    void Subscriber::shutdown() {
        isRunning = false;
        if (subscriber_sock >= 0) {
            ::shutdown(subscriber_sock, SHUT_RDWR);
            close(subscriber_sock);
            subscriber_sock = -1;
        }
        std::cout << "Subscriber " << id << " shutting down" << std::endl;
    }
    
    Subscriber::~Subscriber() {
        shutdown();
        if (running_thread.joinable()) {
            running_thread.join();  // wait for thread to finish
        }
        std::cout << "Final log..." << std::endl;
        for (size_t i = 0; i < log.size(); i++) {
            std::cout << "Index " << i << ": " << log[i] << std::endl;
        }
    }
}}
