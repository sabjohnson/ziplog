#include "subscriber.h"
#include "network_utils.h"
#include <sys/socket.h>     // accept()
#include <netinet/in.h>     // sockaddr_in
#include <unistd.h>
#include <iostream>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(int subscriber_id, const ziplogConfig &cfg) {
        if (subscriber_id < 0 || static_cast<size_t>(subscriber_id) >= cfg.subscribers.size()) {
            throw std::invalid_argument("Id " + std::to_string(subscriber_id) + " not in range of " + std::to_string(cfg.subscribers.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        config = cfg;
        id = subscriber_id;
        auto [ip, p] = cfg.subscribers[subscriber_id];
        ipAddress = ip;
        port = p;
        isRunning = false;
        subscriber_sock = -1;
        next_seq = 0;
        running_thread = std::thread(&Subscriber::run, this);
    }
    
    void Subscriber::run() {
        if (isRunning) {
            std::cerr << "Subscriber " << std::to_string(id) << " already running" << std::endl;
            return;
        }
        
        // create listening socket
        subscriber_sock = NetworkUtils::createListeningSocket(ipAddress, port, true);
        if (subscriber_sock < 0) {
            std::cerr << "Subscriber " << std::to_string(id) << " failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        std::cout << "Subscriber " << id << " listening on " << ipAddress << ":" << port << std::endl;
        isRunning = true;
        while (isRunning) {
            struct sockaddr_in server_addr;
            socklen_t server_len = sizeof(server_addr);
            
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
    }
    
    void Subscriber::handleServer(int server_sock) {
        while (isRunning) {
            message msg;
            // read message
            if (!NetworkUtils::recvMessage(server_sock, msg)) {
                //std::cerr << "Failed to receive message from server" << std::endl;
                break;
            }
            
            // verify validity of sender (note: don't compare shards because "subscribers consume records from one or more shards" pg. 4)
            if (msg.sender_id >= static_cast<uint32_t>(config.servers.size())) {
                break;
            }
            std::cout << "Subscriber " << id << " received message from server " << msg.sender_id
                      << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;
            
            // process message
            if (msg.type == APPEND) {
                processForQuorum(msg);
            }
            
            // send response (default to ack for now)
            message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id;
            ack_msg.seq_or_count = msg.seq_or_count;
            ack_msg.data = "gotchu!";
            
            if (!NetworkUtils::sendMessage(server_sock, ack_msg)) {
                std::cerr << "Failed to send ACK to server" << std::endl;
                // break here?
            } else {
                //std::cout << "Subscriber " << id << " sent ACK for message " << msg.seq_or_count << std::endl;
            }
        }
        close(server_sock);
    }
    
    void Subscriber::processForQuorum(const ziplog::api::message &msg) {
        std::lock_guard<std::mutex> lock(mutex);
        
        // check if applied
        if (applied.count(msg.seq_or_count)) {
            return;
        }
        pending[msg.seq_or_count].insert(msg.sender_id); // store server id that sent this log entry
        
        // check for quorum
        if (pending[msg.seq_or_count].size() > static_cast<size_t>(config.f)) {
            applyOperation(msg);
            applied.insert(msg.seq_or_count);
            pending.erase(msg.seq_or_count);
            //std::cout << "Subscriber " << id << " applied message " << msg.seq_or_count
            //      << " after receiving from " << (config.f + 1) << " servers" << std::endl;
        } else {
//            std::cout << "Subscriber " << id << " buffering message " << msg.seq_or_count
//                  << " (" << pending[msg.seq_or_count].size() << "/" << (config.f + 1) << ")" << std::endl;
        }
    }
    
    void Subscriber::applyOperation(const message &msg) {

        gaps[msg.seq_or_count] = msg.data; // store

        while (gaps.count(next_seq)) {
            //std::cout << "Subscriber " << id << " APPLIED message " << next_seq << " (log size: " << log.size() << ")" << std::endl;
            log.push_back(gaps[next_seq]);
            gaps.erase(next_seq);
            next_seq++;
        }
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
