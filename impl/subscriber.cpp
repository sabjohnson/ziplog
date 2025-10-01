#include "subscriber.h"
#include "network_utils.h"
#include <sys/socket.h>     // accept()
#include <netinet/in.h>     // sockaddr_in
#include <unistd.h>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(NodeId subscriber_id, const ZiplogConfig &cfg) {
        // validate node id
        validate_node_id(subscriber_id, cfg.num_subscribers(), "Subscriber");
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        config_ = cfg;
        id_ = subscriber_id;
        auto [ip, p] = cfg.subscribers[subscriber_id];
        ip_address_ = ip;
        port_ = p;
        is_running_ = false;
        subscriber_sock_ = -1;
        next_seq_ = 0;
        running_thread_ = thread(&Subscriber::run, this);
    }
    
    void Subscriber::run() {
        if (is_running_) {
            std::cerr << "Subscriber " << std::to_string(id_) << " already running" << std::endl;
            return;
        }
        
        // create listening socket
        subscriber_sock_ = NetworkUtils::create_listening_socket(ip_address_, port_, true);
        if (subscriber_sock_ < 0) {
            std::cerr << "Subscriber " << std::to_string(id_) << " failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        std::cout << "Subscriber " << id_ << " listening on " << ip_address_ << ":" << port_ << std::endl;
        is_running_ = true;
        while (is_running_) {
            struct sockaddr_in server_addr;
            socklen_t server_len = sizeof(server_addr);
            
            // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
            int server_socket = accept(subscriber_sock_, (struct sockaddr*)&server_addr, &server_len);
            if (server_socket < 0) {
                if (is_running_) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }
            
            // handle server connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
            thread server_thread(&Subscriber::handle_server, this, server_socket);
            server_thread.detach();
        }
    }
    
    void Subscriber::handle_server(int server_sock) {
        while (is_running_) {
            Message msg;
            // read message
            if (!NetworkUtils::recv_message(server_sock, msg)) {
                //std::cerr << "Failed to receive message from server" << std::endl;
                break;
            }
            
            // verify validity of sender (note: don't compare shards because "subscribers consume records from one or more shards" pg. 4)
            if (msg.sender_id >= static_cast<NodeId>(config_.num_servers())) {
                break;
            }
            std::cout << "Subscriber " << id_ << " received message from server " << msg.sender_id
                      << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;
            
            // process message
            if (msg.type == APPEND) {
                process_for_quorum(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id_;
            ack_msg.seq_or_count = msg.seq_or_count;
            
            if (!NetworkUtils::send_message(server_sock, ack_msg)) {
                std::cerr << "Failed to send ACK to server" << std::endl;
                // break here?
            } else {
                //std::cout << "Subscriber " << id << " sent ACK for message " << msg.seq_or_count << std::endl;
            }
        }
        close(server_sock);
    }
    
    void Subscriber::process_for_quorum(const Message &msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);
        
        // is already applied, return
        if (applied_.count(msg.seq_or_count)) {
            return;
        }

        // add this sender to set of acknowledgers
        pending_quorum_[msg.seq_or_count].insert(msg.sender_id);
        
        // attempt to apply this command if we have reached quorum
        if (pending_quorum_[msg.seq_or_count].size() >= config_.quorum()) {
            apply_operation(msg);
            applied_.insert(msg.seq_or_count);
            pending_quorum_.erase(msg.seq_or_count);
        }
    }
    
    void Subscriber::apply_operation(const Message &msg) {
        // store sequence number and command
        out_of_order_[msg.seq_or_count] = msg.data;

        // add all available consecutive commands
        while (out_of_order_.count(next_seq_)) {
            log_.push_back(out_of_order_[next_seq_]);
            out_of_order_.erase(next_seq_);
            next_seq_++;
        }
    }
    
    void Subscriber::shutdown() {
        is_running_ = false;
        if (subscriber_sock_ >= 0) {
            ::shutdown(subscriber_sock_, SHUT_RDWR);
            close(subscriber_sock_);
            subscriber_sock_ = -1;
        }
        std::cout << "Subscriber " << id_ << " shutting down" << std::endl;
    }
    
    Subscriber::~Subscriber() {
        shutdown();
        if (running_thread_.joinable()) {
            running_thread_.join();  // wait for thread to finish
        }
        std::cout << "Final log..." << std::endl;
        for (size_t i = 0; i < log_.size(); i++) {
            std::cout << "Index " << i << ": " << log_[i] << std::endl;
        }
    }
}}
