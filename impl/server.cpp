#include "server.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Server::Server(NodeId server_id, const ZiplogConfig &cfg)
        : BaseNode(server_id, cfg, cfg.servers[server_id].first, cfg.servers[server_id].second)
    {
        // validate node id
        validate_node_id(server_id, cfg.num_servers(), "Server");
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        start_listening();
    }
    
    void Server::handle_connection(int proxy_socket) {
        while (is_running_) {
            // recv message from proxy
            Message msg;
            if (!NetworkUtils::recv_message(proxy_socket, msg)) {
                //std::cerr << "Failed to receive message from proxy" << std::endl;
                break;
            }
            std::cout << "Server " << id_ << " received message from proxy " << msg.sender_id
                      << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;

            // process message
            if (msg.type == APPEND) {
                broadcast_to_subscribers(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.shard_id = shard_id_;
            ack_msg.sender_id = id_;
            ack_msg.seq_or_count = msg.seq_or_count;
            
            if (!NetworkUtils::send_message(proxy_socket, ack_msg)) {
                std::cerr << "Failed to send ACK to proxy" << std::endl;
                // break here?
            } else {
                std::cout << "Server " << id_ << " sent ACK for message " << msg.seq_or_count << "\n" << std::endl;
            }
        }
        close(proxy_socket);
    }

    void Server::broadcast_to_subscribers(const Message &msg) {
        // verify validity of sender (valid proxy)
        if (msg.shard_id != shard_id_ || !config_.isValidProxy(msg.sender_id)) {
            return;
        }

        Message fwd_msg = msg;
        fwd_msg.sender_id = id_;

        for (size_t i = 0; i < config_.num_subscribers(); i++) {
            auto [subscriber_ip, subscriber_port] = config_.subscribers[i];

            if (!NetworkUtils::send_message_to_address(subscriber_ip, subscriber_port, fwd_msg, config_.timeout_ms, config_.max_retries)) {
                std::cerr << "Server " << id_ << " failed to deliver to subscriber " << i << " after " << config_.max_retries << " attempts" << std::endl;
            }
        }
    }

    void Server::shutdown() {
        BaseNode::shutdown();
        std::cout << "Server " << id_ << " shutting down" << std::endl;
    }

    Server::~Server() {
        shutdown();
    }

}}
