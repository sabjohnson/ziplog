#include "subscriber.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(NodeId subscriber_id, const ZiplogConfig &cfg)
        : BaseNode(subscriber_id, cfg, cfg.servers[subscriber_id].first, cfg.servers[subscriber_id].second)
    {
        // validate node id
        validate_node_id(subscriber_id, cfg.num_subscribers(), "Subscriber");
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        next_seq_ = 0;
        start_listening();
    }

    void Subscriber::handle_connection(int server_sock) {
        while (running()) {
            // read message
            Message msg;
            if (!NetworkUtils::recv_message(server_sock, msg)) {
                break;
            }

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
                break;
            }
        }
        close(server_sock);
    }
    
    void Subscriber::process_for_quorum(const Message &msg) {
        // verify validity of sender (note: don't compare shards because "subscribers consume records from one or more shards" pg. 4)
        if (config_.isValidServer(msg.sender_id)) {
            return;
        }
        cout << "Subscriber " << id() << " received message from server " << msg.sender_id
                  << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << endl;

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

    void Subscriber::print_log() {
        cout << "Current log..." << endl;
        for (size_t i = 0; i < log_.size(); i++) {
            cout << "Index " << i << ": " << string(log_[i].begin(), log_[i].end()) << endl;
        }
    }
    
    void Subscriber::shutdown() {
        BaseNode::shutdown();
        cout << "Subscriber " << id() << " shutting down" << endl;
    }
    
    Subscriber::~Subscriber() {
        shutdown();
        print_log();
    }
}}
