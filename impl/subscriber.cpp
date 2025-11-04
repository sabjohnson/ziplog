#include "subscriber.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(NodeId subscriber_id, const ZiplogConfig &cfg)
        : BaseNode(subscriber_id, cfg, cfg.subscribers[subscriber_id].first, cfg.subscribers[subscriber_id].second)
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
            if (msg.type == APPEND || msg.type == SKIP) {
                process_for_quorum(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id();
            ack_msg.seq_or_count = msg.seq_or_count;
            
            if (!NetworkUtils::send_message(server_sock, ack_msg)) {
                break;
            }
        }
        close(server_sock);
    }
    
    void Subscriber::process_for_quorum(const Message &msg) {
        // verify validity of sender (note: don't compare shards because "subscribers consume records from one or more shards" pg. 4)
        if (!config_.isValidServer(msg.sender_id)) {
            cout << "invalid server: " << msg.sender_id << endl;
            return;
        }
        //cout << "[subscriber " << id() << "] received message from server " << msg.sender_id
             //     << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << endl;

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
        out_of_order_[msg.get_sequence_number()] = msg.data;

        // add all available consecutive commands
        while (out_of_order_.count(next_seq_)) {
            log_.push_back(out_of_order_[next_seq_]);
            out_of_order_.erase(next_seq_);
            next_seq_++;
        }
        //print_log();
    }

    void Subscriber::print_log() {
        cout << "Current log..." << endl;
        for (size_t i = 0; i < log_.size(); i++) {
            cout << "Index " << i << ": " << string(log_[i].begin(), log_[i].end()) << endl;
        }
    }

    /*
        @brief: Removes skips and expands batched commands
        @return: Expanded log
    */
    void Subscriber::print_expanded_log() {
        vector<vector<Command>> new_log;

        // expand all non-empty entries (may be batches)
        for (const Command& entry : log_) {
            vector<Command> batch = CommandBatch::deserialize(entry);
            if (!batch.empty()) new_log.push_back(batch);
        }

        // log is built, print it out
        cout << "-------- expanded log (" << new_log.size() << ") --------" << endl;
        int i = 0;
        for (const vector<Command>& entry : new_log) {
            cout << "index " << i << ": ";
            for (const Command& c : entry) cout << command_to_string(c);
            cout << endl;
            i++;
        }

        cout << "-------- pending log entries (" << pending_quorum_.size() << ") --------" << endl;
        i = 0;
        for (const auto& [seq, servers] : pending_quorum_) {
            cout << "seq " << seq << ": " << servers.size() << " servers" << endl;
            i++;
        }


        cout << "-------- out of order log entries (" << out_of_order_.size() << ") --------" << endl;
        i = 0;
        for (const auto& [seq, cmd] : out_of_order_) {
            cout << "seq " << seq << ": " << command_to_string(cmd) << endl;
            i++;
        }
    }
    
    void Subscriber::shutdown() {
        BaseNode::shutdown();
        cout << "Subscriber " << id() << " shutting down" << endl;
    }
    
    Subscriber::~Subscriber() {
        shutdown();
        print_expanded_log();
    }
}}
