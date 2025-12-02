#include "subscriber.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Subscriber::Subscriber(const SubscriberConfig &cfg)
        : BaseNode<SubscriberConfig>(cfg)
    {
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        log_.push_back(Command());
        next_seq_ = 1;
        start_listening();
    }


    Subscriber::Subscriber(const SubscriberConfig &cfg, bool registered)
            : BaseNode<SubscriberConfig>(cfg)
    {
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        log_.push_back(Command());
        next_seq_ = 1;
        start_listening();

        // register with zipper
        Message req;
        req.type = REGISTER_SUBSCRIBER;
        req.shard_id = shard();
        req.sender_id = id();
        string addr_info = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr_info.begin(), addr_info.end());

        // send message to the zipper
        int sock = connection_pool_.get_connection(config_.zipper);
        if (sock >= 0) {
            NetworkUtils::send_message(sock, req);
        }
    }

    void Subscriber::handle_connection(int server_sock) {
        cout << "[subscriber " << id() << "] handle_connection() called" << endl;
        while (running()) {
            // read message
            Message msg;
            cout << "[subscriber " << id() << "] calling recv_meesage()" << endl;
            if (!NetworkUtils::recv_message(server_sock, msg)) {
                cout << "[subscriber " << id() << "] done calling recv_meesage()" << endl;
                break;
            }
            cout << "[subscriber " << id() << "] done calling recv_meesage()" << endl;

            // process message
            if (msg.type == APPEND || msg.type == SKIP) {
                process_for_quorum(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id();
            ack_msg.seq_or_count = msg.seq_or_count;

            cout << "[subscriber " << id() << "] calling send_meesage()" << endl;
            if (!NetworkUtils::send_message(server_sock, ack_msg)) {
                cout << "[subscriber " << id() << "] done calling send_meesage()" << endl;
                break;
            }
            cout << "[subscriber " << id() << "] done calling send_meesage()" << endl;
        }
        close(server_sock);
        cout << "[subscriber " << id() << "] handle_connection() exitting" << endl;
    }
    
    void Subscriber::process_for_quorum(const Message &msg) {
        // verify validity of sender (note: don't compare shards because "subscribers consume records from one or more shards" pg. 4)
        if (!config_.isValidServer(msg.sender_id)) {
            cout << "invalid server: " << msg.sender_id << endl;
            return;
        }
        cout << "[subscriber " << id() << "] received message from server " << msg.sender_id
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
        if (pending_quorum_[msg.seq_or_count].size() >= quorum()) {
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
        for (size_t i = 1; i < log_.size(); i++) {
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
        int i = 1;
        for (const vector<Command>& entry : new_log) {
            cout << "index " << i << ": ";
            for (const Command& c : entry) cout << command_to_string(c);
            cout << endl;
            i++;
        }

        cout << "-------- pending log entries (" << pending_quorum_.size() << ") --------" << endl;
        i = 1;
        for (const auto& [seq, servers] : pending_quorum_) {
            cout << "seq " << seq << ": " << servers.size() << " servers" << endl;
            i++;
        }


        cout << "-------- out of order log entries (" << out_of_order_.size() << ") --------" << endl;
        i = 1;
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
        connection_pool_.close_all();
        shutdown();
        print_expanded_log();
    }
}}
