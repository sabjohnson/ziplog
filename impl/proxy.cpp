#include "proxy.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Proxy::Proxy(NodeId proxy_id, const ZiplogConfig &cfg)
        : BaseNode(proxy_id, cfg, cfg.proxies[proxy_id].first, cfg.proxies[proxy_id].second)
    {
        // validate node id
        validate_node_id(proxy_id, cfg.num_proxies(), "Proxy");

        // set value of members (we already know ip addr is in our valid range based on parsed config)
        batch_size_ = 3;
        start_listening();

        // epoch tracking
        //epoch_thread = thread(&Proxy::epoch_timer(), this);
    }

    void Proxy::handle_connection(int client_socket) {
        // read from and respond to valid request
        Message req;
        if (!NetworkUtils::recv_message(client_socket, req)) {
            close(client_socket);
            return;
        }
        cout << "received smthn" << endl;

        bool success = false;
        if (req.type == APPEND) {
            cout << "append" << endl;
            success = handle_append(req.data);
        }

        // build and send response
        Message resp;
        resp.type = FAILURE;

        if (success) {
            resp.type = SUCCESS;
        }

        NetworkUtils::send_message(client_socket, resp);
        close(client_socket);
    }

    bool Proxy::handle_append(const Command& data) {
        // get current timestamp
        Timestamp now = now_ms();

        // add timestamp and data to batch
        batch_times_.push_back(now);
        batch_values_.push_back(data);

        if (batch_times_.size() < batch_size_) {
            return true;
        }

        // build request to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard();
        zip_req.sender_id = id_;
        zip_req.set_num_requests(batch_size_);
        zip_req.set_timestamps(batch_times_);

        Message zip_resp;
        if (!NetworkUtils::request_from_zipper(config_.zipper.first, config_.zipper.second, zip_req, zip_resp, config_.timeout_ms, config_.max_retries)) {
            return false;
        }

        if (zip_resp.get_assigned_sequences().empty()) {
            std::cerr << "Zipper returned no sequence numbers" << std::endl;
            return false;
        }

        batch_times_.clear();
        batch_values_.clear();

        // create msg to be broadcasted
        Message msg;
        msg.type = APPEND;
        msg.shard_id = shard();
        msg.sender_id = id();
        msg.seq_or_count = zip_resp.get_assigned_sequences()[0];
        msg.data = data;

        return replicate_on_quorum(msg);
    }

    // attempt to replicate on f + 1 storage servers
    bool Proxy::replicate_on_quorum(Message& msg) {
        size_t successful_sends = 0;
        for (size_t i = 0; i < config_.num_servers() && successful_sends < config_.quorum(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            if (NetworkUtils::send_message_to_address(server_ip, server_port, msg, config_.timeout_ms, config_.max_retries)) {
                successful_sends++;
            }
        }
        return successful_sends == config_.quorum();
    }
    
    void Proxy::shutdown() {
        BaseNode::shutdown();
        cout << "Proxy shutting down" << endl;
    }

    Proxy::~Proxy() {
        shutdown();
    }
}}
