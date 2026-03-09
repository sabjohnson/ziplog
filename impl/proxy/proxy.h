#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "slot_scheduler.h"
#include "server_replicator.h"
#include "client_buffer_manager.h"
#include "proxy_epoch_timer.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    class Proxy : public BaseNode<ProxyConfig>
    {
    public:
        Proxy(const ProxyConfig &cfg);
        Proxy(const ProxyConfig &cfg, bool registered);
        ~Proxy();

        void attempt_join(bool is_new);
        size_t num_servers() const { return config_.servers.size(); }

    private:
        // --- components ---
        SlotScheduler slot_scheduler_;
        ServerReplicator replicator_;
        ClientBufferManager client_buffers_;
        ProxyEpochTimer epoch_timer_;

        // --- zipper connection ---
        ConnectionPool zipper_pool_;
        atomic<bool> registered_{true};

        // --- epoch callbacks (called by epoch_timer_) ---
        void update_slot_estimate();
        void send_out_batch();

        // --- message handling ---
        void handle_connection(int client_socket) override;
        void handle_zip_response(const Message &msg);
    };
} // namespace ziplog::impl