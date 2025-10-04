#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Client {
    private:
        ZiplogConfig config_;
        NodeId proxy_id_;

    public:
        Client(const ZiplogConfig& cfg, NodeId proxy_id);

        bool append(const Command& data);

        bool append(const string& data) {
            return append(Command(data.begin(), data.end()));
        }

        bool bulk_append(const vector<Command>& commands);
    };
}}