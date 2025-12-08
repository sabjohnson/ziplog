#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include "zipper.h"
#include "client.h"
#include "proxy.h"
#include "server.h"
#include "subscriber.h"
#include <gtest/gtest.h>

using namespace ziplog::api;
using namespace ziplog::impl;

namespace ziplog::test {

/**
 * @brief Waits for epoch propagation
 * @param num Number of epochs to wait for
 */
inline void wait_for_propagation(int num = 3) {
    auto duration = num * ziplog::EPOCH_DURATION_MS;
    std::this_thread::sleep_for(std::chrono::milliseconds(duration));
}

/**
 * @brief Verifies the provided vector of commands matches the expected size and entries
 * @param output The actual commands from the log
 * @param expected The expected command strings
 */
inline void verify_index_matches_expected(const std::vector<Command>& output,
                                          const std::vector<std::string>& expected) {
    ASSERT_EQ(output.size(), expected.size());
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(command_to_string(output[i]), expected[i]);
    }
}

/**
 * @brief Removes skips and expands batched commands
 * @param original_log The raw log with possible batches and skips
 * @return Expanded log as a vector of batches
 */
inline std::vector<std::vector<Command>> expand_log(const std::vector<Command>& original_log) {
    std::vector<std::vector<Command>> log;

    // expand all non-empty entries (may be batches)
    for (const Command& entry : original_log) {
        std::vector<Command> batch = CommandBatch::deserialize(entry);
        if (!batch.empty()) {
            log.push_back(batch);
        }
    }

    return log;
}

/**
 * @brief Base test fixture with common setup/teardown and utilities
 */
class ZiplogTestBase : public ::testing::Test {
protected:
    void StartSystem(const std::string& filename) {
        // load test config
        config = parse_config(filename);

        // start all components
        zipper = std::make_unique<Zipper>(config.make_zipper_config());
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_subscribers(); i++) {
            subscribers.push_back(std::make_unique<Subscriber>(config.make_subscriber_config(i)));
        }
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_servers(); i++) {
            servers.push_back(std::make_unique<Server>(config.make_server_config(i)));
        }
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_proxies(); i++) {
            proxies.push_back(std::make_unique<Proxy>(config.make_proxy_config(i)));
        }

        for (size_t i = 0; i < config.num_proxies(); i++) {
            clients.push_back(std::make_unique<Client>(config.proxies[i]));
        }
    }

    void TearDown() override {
        clients.clear();
        subscribers.clear();
        servers.clear();
        proxies.clear();
        zipper.reset();
    }

    /**
     * @param client_id Client/proxy id we want to talk to
     * @param cmd The command string to send
     * @return True if successful response from proxy
     */
    bool send_append(NodeId client_id, const std::string& cmd) {
        if (client_id >= clients.size()) return false;
        return clients[client_id]->append(string_to_command(cmd));
    }

    // Shared member variables
    ZiplogConfig config;
    std::unique_ptr<Zipper> zipper;
    std::vector<std::unique_ptr<Client>> clients;
    std::vector<std::unique_ptr<Proxy>> proxies;
    std::vector<std::unique_ptr<Server>> servers;
    std::vector<std::unique_ptr<Subscriber>> subscribers;
};

} // namespace ziplog::test

#endif // TEST_UTILS_H