#include "zipper.h"
#include "client.h"
#include "proxy.h"
#include "server.h"
#include "subscriber.h"
#include <gtest/gtest.h>

/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     1       1       2       1           setup1.json
*/

using namespace ziplog::api;
using namespace ziplog::impl;

class E2ETest : public ::testing::Test {
protected:
    void SetUp() override {

    }

    void StartSystem(const string& filename) {
        // load test config
        config = parse_config(filename);

        // start all components
        zipper = std::make_unique<Zipper>(config);
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_subscribers(); i++) {
            subscribers.push_back(std::make_unique<Subscriber>(i, config));
        }
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_servers(); i++) {
            servers.push_back(std::make_unique<Server>(i, config));
        }
        std::this_thread::sleep_for(100ms);

        for (size_t i = 0; i < config.num_proxies(); i++) {
            proxies.push_back(std::make_unique<Proxy>(i, config));
        }

        for (size_t i = 0; i < config.num_proxies(); i++) {
            clients.push_back(std::make_unique<Client>(config, i));
        }
    }

    void TearDown() override {

        // Stop everything gracefully first
        for (auto& p : proxies) if (p) p->shutdown();
        for (auto& s : servers) if (s) s->shutdown();
        for (auto& s : subscribers) if (s) s->shutdown();

        std::this_thread::sleep_for(200ms);

        clients.clear();
        subscribers.clear();
        servers.clear();
        proxies.clear();
        zipper.reset();
    }

    /*
        @param client_id: Client/proxy id we want to talk to
        @return: True if successful response from proxy
    */
    bool send_append(NodeId client_id, const string& cmd) {
        if (client_id >= clients.size()) return false;
        return clients[client_id]->append(string_to_command(cmd));
    }

    void wait_for_propagation(int num = 3) {
        auto duration = num * ziplog::EPOCH_DURATION_MS;
        std::this_thread::sleep_for(milliseconds(duration));
    }

    /*
        @brief: verifies the provided vector or commands matches the expected size and entries (as strings)
        @return: True if condition holds, false otherwise.
    */
    void verify_index_matches_expected(vector<Command> output, vector<string> expected) {
        ASSERT_EQ(output.size(), expected.size());
        for (size_t i = 0; i < expected.size(); i++) {
            EXPECT_EQ(command_to_string(output[i]), expected[i]);
        }
    }

    /*
        @brief: Removes skips and expands batched commands
        @return: Expanded log
    */
    vector<vector<Command>> expand_log(const vector<Command> original_log) {
        vector<vector<Command>> log;

        // expand all non-empty entries (may be batches)
        for (const Command& entry : original_log) {
            vector<Command> batch = CommandBatch::deserialize(entry);
            if (!batch.empty()) log.push_back(batch);
        }

        return log;
    }

    ZiplogConfig config;
    std::unique_ptr<Zipper> zipper;
    vector<std::unique_ptr<Client>> clients;    // client 0 to proxy 0 and so on...
    vector<std::unique_ptr<Proxy>> proxies;
    vector<std::unique_ptr<Server>> servers;
    vector<std::unique_ptr<Subscriber>> subscribers;
};

TEST_F(E2ETest, BasicReconfig_ReplaceSubscriber) {
    StartSystem("config/reconfiguration.json");

    // Send some messages through proxy 0 before killing subscriber
    std::thread t1([&]() { ASSERT_TRUE(send_append(0, "before kill 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(0, "before kill 2")); });

    t1.join();
    t2.join();

    wait_for_propagation(2);

    // Count actual commands in expanded log
    auto expanded_log_before = expand_log(subscribers[0]->log());
    int command_count_before = 0;
    for (const auto& batch : expanded_log_before) {
        command_count_before += batch.size();
    }
    ASSERT_EQ(command_count_before, 2);

    // Kill subscriber 0
    std::cout << "[TEST] Killing subscriber 0" << std::endl;
    subscribers[0].reset();

    std::this_thread::sleep_for(200ms);

    // Add new subscriber at a new address
    auto new_sub_config = config;
    string new_ip = "127.0.0.1";
    int new_port = 9000;
    new_sub_config.subscribers[0] = {new_ip, new_port};

    std::cout << "[TEST] Adding new subscriber at " << new_ip << ":" << new_port << std::endl;
    subscribers[0] = std::make_unique<Subscriber>(0, new_sub_config, false);

    wait_for_propagation(3);

    // Send more messages - new subscriber should receive these
    std::thread t3([&]() { ASSERT_TRUE(send_append(0, "after add 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t4([&]() { ASSERT_TRUE(send_append(0, "after add 2")); });

    t3.join();
    t4.join();

    wait_for_propagation(3);

    // Count commands in new subscriber's log
    auto expanded_log_after = expand_log(subscribers[0]->log());
    int command_count_after = 0;
    for (const auto& batch : expanded_log_after) {
        command_count_after += batch.size();
    }
    ASSERT_GE(command_count_after, 2);

    std::cout << "[TEST] New subscriber has " << command_count_after << " commands" << std::endl;
}

TEST_F(E2ETest, BasicReconfig_AddNewProxy) {
    StartSystem("config/reconfiguration.json");

    // Send messages through existing proxy (only proxy 0 exists in config)
    std::thread t1([&]() { ASSERT_TRUE(send_append(0, "proxy 0 msg 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(0, "proxy 0 msg 2")); });

    t1.join();
    t2.join();

    wait_for_propagation(2);

    // Count commands before adding new proxy
    auto expanded_log_before = expand_log(subscribers[0]->log());
    int command_count_before = 0;
    for (const auto& batch : expanded_log_before) {
        command_count_before += batch.size();
    }
    ASSERT_EQ(command_count_before, 2);

    // Add a new proxy dynamically
    string new_proxy_ip = "127.0.0.1";
    int new_proxy_port = 10000;

    auto new_proxy_config = config;
    NodeId new_proxy_id = config.num_proxies();
    new_proxy_config.proxies.push_back({new_proxy_ip, new_proxy_port});

    std::cout << "[TEST] Adding new proxy " << new_proxy_id << " at " << new_proxy_ip << ":" << new_proxy_port << std::endl;

    proxies.push_back(std::make_unique<Proxy>(new_proxy_id, new_proxy_config, false));
    clients.push_back(std::make_unique<Client>(new_proxy_config, new_proxy_id));

    wait_for_propagation(4);

    // Send messages through both proxies
    std::thread t3([&]() {
        std::this_thread::sleep_for(100ms);
        int retries = 0;
        while (!send_append(new_proxy_id, "new proxy msg 1") && retries++ < 10) {
            cout << "[TEST] trying again" << endl;
            std::this_thread::sleep_for(500ms);
        }
    });
    std::thread t4([&]() { ASSERT_TRUE(send_append(0, "proxy 0 msg 3")); });

    t3.join();
    t4.join();

    wait_for_propagation(3);

    // Count total commands
    auto expanded_log_after = expand_log(subscribers[0]->log());
    int command_count_after = 0;
    for (const auto& batch : expanded_log_after) {
        command_count_after += batch.size();
    }
    ASSERT_EQ(command_count_after, 4);

    // Verify the new proxy's message is in the log
    bool found_new_proxy_msg = false;
    for (const auto& batch : expanded_log_after) {
        for (const auto& cmd : batch) {
            if (command_to_string(cmd) == "new proxy msg 1") {
                found_new_proxy_msg = true;
                break;
            }
        }
    }
    ASSERT_TRUE(found_new_proxy_msg) << "New proxy's message not found in log";

    std::cout << "[TEST] New proxy successfully integrated, total commands: " << command_count_after << std::endl;
}
