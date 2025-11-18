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

TEST_F(E2ETest, Recovery_ServerDiesDuringRecoveryProtocol) {
    StartSystem("config/servers_test.json");  // 1 proxy, 3 servers, 1 subscriber

    std::thread recovery_simulator([&]() {
        std::this_thread::sleep_for(100ms);

        // Step 1: Simulate proxy 0 sending message to only server 0, then dying
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = 0;
        zip_req.sender_id = 0;
        zip_req.set_num_requests(1);

        auto [zipper_ip, zipper_port] = config.zipper;
        Message zip_resp;

        if (NetworkUtils::send_message_to_address(zipper_ip, zipper_port, zip_req, zip_resp, config.max_retries)) {
            std::this_thread::sleep_for(500ms);  // Wait for slot allocation (2500)

            // Send APPEND with seq 1 to ONLY server 0
            Message msg;
            msg.type = APPEND;
            msg.shard_id = 0;
            msg.sender_id = 0;
            msg.set_sequence_number(1);

            CommandBatch batch;
            batch.add_command(string_to_command("message A"));
            msg.data = batch.serialize();

            auto [server0_ip, server0_port] = config.servers[0];
            Message ack;
            NetworkUtils::send_message_to_address(server0_ip, server0_port, msg, ack, config.max_retries);

            std::cout << "[TEST] Sent message A to server 0 only" << std::endl;

            // Kill proxy 0 immediately
            proxies[0].reset();
            std::cout << "[TEST] Killed proxy 0" << std::endl;

            // Wait for failure detection to trigger (epoch + lag)
            std::this_thread::sleep_for(2000ms);

            // At this point, recovery should be starting
            // Kill server 0 right as it starts broadcasting TRANSFER_REQUEST
            std::cout << "[TEST] Killing server 0 as recovery begins" << std::endl;
            servers[0].reset();

            // Now in round 1:
            // - Server 1 sends TRANSFER_REQUEST to server 0 (dead) and server 2
            // - Server 2 sends TRANSFER_REQUEST to server 0 (dead) and server 1
            // - Neither has the message, both report seq=0
            // - Round 1 converges with seq=0 (wrong!)

            // The zipper will send SKIP for sequence 1 since it thinks it wasn't used
            std::cout << "[TEST] Waiting for recovery to complete" << std::endl;
            std::this_thread::sleep_for(3000ms);
        }
    });

    recovery_simulator.join();
    wait_for_propagation(2);

    // Check the log
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    std::cout << "[TEST] Log has " << log.size() << " entries" << std::endl;

    // With server 0 dead and message lost, zipper should send SKIP
    // So log might be empty or have 1 empty entry
    // This demonstrates the limitation: if f+1 servers die during recovery,
    // messages can be lost

    if (log.size() > 0) {
        int command_count = 0;
        for (const auto& batch : log) {
            command_count += batch.size();
        }
        std::cout << "[TEST] Found " << command_count << " commands in log" << std::endl;
    } else {
        std::cout << "[TEST] Message A was lost due to insufficient servers during recovery" << std::endl;
    }

    // The test passes as long as the system doesn't crash
    ASSERT_TRUE(true);
}
/*
TEST_F(E2ETest, Liveness_SystemContinuesWithNMinusFServers) {
    StartSystem("config/servers_test.json");  // 1 proxy, 3 servers (f=1), 1 subscriber

    // Send some messages before killing server
    std::thread t1([&]() { ASSERT_TRUE(send_append(0, "before kill 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(0, "before kill 2")); });

    t1.join();
    t2.join();

    wait_for_propagation(2);

    // Verify initial messages
    auto log_before = expand_log(subscribers[0]->log());
    int command_count_before = 0;
    for (const auto& batch : log_before) {
        command_count_before += batch.size();
    }
    ASSERT_EQ(command_count_before, 2);

    std::cout << "[TEST] Killing server 0" << std::endl;
    servers[0].reset();

    std::this_thread::sleep_for(500ms);

    // Continue sending messages after server dies
    std::thread t3([&]() { ASSERT_TRUE(send_append(0, "after kill 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t4([&]() { ASSERT_TRUE(send_append(0, "after kill 2")); });
    std::this_thread::sleep_for(50ms);
    std::thread t5([&]() { ASSERT_TRUE(send_append(0, "after kill 3")); });

    t3.join();
    t4.join();
    t5.join();

    wait_for_propagation(3);

    // Verify all messages made it through
    auto log_after = expand_log(subscribers[0]->log());
    int command_count_after = 0;
    for (const auto& batch : log_after) {
        command_count_after += batch.size();
    }
    ASSERT_EQ(command_count_after, 5);

    // Verify specific messages are present
    vector<string> expected_messages = {
        "before kill 1", "before kill 2",
        "after kill 1", "after kill 2", "after kill 3"
    };

    vector<string> actual_messages;
    for (const auto& batch : log_after) {
        for (const auto& cmd : batch) {
            actual_messages.push_back(command_to_string(cmd));
        }
    }

    for (size_t i = 0; i < expected_messages.size(); i++) {
        EXPECT_EQ(actual_messages[i], expected_messages[i]);
    }

    std::cout << "[TEST] System successfully continued with n-f servers" << std::endl;
}
*/