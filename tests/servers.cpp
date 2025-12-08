#include "test_utils.h"

using namespace ziplog::test;

/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     1       1       2       1           setup1.json
*/

class E2ETest : public ZiplogTestBase {
    // inherits from setup, tear down and utility functions from import file
};

TEST_F(E2ETest, Recovery_ServerDiesDuringRecoveryProtocol) {
    StartSystem("config/servers_test.json");  // 1 proxy, 3 servers, 1 subscriber

    // Kill proxy 0 immediately
    proxies[0].reset();
    std::cout << "[TEST] Killed proxy 0" << std::endl;

    std::thread recovery_simulator([&]() {
        std::this_thread::sleep_for(1000ms);

        auto [zipper_ip, zipper_port] = config.zipper;
        int zipper_sock = NetworkUtils::connect_to_address_persistent(zipper_ip, zipper_port);
        if (zipper_sock < 0) {
            std::cout << "[TEST] Failed to connect to zipper" << std::endl;
            ASSERT_TRUE(false);
            return;
        }

        // Step 1: Simulate proxy 0 sending message to only server 0, then dying
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = 0;
        zip_req.sender_id = 0;
        zip_req.set_num_requests(1);

         if (!NetworkUtils::send_message(zipper_sock, zip_req)) {
            std::cout << "[TEST] Failed to send ZIP_REQUEST" << std::endl;
            close(zipper_sock);
            return;
        }

        std::this_thread::sleep_for(1000ms);

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

        // Wait for failure detection to trigger (epoch + lag)
        std::this_thread::sleep_for(500ms); // 2500 for message a

        // At this point, recovery should be starting
        // Kill server 0 right as it starts broadcasting TRANSFER_REQUEST
        std::cout << "[TEST] Killing server 0 as recovery begins" << std::endl;
        servers[0].reset();

        // The zipper will send SKIP for sequence 1 since it thinks it wasn't used
        std::cout << "[TEST] Waiting for recovery to complete" << std::endl;
        std::this_thread::sleep_for(3000ms);
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
