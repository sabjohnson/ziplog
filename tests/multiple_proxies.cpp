#include "test_utils.h"

using namespace ziplog::test;
/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     3       3       3       3           setup1.json
*/

class E2ETest : public ZiplogTestBase {
    // inherits from setup, tear down and utility functions from import file
};

TEST_F(E2ETest, Multiple_SingleAppend) {
    StartSystem("config/servers.json");

    // client sends append
    std::thread t1([&]() { ASSERT_TRUE(send_append(0, "amish donuts ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(1, "are the ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() { ASSERT_TRUE(send_append(2, "best")); });

    t1.join(); t2.join(); t3.join();

    // wait for propagation (3 epochs)
    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 3);

    // verify the desired log entry has the correct contents
    vector<vector<string>> expected = {{"amish donuts "}, {"are the "}, {"best"}};
    for (int i = 0; i < 3; i++) {
        verify_index_matches_expected(log[i], expected[i]);  // output commands, expected strings
    }
}

// currently client simply unable to make requests, shoulf function sa normal
TEST_F(E2ETest, Multiple_SingleAppendKillOneProxy) {
    StartSystem("config/servers.json");
    proxies[0].reset();

    // client sends append
    std::thread t1([&]() {

        if (send_append(0, "amish donuts ")) {
            std::cout << "wabi" << std::endl;
        } else {
            std::cout << "sabi" << std::endl;
        }
    });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() {
        for (int i = 0; i < 10; i++) {
            if (send_append(1, "are the ")) {
                ASSERT_TRUE(true);
                return;
            }
        }
        ASSERT_TRUE(false);
    });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() {
        for (int i = 0; i < 10; i++) {
            if (send_append(2, "best")) {
                ASSERT_TRUE(true);
                return;
            }
        }
        ASSERT_TRUE(false);
    });

    t1.join(); t2.join(); t3.join();

    // wait for propagation (3 epochs)
    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 2);

    // verify the desired log entry has the correct contents
    vector<vector<string>> expected = {{"are the "}, {"best"}};
    for (int i = 0; i < 2; i++) {
        verify_index_matches_expected(log[i], expected[i]);  // output commands, expected strings
    }
}

// currently client simply unable to make requests, shoulf function sa normal
TEST_F(E2ETest, Multiple_SingleAppendKillOneProxyAfterZipperRequest) {
    StartSystem("config/servers.json");

    // kill proxy 0 (we will simulate its messages)
    proxies[0].reset();

    // client sends append (client 0 to proxy 0, client 1 to proxy 1 and so on...)
    std::thread t1([&]() { ASSERT_FALSE(send_append(0, "amish donuts ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() {
        for (int i = 0; i < 10; i++) {
            if (send_append(1, "are the ")) {
                ASSERT_TRUE(true);
                return;
            }
        }
        ASSERT_TRUE(false);
    });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() {
        for (int i = 0; i < 10; i++) {
            if (send_append(2, "best")) {
                ASSERT_TRUE(true);
                return;
            }
        }
        ASSERT_TRUE(false);
    });

    t1.join(); t2.join(); t3.join();

    // wait for propagation (3 epochs)
    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 2);

    // verify contents
    vector<vector<string>> expected = {{"are the "}, {"best"}};
    for (size_t i = 0; i < 2; i++) {
        verify_index_matches_expected(log[i], expected[i]);
    }
}

TEST_F(E2ETest, Multiple_PartialReplication_OneServerGetsRequest) {
    StartSystem("config/servers.json");

    std::thread simulator([&]() {
        // kill proxy 0 immediately
        proxies[0].reset();

        // simulate zip request of 1 client request to proxy 0
        std::this_thread::sleep_for(100ms);  // Let system start

        auto [zipper_ip, zipper_port] = config.zipper;
        int zipper_sock = NetworkUtils::connect_to_address_persistent(zipper_ip, zipper_port);
        if (zipper_sock < 0) {
            std::cout << "[TEST] Failed to connect to zipper" << std::endl;
            ASSERT_TRUE(false);
            return;
        }

        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = 0;
        zip_req.sender_id = 0;  // Proxy 0
        zip_req.set_num_requests(1);  // Request 1 slot

        if (NetworkUtils::send_message(zipper_sock, zip_req)) {
            // wait for zipper to allocate sequences
            std::this_thread::sleep_for(1500ms);

            // we know it will be sequence 1 in the first epoch
            SequenceNumber allocated_seq = 1;

            // manually send APPEND to only server 0
            Message msg;
            msg.type = APPEND;
            msg.shard_id = 0;
            msg.sender_id = 0;  // From proxy 0
            msg.set_sequence_number(allocated_seq);

            CommandBatch batch;
            batch.add_command(string_to_command("simulated client 0"));
            msg.data = batch.serialize();

            auto [server_ip, server_port] = config.servers[0];
            Message ack;
            NetworkUtils::send_message_to_address(server_ip, server_port, msg, ack, config.max_retries);

            std::cout << "[TEST] Sent message to server 0 only with seq " << allocated_seq << std::endl;
        } else {
            std::cout << "[TEST] Failed to send to zipper" << std::endl;
            ASSERT_TRUE(false);
        }
    });


    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(1, "client 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() { ASSERT_TRUE(send_append(2, "client 2")); });

    t2.join();
    t3.join();
    simulator.join();

    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    ASSERT_EQ(log.size(), 3);

    vector<vector<string>> expected = {{"simulated client 0"}, {"client 1"}, {"client 2"}};
    for (size_t i = 0; i < 3; i++) {
        verify_index_matches_expected(log[i], expected[i]);
    }
}

TEST_F(E2ETest, Multiple_StressAppend) {
    StartSystem("config/performance.json");

    auto start = std::chrono::high_resolution_clock::now();

    // client sends append
    std::thread t1([&]() {
        for (int i = 0; i < 100; i++) ASSERT_TRUE(send_append(0, "client 0 - " + std::to_string(i)));
    });
    std::thread t2([&]() {
        for (int i = 0; i < 100; i++) ASSERT_TRUE(send_append(1, "client 1 - " + std::to_string(i)));
    });
    std::thread t3([&]() {
        for (int i = 0; i < 100; i++) ASSERT_TRUE(send_append(2, "client 2 - " + std::to_string(i)));
    });

    t1.join(); t2.join(); t3.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    cout << "[PERF] Test completed in " << duration.count() << " ms" << endl;
    cout << "[PERF] Throughput: " << (300.0 / (duration.count() / 1000.0)) << " ops/sec" << endl;

    // wait for propagation (3 epochs)
    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();
    cout << "[PERF] actual log size = " << original_log.size() << endl;
    vector<vector<Command>> log = expand_log(original_log);
    ASSERT_EQ(log.size(), 300);
}
/*
TEST_F(E2ETest, Multiple_StressAppend3ClientsOneProxy) {
    StartSystem("config/performance.json");

    auto start = std::chrono::high_resolution_clock::now();

    // client sends append
    std::thread t1([&]() {
        Client client(config, 0);
        for (int i = 0; i < 100; i++) ASSERT_TRUE(client.append(string_to_command("client 0 - " + std::to_string(i))));
    });
    std::thread t2([&]() {
        Client client(config, 0);
        for (int i = 0; i < 100; i++) ASSERT_TRUE(client.append(string_to_command("client 1 - " + std::to_string(i))));
    });
    std::thread t3([&]() {
        Client client(config, 0);
        for (int i = 0; i < 100; i++) ASSERT_TRUE(client.append(string_to_command("client 2 - " + std::to_string(i))));
    });

    t1.join(); t2.join(); t3.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    cout << "[PERF] Test completed in " << duration.count() << " ms" << endl;
    cout << "[PERF] Throughput: " << (300.0 / (duration.count() / 1000.0)) << " ops/sec" << endl;

    // wait for propagation (3 epochs)
    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();

    size_t total_commands = 0;
    for (const Command& entry : original_log) {
        vector<Command> batch = CommandBatch::deserialize(entry);
        total_commands += batch.size();
    }
    cout << "[PERF] actual log size = " << original_log.size() << endl;
    cout << "[PERF] total processed commands = " << total_commands << endl;

    ASSERT_EQ(total_commands, 300);
}
*/