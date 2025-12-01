#include "zipper.h"
#include "client.h"
#include "proxy.h"
#include "server.h"
#include "subscriber.h"
#include <gtest/gtest.h>

/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     3       3       3       3           setup1.json
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
            clients.push_back(std::make_unique<Client>(config, i));
        }
    }

    void TearDown() override {

        // Stop everything gracefully first
        for (auto& p : proxies) if (p) p->shutdown();
        cout << "[TEST] proxies torn down" << endl;
        for (auto& s : servers) if (s) s->shutdown();
        cout << "[TEST] servers torn down" << endl;
        for (auto& s : subscribers) if (s) s->shutdown();
        cout << "[TEST] subscribers torn down" << endl;

        std::this_thread::sleep_for(200ms);

        clients.clear();
        cout << "[TEST] clients cleared" << endl;
        subscribers.clear();
        cout << "[TEST] subscribers cleared" << endl;
        servers.clear();
        cout << "[TEST] servers cleared" << endl;
        proxies.clear();
        cout << "[TEST] proxies cleared" << endl;
        zipper.reset();
        cout << "[TEST] zipper cleared" << endl;
        cout << "[TEST] tear down complete" << endl;
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
/*
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
    proxies[0]->shutdown();

    // client sends append
    std::thread t1([&]() { send_append(0, "amish donuts "); });
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
    ASSERT_EQ(log.size(), 2);

    // verify the desired log entry has the correct contents
    vector<vector<string>> expected = {{"are the "}, {"best"}};
    for (int i = 0; i < 2; i++) {
        verify_index_matches_expected(log[i], expected[i]);  // output commands, expected strings
    }
}

TEST_F(E2ETest, Multiple_SingleAppendKillOneProxyAfterZipperRequest) {
    StartSystem("config/servers.json");
    std::cout << "sizeof(Server) = " << sizeof(Server) << std::endl;
    std::cout << "sizeof(ServerConfig) = " << sizeof(ServerConfig) << std::endl;
    std::cout << "sizeof(BaseNode<ServerConfig>) = " << sizeof(BaseNode<ServerConfig>) << std::endl;
    std::cout << "sizeof(Proxy) = " << sizeof(Proxy) << std::endl;
    std::cout << "sizeof(ProxyConfig) = " << sizeof(ProxyConfig) << std::endl;
    std::cout << "sizeof(BaseNode<ProxyConfig>) = " << sizeof(BaseNode<ProxyConfig>) << std::endl;
    std::cout << "sizeof(mutex) = " << sizeof(std::mutex) << std::endl;
    std::cout << "sizeof(condition_variable) = " << sizeof(std::condition_variable) << std::endl;
    std::cout << "sizeof(thread) = " << sizeof(std::thread) << std::endl;
    std::cout << "sizeof(atomic<bool>) = " << sizeof(std::atomic<bool>) << std::endl;

    // Thread t1 expects failure (proxy will be killed)
    atomic<bool> t1_completed{false};
    std::thread t1([&]() {
        cout << "[TEST] started t1" << endl;
        bool result = send_append(0, "amish donuts ");
        cout << "[TEST] after send append" << endl;
        t1_completed = true;
        // Expect false since proxy gets killed
        EXPECT_FALSE(result);
        cout << "[TEST] after expect false" << endl;
    });

    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(1, "are the ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() { ASSERT_TRUE(send_append(2, "best")); });

    std::thread killer([&]() {
        cout << "[TEST] before shutdown" << endl;
        std::this_thread::sleep_for(1100ms);
        proxies[0]->shutdown();  // Shutdown gracefully first
        cout << "[TEST] before deconstructor" << endl;
        std::this_thread::sleep_for(100ms);
        proxies[0].reset();
        cout << "[TEST] after deconstructor" << endl;
    });

    // Join with timeout
    t2.join();
    t3.join();
    killer.join();

    // Wait for t1 with timeout
    auto start = std::chrono::steady_clock::now();
    while (!t1_completed) {
        std::this_thread::sleep_for(100ms);
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            std::cout << "[TEST] WARNING: t1 timed out, proxy may not have closed connection properly" << std::endl;
            break;
        }
    }

    cout << "[TEST] before t1.join()" << endl;
    if (t1.joinable()) t1.join();
    cout << "[TEST] after t1.join()" << endl;

    wait_for_propagation();

    // Rest of test...
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    ASSERT_EQ(log.size(), 2);

    vector<vector<string>> expected = {{"are the "}, {"best"}};
    for (size_t i = 0; i < 2; i++) {
        verify_index_matches_expected(log[i], expected[i]);
    }
    cout << "[TEST] shutting down" << endl;
}

TEST_F(E2ETest, Multiple_PartialReplication_OneServerGetsRequest) {
    StartSystem("config/servers.json");

    std::thread killer([&]() {
        // kill proxy 0 immediately
        proxies[0].reset();

        // simulate zip request of 1 client request to proxy 0
        std::this_thread::sleep_for(100ms);  // Let system start

        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = 0;
        zip_req.sender_id = 0;  // Proxy 0
        zip_req.set_num_requests(1);  // Request 1 slot

        auto [zipper_ip, zipper_port] = config.zipper;
        Message zip_resp;

        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, zipper_ip, zipper_port)) {
            if (NetworkUtils::send_message(sock, zip_req)) {
                // wait for zipper to allocate sequences
                std::this_thread::sleep_for(1000ms);

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
            }
        }
    });


    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(1, "client 1")); });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() { ASSERT_TRUE(send_append(2, "client 2")); });

    t2.join();
    t3.join();
    killer.join();

    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    ASSERT_EQ(log.size(), 3);

    vector<vector<string>> expected = {{"simulated client 0"}, {"client 1"}, {"client 2"}};
    for (size_t i = 0; i < 3; i++) {
        verify_index_matches_expected(log[i], expected[i]);
    }
}
/*
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