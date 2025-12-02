#include "zipper.h"
#include "client.h"
#include "proxy.h"
#include "server.h"
#include "subscriber.h"
#include <gtest/gtest.h>

/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     1       1       1       1           setup1.json
    setup2:     2       1       1       1           setup1.json (make clients manually)
    setup3:     2       1       1       2           setup3.json (make clients manually)
    setup4:     2       1       2       2           setup4.json (make clients manually)
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

TEST_F(E2ETest, Setup1_SingleAppend) {
    StartSystem("config/setup1.json");

    // client sends append
    ASSERT_TRUE(send_append(0, "amish donuts"));

    // wait for propagation (3 epochs)
    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 1);

    // verify the desired log entry has the correct contents
    vector<string> expected = {"amish donuts"};
    verify_index_matches_expected(log[0], expected);  // output commands, expected strings
}

TEST_F(E2ETest, Setup1_MultipleAppend) {
    StartSystem("config/setup1.json");

    // send messages from multiple proxies
    std::thread t1([&]() { ASSERT_TRUE(send_append(0, "amish donuts ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t2([&]() { ASSERT_TRUE(send_append(0, "are the ")); });
    std::this_thread::sleep_for(50ms);
    std::thread t3([&]() { ASSERT_TRUE(send_append(0, "best")); });

    t1.join(); t2.join(); t3.join();

    // wait for propagation (3 epochs) then stop system
    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // check log size
    ASSERT_EQ(log.size(), 1);

    // verify batch contents
    vector<string> expected = {"amish donuts ", "are the ", "best"};
    verify_index_matches_expected(log[0], expected);  // output commands, expected strings
}

TEST_F(E2ETest, Setup1_SingleAppendThreeEpochs) {
    StartSystem("config/setup1.json");

    for (int i = 0; i < 3; i++) {
        // client sends append
        ASSERT_TRUE(send_append(0, "amish donuts"));

        // wait for one epoch
        wait_for_propagation(1);
    }

    wait_for_propagation();

    // expand log and remove skips
    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 3);

    // verify the desired log entry has the correct contents
    vector<string> expected = {"amish donuts"};
    for (int i = 0; i < 3; i++) {
        verify_index_matches_expected(log[i], expected);  // output commands, expected strings
    }
}

TEST_F(E2ETest, Setup1_TwoClientsSingleAppend) {
    StartSystem("config/setup1.json");

    //
    std::thread t1([&]() {
        auto c0 = std::make_unique<Client>(config, 0);
        ASSERT_TRUE(c0->append(string_to_command("client 0")));
    });
    std::thread t2([&]() {
        auto c1 = std::make_unique<Client>(config, 0);
        ASSERT_TRUE(c1->append(string_to_command("client 1")));
    });

    t1.join(); t2.join();

    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 1);

    // verify the presence of
    unordered_map<string, bool> expected;
    expected["client 0"] = true;
    expected["client 1"] = true;

    for (const Command& cmd : log[0]) {
        const string& cmd_str = command_to_string(cmd);
        if (expected.count(cmd_str)) expected.erase(cmd_str);
    }

    ASSERT_EQ(expected.empty(), true);
}


TEST_F(E2ETest, Setup1_TwoClientsSingleAppendTwoEpochs) {
    StartSystem("config/setup1.json");

    //
    std::thread t1([&]() {
        auto c0 = std::make_unique<Client>(config, 0);
        ASSERT_TRUE(c0->append(string_to_command("0,0")));
        wait_for_propagation(1);
        ASSERT_TRUE(c0->append(string_to_command("0,1")));
    });
    std::thread t2([&]() {
        auto c1 = std::make_unique<Client>(config, 0);
        ASSERT_TRUE(c1->append(string_to_command("1,0")));
        wait_for_propagation(1);
        ASSERT_TRUE(c1->append(string_to_command("1,1")));
    });

    t1.join(); t2.join();

    wait_for_propagation();

    const auto& original_log = subscribers[0]->log();
    vector<vector<Command>> log = expand_log(original_log);

    // verify log size
    ASSERT_EQ(log.size(), 2);

    // verify the presence of
    unordered_map<string, bool> expected;
    expected["0,0"] = true;
    expected["1,0"] = true;

    for (const Command& cmd : log[0]) {
        const string& cmd_str = command_to_string(cmd);
        if (expected.count(cmd_str)) expected.erase(cmd_str);
    }

    ASSERT_EQ(expected.empty(), true);

    expected["0,1"] = true;
    expected["1,1"] = true;

    for (const Command& cmd : log[1]) {
        const string& cmd_str = command_to_string(cmd);
        if (expected.count(cmd_str)) expected.erase(cmd_str);
    }

    ASSERT_EQ(expected.empty(), true);
}
