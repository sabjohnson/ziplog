#include "test_utils.h"

using namespace ziplog::test;

/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     1       1       1       1           setup1.json
    setup2:     2       1       1       1           setup1.json (make clients manually)
    setup3:     2       1       1       2           setup3.json (make clients manually)
    setup4:     2       1       2       2           setup4.json (make clients manually)
*/

class E2ETest : public ZiplogTestBase
{
    // inherits from setup, tear down and utility functions from import file
};

// 1 client send a request to 1 proxy
TEST_F(E2ETest, Setup1_SingleAppend)
{
    StartSystem("config/setup1.json");

    // client sends append
    ASSERT_TRUE(send_append(0, "amish donuts"));

    // wait for propagation (3 epochs)
    // wait_for_propagation();
    subscribers[0]->wait_for_log_size(1);

    // verify the desired log entry has the correct contents
    vector<Command> output = subscribers[0]->log().expand_unraveled();
    vector<string> expected = {"amish donuts"};
    verify_index_matches_expected(output, expected); // output commands, expected strings
}

// 3 clients concurrently sending a request to 1 proxy.
TEST_F(E2ETest, Setup1_MultipleAppend)
{
    StartSystem("config/setup1.json");

    // send messages from multiple clients
    std::thread t1([&]()
                   {
        auto c0 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c0->append(string_to_command("amish donuts "))); });

    std::thread t2([&]()
                   {
        auto c0 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c0->append(string_to_command("are the "))); });

    std::thread t3([&]()
                   {
        auto c0 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c0->append(string_to_command("best"))); });

    t1.join();
    t2.join();
    t3.join();

    // wait for propagation (3 epochs) then stop system
    wait_for_propagation();

    // verify batch contents
    vector<Command> output = subscribers[0]->log().expand_unraveled();
    vector<string> expected = {"amish donuts ", "are the ", "best"};
    verify_elements_match_expected(output, expected); // output commands, expected strings
}

TEST_F(E2ETest, Setup1_SingleAppendThreeEpochs)
{
    StartSystem("config/setup1.json");

    for (int i = 0; i < 3; i++)
    {
        // client sends append
        ASSERT_TRUE(send_append(0, "amish donuts"));

        // wait for one epoch
        wait_for_propagation(1);
    }

    wait_for_propagation();

    // verify the desired log entry has the correct contents
    vector<Command> output = subscribers[0]->log().expand_unraveled();
    vector<string> expected = {"amish donuts", "amish donuts", "amish donuts"};
    verify_elements_match_expected(output, expected);
}

TEST_F(E2ETest, Setup1_TwoClientsSingleAppend)
{
    StartSystem("config/setup1.json");

    //
    std::thread t1([&]()
                   {
        auto c0 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c0->append(string_to_command("client 0"))); });
    std::thread t2([&]()
                   {
        auto c1 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c1->append(string_to_command("client 1"))); });

    t1.join();
    t2.join();

    wait_for_propagation();

    vector<Command> output = subscribers[0]->log().expand_unraveled();
    vector<string> expected = {"client 0", "client 1"};
    verify_elements_match_expected(output, expected);
}

TEST_F(E2ETest, Setup1_TwoClientsSingleAppendTwoEpochs)
{
    StartSystem("config/setup1.json");

    //
    std::thread t1([&]()
                   {
        auto c0 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c0->append(string_to_command("0,0")));
        wait_for_propagation(1);
        ASSERT_TRUE(c0->append(string_to_command("0,1"))); });
    std::thread t2([&]()
                   {
        auto c1 = std::make_unique<Client>(config.proxies[0]);
        ASSERT_TRUE(c1->append(string_to_command("1,0")));
        wait_for_propagation(1);
        ASSERT_TRUE(c1->append(string_to_command("1,1"))); });

    t1.join();
    t2.join();

    wait_for_propagation();

    vector<vector<Command>> log = subscribers[0]->log().expand();

    // verify log size
    ASSERT_EQ(log.size(), 2);

    // verify the presence of
    unordered_map<string, bool> expected;
    expected["0,0"] = true;
    expected["1,0"] = true;

    for (const Command &cmd : log[0])
    {
        const string &cmd_str = command_to_string(cmd);
        if (expected.count(cmd_str))
            expected.erase(cmd_str);
    }

    ASSERT_EQ(expected.empty(), true);

    expected["0,1"] = true;
    expected["1,1"] = true;

    for (const Command &cmd : log[1])
    {
        const string &cmd_str = command_to_string(cmd);
        if (expected.count(cmd_str))
            expected.erase(cmd_str);
    }

    ASSERT_EQ(expected.empty(), true);
}
