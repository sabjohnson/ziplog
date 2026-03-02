/*
#include "test_utils.h"

using namespace ziplog::test;

*/
/*
    * all tests are using one shard *
    number of   clients proxies servers subscribers config_file
    setup1:     1       1       2       1           setup1.json
*/
/*


class E2ETest : public ZiplogTestBase {
  // inherits from setup, tear down and utility functions from import file
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
  string new_ip = "127.0.0.1";
  int new_port = 9000;

  // Add new subscriber at a new address
  SubscriberConfig new_sub_config;
  new_sub_config.zipper = config.zipper;
  new_sub_config.address = Address(new_ip, new_port);
  new_sub_config.f = config.f;
  new_sub_config.max_retries = config.max_retries;
  new_sub_config.servers = config.servers;

  std::cout << "[TEST] Adding new subscriber at " << new_ip << ":" << new_port << std::endl;
  subscribers[0] = std::make_unique<Subscriber>(new_sub_config, false);

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
  ASSERT_GE(command_count_after, 4);

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
  int new_proxy_id = config.num_proxies();

  ProxyConfig new_proxy_config;
  new_proxy_config.id = new_proxy_id;
  new_proxy_config.shard = 0;
  new_proxy_config.address = Address(new_proxy_ip, new_proxy_port);
  new_proxy_config.f = config.f;
  new_proxy_config.max_retries = config.max_retries;
  new_proxy_config.max_epoch_history = config.max_epoch_history;
  new_proxy_config.epoch_duration_ms = config.epoch_duration_ms;
  new_proxy_config.zipper = config.zipper;
  new_proxy_config.servers = config.servers;

  std::cout << "[TEST] Adding new proxy " << new_proxy_id << " at " << new_proxy_ip << ":" << new_proxy_port << std::endl;

  proxies.push_back(std::make_unique<Proxy>(new_proxy_config, false));
  config.proxies.push_back(Address(new_proxy_ip, new_proxy_port));
  clients.push_back(std::make_unique<Client>(config.proxies[new_proxy_id]));

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
*/
