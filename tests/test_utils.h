#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include "zipper/zipper.h"
#include "client/client.h"
#include "proxy/proxy.h"
#include "server/server.h"
#include "subscriber/subscriber.h"
#include <gtest/gtest.h>

using namespace ziplog::api;
using namespace ziplog::impl;

namespace ziplog::test
{

    /**
     * @brief Waits for epoch propagation
     * @param num Number of epochs to wait for
     */
    inline void wait_for_propagation(int num = 3)
    {
        auto duration = num * ziplog::EPOCH_DURATION_MS;
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    }

    /**
     * @brief Verifies the provided vector of commands matches the expected size and entries
     * @param output The actual commands from the log
     * @param expected The expected command strings
     */
    inline void verify_index_matches_expected(const std::vector<Command> &output,
                                              const std::vector<std::string> &expected)
    {
        ASSERT_EQ(output.size(), expected.size());
        for (size_t i = 0; i < expected.size(); i++)
        {
            EXPECT_EQ(command_to_string(output[i]), expected[i]);
        }
    }

    inline void verify_elements_match_expected(const std::vector<Command> &output,
                                               const std::vector<std::string> &expected)
    {
        ASSERT_EQ(output.size(), expected.size());

        std::multiset<std::string> output_set;
        for (const auto &cmd : output)
        {
            output_set.insert(command_to_string(cmd));
        }

        std::multiset<std::string> expected_set(expected.begin(), expected.end());

        EXPECT_EQ(output_set, expected_set);
    }

    inline void verify_elements_match_expected_nested(const std::vector<std::vector<Command>> &output,
                                                      const std::vector<std::vector<std::string>> &expected)
    {
        std::multiset<std::string> output_set;
        std::multiset<std::string> expected_set;

        for (const auto &inner : output)
        {
            for (const auto &cmd : inner)
            {
                output_set.insert(command_to_string(cmd));
            }
        }

        for (const auto &inner : expected)
        {
            for (const auto &s : inner)
            {
                expected_set.insert(s);
            }
        }

        EXPECT_EQ(output_set, expected_set);
    }

    /**
     * @brief Removes skips and expands batched commands
     * @param original_log The raw log with possible batches and skips
     * @return Expanded log as a vector of batches
     */
    inline std::vector<std::vector<Command>> expand_log(const std::vector<Command> &original_log)
    {
        std::vector<std::vector<Command>> log;

        // expand all non-empty entries (may be batches)
        for (const Command &entry : original_log)
        {
            std::vector<Command> batch = CommandBatch::deserialize(entry);
            if (!batch.empty())
            {
                log.push_back(batch);
            }
        }

        return log;
    }

    /**
     * @brief Base test fixture with common setup/teardown and utilities
     */
    class ZiplogTestBase : public ::testing::Test
    {
    protected:
        void StartSystem(const std::string &filename)
        {
            // load test config
            config = parse_config(filename);

            // start all components
            zipper = std::make_unique<Zipper>(config.make_zipper_config());
            zipper->wait_until_listening();

            for (size_t i = 0; i < config.num_subscribers(); i++)
            {
                subscribers.push_back(std::make_unique<Subscriber>(config.make_subscriber_config(i)));
            }
            for (auto &subscriber : subscribers)
            {
                subscriber->wait_until_listening();
            }

            for (size_t i = 0; i < config.num_servers(); i++)
            {
                servers.push_back(std::make_unique<Server>(config.make_server_config(i)));
            }
            for (auto &server : servers)
            {
                server->wait_until_listening();
            }

            for (size_t i = 0; i < config.num_proxies(); i++)
            {
                proxies.push_back(std::make_unique<Proxy>(config.make_proxy_config(i)));
            }
            for (auto &proxy : proxies)
            {
                proxy->wait_until_listening();
            }

            for (size_t i = 0; i < config.num_proxies(); i++)
            {
                clients.push_back(std::make_unique<Client>(config.proxies[i]));
            }
        }

        void StartSystem_sleeps(const std::string &filename)
        {
            // load test config
            config = parse_config(filename);

            // start all components
            zipper = std::make_unique<Zipper>(config.make_zipper_config());
            std::this_thread::sleep_for(100ms);

            for (size_t i = 0; i < config.num_subscribers(); i++)
            {
                subscribers.push_back(std::make_unique<Subscriber>(config.make_subscriber_config(i)));
            }
            std::this_thread::sleep_for(100ms);

            for (size_t i = 0; i < config.num_servers(); i++)
            {
                servers.push_back(std::make_unique<Server>(config.make_server_config(i)));
            }
            std::this_thread::sleep_for(100ms);

            for (size_t i = 0; i < config.num_proxies(); i++)
            {
                proxies.push_back(std::make_unique<Proxy>(config.make_proxy_config(i)));
            }

            for (size_t i = 0; i < config.num_proxies(); i++)
            {
                clients.push_back(std::make_unique<Client>(config.proxies[i]));
            }
        }

        void TearDown() override
        {
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
        bool send_append(NodeId client_id, const std::string &cmd)
        {
            if (client_id >= clients.size())
            {
                cout << "client size " << client_id << " >= " << clients.size() << endl;
                return false;
            }
            cout << "send append cmd= " << cmd << endl;
            if (clients[client_id]->append(string_to_command(cmd)))
                return true;
            false_return();
            return false;
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