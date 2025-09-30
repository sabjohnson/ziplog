#include "subscriber.h"
#include "config.h"
#include <iostream>
#include <vector>
#include <cassert>

using namespace ziplog::api;
using namespace ziplog::impl;

class SubscriberTester {
private:
    Subscriber* subscriber;
    ziplogConfig config;

public:
    SubscriberTester() {
        // minimal config for testing
        config.f = 1;  // need 2 servers for quorum
        config.max_retries = 3;
        config.timeout_ms = 1000;

        // add subscriber address (won't actually bind in test)
        config.subscribers.push_back({"127.0.0.1", 50000});

        // attempt to create subscriber
        try {
            subscriber = new Subscriber(0, config);
        } catch (...) {
            subscriber = nullptr;
        }
    }

    // simulate receiving a message from a server
    void simulateMessage(uint32_t seq_num, const string& data, int server_id) {
        message msg;
        msg.type = APPEND;
        msg.sender_id = server_id;
        msg.sequence_number = seq_num;
        msg.data = data;
        msg.shard_id = 0;

        if (subscriber) {
            subscriber->processForQuorum(msg);
        }
    }

    // create a basic subscriber for testing without network
    void createTestSubscriber() {
        if (!subscriber) {
            subscriber = new Subscriber();
            subscriber->id = 0;
            subscriber->config = config;
            subscriber->next_seq = 0;
        }
    }

    void runOutOfOrderTest() {
        std::cout << "=== Testing Out-of-Order Message Delivery ===" << std::endl;

        if (!subscriber) {
            std::cout << "Creating test subscriber..." << std::endl;
            return;
        }

        // simulate messages arriving out of order: 0, 2, 4, 3, 1
        std::vector<std::pair<uint32_t, string>> messages = {
            {0, "First message"},
            {2, "Third message"},
            {4, "Fifth message"},
            {3, "Fourth message"},
            {1, "Second message"}
        };

        for (auto& [seq, data] : messages) {
            std::cout << "\nSending message seq=" << seq << ": " << data << std::endl;

            // send from multiple servers to achieve quorum (f+1 = 2 servers)
            simulateMessage(seq, data, 0);  // Server 0
            simulateMessage(seq, data, 1);  // Server 1

            std::cout << "Current log size: " << subscriber->log.size() << std::endl;
            std::cout << "Pending gaps: " << subscriber->gaps.size() << std::endl;
        }

        // Verify final log order (shutdown will print it for now)
        std::cout << "\n=== Final Log Contents ===" << std::endl;
//        for (size_t i = 0; i < subscriber->log.size(); i++) {
//            std::cout << "Index " << i << ": " << subscriber->log[i] << std::endl;
//        }

        // Assert correct order
        assert(subscriber->log.size() == 5);
        assert(subscriber->log[0] == "First message");
        assert(subscriber->log[1] == "Second message");
        assert(subscriber->log[2] == "Third message");
        assert(subscriber->log[3] == "Fourth message");
        assert(subscriber->log[4] == "Fifth message");

        std::cout << "\nMessages correctly reordered..... PASSED" << std::endl;
    }

    ~SubscriberTester() {
        delete subscriber;
    }
};

// directly test the applyOperation logic
class DirectSubscriberTest {
private:
    std::vector<string> log;
    std::map<uint32_t, string> gaps;
    uint32_t next_seq = 0;

public:
    // copy and paste for now
    void applyOperation(uint32_t seq_num, const string& data) {
        gaps[seq_num] = data;

        while (gaps.count(next_seq)) {
            log.push_back(gaps[next_seq]);
            gaps.erase(next_seq);
            next_seq++;
        }

        std::cout << "APPLIED message " << seq_num << " (log size: " << log.size()
                  << ", gaps: " << gaps.size() << ")" << std::endl;
    }

    void runTest() {
        std::cout << "=== Direct Logic Test ===" << std::endl;

        // test out-of-order delivery: 0, 3, 1, 4, 2, 6, 5
        std::vector<std::pair<uint32_t, string>> test_data = {
            {0, "msg0"}, {3, "msg3"}, {1, "msg1"}, {4, "msg4"},
            {2, "msg2"}, {6, "msg6"}, {5, "msg5"}
        };

        for (auto& [seq, data] : test_data) {
            std::cout << "\nProcessing seq " << seq << ": " << data << std::endl;
            applyOperation(seq, data);

            std::cout << "Log: [";
            for (size_t i = 0; i < log.size(); i++) {
                std::cout << log[i];
                if (i < log.size() - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }

        // verify final state
        assert(log.size() == 7);  // should be 0,1,2,3,4,5,6 (5 total)
        assert(gaps.size() == 0); // should be no gaps
        assert(next_seq == 7);    // waiting for sequence 7

        std::cout << "\nDirect logic test ......PASSED" << std::endl;
    }
};

int main() {
    try {
        DirectSubscriberTest direct_test;
        direct_test.runTest();

        // figure our how to test w/ networking
//      SubscriberTester full_test;
//      full_test.runOutOfOrderTest();

    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}