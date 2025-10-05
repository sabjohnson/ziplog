# Ziplog Implementation

Project uses c++17, Make and an [external library](https://github.com/nlohmann/json) for JSON parsing.

## Basic Compilation Commands
`make` to create executable "ziplog".
`make clean` to clean up executable and respective object files (in obj/ directory).
`make external_clean` to clean up external json library (in /third_party).
`make test_build` to build test target "test_runner".
`make test` to build and run test target "test_runner".

## Running the Program
**Option 1**
interactive testing for now (ex. starting client/server/subscriber in terminal and observing their behavior).
```
./ziplog client/zipper/proxy/server/subscriber configuration_filename index_in_configuration (optional for zipper)
# examples
# ./ziplog client config/setup1.json 2
# ./ziplog zipper config/servers.json
```
**Option 2**
[End-to-end](#testing) testing via google test suite.

## Current Functionality
- Configuration file/string parsing from JSON format (provides customizable 'f', max retries, connection timeouts).
- Serializable message structs.
- Networking utilities for sending messages with a 2 byte header.

## Zipper
Operate based on epochs. Keeps track of all proxies slot estimates (all initialized to 0 on startup). As the epoch progresses, it receives
ZIP_REQUEST type messages from proxies and notes how many slots a particular proxy needs in the next epoch (value and proxy id are a part of
the message). At 3/4 of an epoch's duration, the zipper allocates slots for the following epoch and shares those values back to all proxies.
If a proxy does not respond during an epoch with an updated slot estimate, the zipper uses the last known value it received from that proxy.

## Proxies
Operate based on epochs. Keeps track of number of and the exact client requests its has received during that epoch progresses and stores them
in a `vector<Command>`. As the interval ends, it batched all of its stored commands into a CommandBatch (nested command), sends it out to
f + 1 servers and responds to the client.

At the end of an epoch the proxy determines its slot estimate which it will then send to the zipper. It is taken as the ceiling of the
average number of requests it has received over the last MAX_EPOCH_HISTORY (10 epochs). The Zipper then allocates that many sequence numbers
to a proxy.

When servicing a batch every (slot estimate / epoch duration) milliseconds, if there is a sequence number available a proxy will send out
a batch if it has stored commands or a skip operations if it does not. Otherwise, it continues storing client commands until it receives
sequence numbers from the zipper.

## Command Batching
Subscribers' logs are of type `vector<Command>`. With the introduction of epochs and batching, commands can be nested. the logic for this
can be found in types.h. Commands are currently a vector<uint8_t> so the size of the command is written as a 4 bytes header in the Command
before the actual value. Note that a log entry is a batched command. Thus, when interacting with logs you must "deserialize" the batched command.
There is a helper `CommandBatch::deserialize(const Command& cmd)` that will return a vector<Commands> representing all commands in that batch.
Reference api/types.h for the source code.

### Clients
Clients are only capable of sending `APPEND` operations. They contact their assigned proxy and wait for a success response.

### Servers
Servers listen for and accept connections (the only nodes connecting to servers at this point are proxies). They forward `APPEND`s and `SKIP`s
to subscribers and wait for `ACK`s before continuing their broadcast (using timeouts and retries before giving up).

Servers immediately `ACK` the requesting proxy before forwarding a message to all subscribers.

### Subscribers
Subscribers listen for and accept connections (the only nodes connecting to subscribers at this point are servers). They
apply operations once they have received `f + 1` forwarded messages from distinct servers (messages are buffered before then).

Operations are applied in consecutive order relative to sequence numbers. If a command reaches quorum but is not the next 
expected sequence number, it is buffered until the gaps have been filled on all preceding commands.

## Testing
Handled using Google Test. config/setup1.json specifies the main setups we aim to test for. There are basic tests for set up 1 and 2 
currently. This includes one/two clients and one of each system component (proxy, storage server, subscriber). You should install google test.

For linux
```
sudo apt-get install libgtest-dev
cd /usr/src/gtest
sudo cmake .
sudo make
sudo cp lib/*.a /usr/lib
```
## Configuration
The config is parsed using an [external library](https://github.com/nlohmann/json) for JSON parsing.

## Type Alias
api/types.h contains the aliases for each type and some helpers. Developed to make changing types slightly easier if need be.

## Base Node Class
The zipper, proxies, server and subscriber class all have a listening socket and similar functionality (i.e., node id, shard id, is running boolean, etc.).
The base node class allows you to abstract out that repetitive functionality so each class focusing on key behaviors in the algorithm. Found in 
impl/base_node.h.

## Message types
Types and helpers found in message.h     
```
    enum MessageTypes : uint32_t {
        APPEND,         // client to proxy, proxy to server OR server to subscriber
        SKIP,           // proxy to server OR server to subscriber
        SUCCESS,        // sent from proxy to client after successfully servicing a request
        FAILURE,        // sent from proxy to client after successfully servicing a request
        ACK,            // sent between system components in repsonse to the receipt of a message (ZIP_REQUEST, ZIP_RESPONSE, APPEND, SKIP).
        ZIP_REQUEST,    // proxy to zipper
        ZIP_RESPONSE,   // zipper to proxy
    };
```
## Notes/Comments
- **Epoch are not synchronized across components.** The zipper and each proxy runs their own epoch timer; which is why descriptions discuss epoch relative behavior in terms of what that component know.
- Each epoch is hardcoded at 1000ms (constant names EPOCH_DURATION_MS)
- Max epoch history is hardcoded at size 10 (constant named MAX_EPOCH_HISTORY)
- Communication is handled using TCP connection from the `<sys.socket>`, `<netinet/in.h>`, `<arpa/inet.h>` libraries.
- Logs are batched commands. Testing has functionality for expanding the log.
