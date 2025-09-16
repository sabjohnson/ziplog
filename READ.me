# Ziplog Implementation
### Sabrina Johnson

Project uses c++17, Make and an [external library](https://github.com/nlohmann/json) for JSON parsing.

## Basic Compilation Commands
`make` to create executable "ziplog".
`make clean` to clean up executable and respective object files (in obj/ directory).
`make external_clean` to clean up external json library (in /third_party).

## Running the Program
interactive testing for now (ex. starting client/server/subscriber in terminal and observing their behavior).
```
./ziplog client/server/subscriber configuration_filename index_in_configuration
```

## Current Functionality
- Configuration file/string parsing from JSON format (provides customizable 'f', max retries, connection timeouts).
- Serializable message structs.
- Networking utilities for sending messages with a 2 byte header.

### Clients
Clients are only capable of sending `APPEND` operations. They currently attempt to broadcast an operation to all servers. Clients
wait for `ACK`s before continuing a broadcast (using timeouts and retries before giving up).

Because there is no Zipper yet, clients increment their operation numbers after attempting an operation. The sequence number in their requests
 are `operation_number * num_of_clients + client_id` (ex. client 0 gets sequence numbers 0, 3, 6).

### Servers
Servers listen for and accept connections (the only nodes connecting to servers at this point are clients). They forward `APPEND`s
to subscribers and wait for `ACK`s before continuing their broadcast (using timeouts and retries before giving up).

Once a server has attempted to contact all subscribers, it will send an `ACK` to the requesting client.

### Subscribers
Subscribers listen for and accept connections (the only nodes connecting to subscribers at this point are servers). They
apply operations once they have received `f + 1` forwarded messages from distinct servers (messages are buffered before then).

Operations are applied in the order they reach quorum. This implementation does not take sequence numbers into account
for log placement yet.

## Notes/Comments
- Communication is handled using TCP connection from the <sys.socket>, <netinet/in.h>, <arpa/inet.h> libraries.