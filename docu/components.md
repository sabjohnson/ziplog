# Ziplog: Component Specifications

## Overview

Ziplog is a shared log system. Clients send append requests to **Proxies**, which batch and replicate them to storage servers in a globally consistent order. The **Zipper** is a central sequencer that assigns sequence numbers and send timestamps to proxies each epoch.

---

## Zipper

### Responsibility
The Zipper is the global sequencer for a shard. It allocates ordered sequence numbers to proxies based on their estimated request load, and manages proxy membership and failure recovery.

### Configuration (`ZipperConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `proxies` | `vector<Address>` | Initial proxy addresses |
| `subscribers` | `vector<Address>` | Subscriber addresses |
| `epoch_duration_ms` | `Timestamp` | Length of one epoch in milliseconds |
| `shard_id` | `ShardId` | Shard this zipper is responsible for |

### Lifecycle
```cpp
ZipperConfig cfg = ...;
Zipper zipper(cfg);   // starts listening + epoch timer automatically
// ...
zipper.shutdown();    // stops epoch timer, closes all connections
```

### Epoch Cycle
Managed by `EpochTimer`, which runs on its own thread with two trigger points:

**At 75% of epoch duration** — `allocate_slots()`:
- Reads each active proxy's estimated request count from `ProxyRegistry`
- Distributes sequence numbers evenly across the epoch window using interleaved timestamps
- Sends `ZIP_RESPONSE` to each proxy with their assigned `[timestamp, seq, ...]` pairs
- Broadcasts allocations to all servers
- Skips proxies in `RECONFIGURING` or `BLOCKED` state

**At 100% of epoch duration** — `introduce_proxies()`:
- Admits any proxies that completed registration during the epoch
- Adds them to `ProxyRegistry` and `config_.proxies`
- Sends `INCLUDE_PROXY` confirmation to each new proxy

### Messages Handled
| Type | Handler | Description |
|---|---|---|
| `ZIP_REQUEST` | `update_slot_estimate` | Proxy reports how many slots it needs next epoch |
| `REPORT` | `reconfig_manager_.handle_report` | Server reports a proxy as failed |
| `FREEZE_RESPONSE` | `reconfig_manager_.handle_freeze_response` | Server responds to freeze broadcast |
| `REGISTER_PROXY` | `add_proxy(msg, true)` | New proxy joining the system |
| `REJOIN_PROXY` | `add_proxy(msg, false)` | Previously failed proxy rejoining |
| `REGISTER_SUBSCRIBER` | `introduce_subscriber` | New subscriber joining |

### Proxy Registration Flow
1. Proxy sends `REGISTER_PROXY` with its `ip:port` as payload
2. Zipper broadcasts `INCLUDE_PROXY` to all servers and waits for quorum acks
3. On quorum — proxy is queued in `joining_proxies_`
4. At next epoch boundary — proxy is admitted, added to registry, sent `INCLUDE_PROXY`

### Proxy Failure & Reconfiguration
Managed by `ReconfigManager`:

1. Servers detect a failed proxy and send `REPORT` to the Zipper
2. Once a quorum of servers have reported the same proxy as failed:
   - Proxy status set to `RECONFIGURING`
   - Zipper broadcasts `FREEZE` to all servers
3. Servers respond with `FREEZE_RESPONSE` containing the last sequence number they saw from the proxy
4. If quorum responses **agree** on last sequence number:
   - Sends `SKIP` for any allocated-but-unused sequence numbers above last seen
   - Broadcasts `FREEZE_COMPLETE` to all servers
   - Proxy status set to `BLOCKED`
5. If quorum responses **disagree** → start a new freeze round with incremented round number
6. Proxy remains `BLOCKED` until it sends `REJOIN_PROXY`, which resets its state to `ACTIVE`

### Proxy State
Each proxy tracked in `ProxyRegistry` with a `ProxyState` struct:

| Field | Description |
|---|---|
| `address` | Proxy's `ip:port` |
| `status` | `ACTIVE`, `RECONFIGURING`, or `BLOCKED` |
| `estimate` | Slot count requested for current epoch |
| `allocated_sequences` | Sequences assigned this epoch (used for SKIP detection) |
| `freeze_round` | Current freeze round number |
| `freeze_responders` | Servers that have responded to current freeze |
| `reporters` | Servers that have reported this proxy as failed |
| `last_sequences` | Last sequence numbers reported by servers during freeze |

---

## Proxy

### Responsibility
A Proxy accepts append requests from clients, batches them, and replicates each batch to a quorum of storage servers in the globally ordered sequence assigned by the Zipper.

### Configuration (`ProxyConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `zipper` | `Address` | Zipper address |
| `epoch_duration_ms` | `Timestamp` | Must match Zipper's epoch duration |
| `max_epoch_history` | `size_t` | Epochs of history used for slot estimate |
| `shard_id` | `ShardId` | Shard this proxy belongs to |

### Lifecycle
```cpp
// Pre-registered proxy — starts immediately
ProxyConfig cfg = ...;
Proxy proxy(cfg);

// Joining proxy — must register with Zipper first
Proxy proxy(cfg, false);
// epoch timer starts automatically after INCLUDE_PROXY is received
```

### Epoch Cycle
Managed by `ProxyEpochTimer` on its own thread with two trigger conditions:

**Every `epoch_duration_ms`** — `update_slot_estimate()`:
- Computes rolling average of `request_count` over last `max_epoch_history` epochs
- Resets `request_count` to 0
- Sends `ZIP_REQUEST` to Zipper with the computed estimate

**When `now_ms() >= next_send`** — `send_out_batch()`:
- Pops next `(seq, timestamp)` pair from `SlotScheduler`
- Drains client buffers round-robin up to 60KB via `ClientBufferManager::drain_batch`
- If no pending data → message type set to `SKIP`, empty payload
- If data available → message type set to `APPEND`, serialized `CommandBatch` as payload
- Replicates via `BatchReplicator` — blocks until f+1 server acks received
- Sends `SUCCESS` or `FAILURE` back to all participating client sockets

### Messages Handled
| Type | Action |
|---|---|
| `APPEND` | Push command into client's `CircularBuffer`, increment request count |
| `ZIP_RESPONSE` | Load `[timestamp, seq, ...]` pairs into `SlotScheduler` |
| `INCLUDE_PROXY` | Set `registered_ = true`, start epoch timer |
| `FREEZE` | Set `registered_ = false`, pause epoch timer, call `attempt_join(false)` |

### Components

#### `SlotScheduler`
Manages allocated sequence numbers, send timestamps, and rolling slot estimates.

| Method | Description |
|---|---|
| `load_slots(ordering_values)` | Loads interleaved `[timestamp, seq, ...]` from `ZIP_RESPONSE` |
| `record_request()` | Increments request count for current epoch |
| `compute_estimate()` | Rolls history, returns new estimate, resets count |
| `pop_next_slot(seq, time)` | Pops next `(seq, timestamp)` pair, returns false if empty |
| `next_send()` | Returns timestamp of next scheduled send, 0 if no slots |

#### `BatchReplicator`
Maintains one persistent thread and socket per storage server.

| Method | Description |
|---|---|
| `start()` | Spawns one `ServerWorker` thread per server, establishes persistent connections |
| `replicate(msg)` | Pushes message to all worker queues, blocks until f+1 acks received |
| `shutdown()` | Signals all workers to stop, joins threads |

Each `ServerWorker`:
- Holds a persistent TCP connection to its assigned server
- Waits on a queue for `WorkItem`s
- Sends message, waits for ack, signals shared `ReplicationState`
- On send/recv failure — marks itself as shutdown

#### `ClientBufferManager`
Per-socket `CircularBuffer<PendingRequest>` with blocking push.

| Method | Description |
|---|---|
| `push(socket, cmd)` | Blocking push into socket's buffer — blocks if buffer full |
| `remove(socket)` | Removes buffer when client disconnects |
| `drain_batch()` | Round-robin drain across all buffers up to 60KB, returns `(CommandBatch, set<int>)` |
| `buffer_size(socket)` | Returns current size of a socket's buffer |

#### `ProxyEpochTimer`
Runs on its own thread, triggers epoch callbacks independently of message handling.

| Method | Description |
|---|---|
| `start()` | Starts the timer thread |
| `stop()` | Stops and joins the timer thread |
| `pause()` | Suppresses callbacks without stopping the thread (used during FREEZE) |
| `resume()` | Re-enables callbacks after rejoin |

---

## Key Invariants

- Sequence numbers are globally unique and monotonically increasing across all proxies in a shard
- A proxy only sends a batch when its assigned timestamp is reached
- A proxy in `BLOCKED` state will not receive new slot allocations until it rejoins
- `SKIP` messages fill gaps for allocated-but-unused sequence numbers so servers never stall waiting for a missing entry (this is equivalent to empty batch of commands)
- Each proxy maintains exactly one persistent connection per storage server via `BatchReplicator`
- Client buffers block on push when full (ideally to provide backpressure from servers to clients)
### Responsibility
The Zipper is the global sequencer for a shard. It allocates ordered sequence numbers to proxies based on their estimated request load, and manages proxy membership and failure recovery.

### Configuration (`ZipperConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `proxies` | `vector<Address>` | Initial proxy addresses |
| `subscribers` | `vector<Address>` | Subscriber addresses |
| `epoch_duration_ms` | `Timestamp` | Length of one epoch in milliseconds |
| `shard_id` | `ShardId` | Shard this zipper is responsible for |

### Lifecycle
```cpp
ZipperConfig cfg = ...;
Zipper zipper(cfg);   // starts listening + epoch timer automatically
// ...
zipper.shutdown();    // stops epoch timer, closes all connections
```

### Epoch Cycle
Managed by `EpochTimer`, which runs on its own thread with two trigger points:

**At 75% of epoch duration** — `allocate_slots()`:
- Reads each active proxy's estimated request count from `ProxyRegistry`
- Distributes sequence numbers evenly across the epoch window using interleaved timestamps
- Sends `ZIP_RESPONSE` to each proxy with their assigned `[timestamp, seq, ...]` pairs
- Broadcasts allocations to all servers
- Skips proxies in `RECONFIGURING` or `BLOCKED` state

**At 100% of epoch duration** — `introduce_proxies()`:
- Admits any proxies that completed registration during the epoch
- Adds them to `ProxyRegistry` and `config_.proxies`
- Sends `INCLUDE_PROXY` confirmation to each new proxy

### Messages Handled
| Type | Handler | Description |
|---|---|---|
| `ZIP_REQUEST` | `update_slot_estimate` | Proxy reports how many slots it needs next epoch |
| `REPORT` | `reconfig_manager_.handle_report` | Server reports a proxy as failed |
| `FREEZE_RESPONSE` | `reconfig_manager_.handle_freeze_response` | Server responds to freeze broadcast |
| `REGISTER_PROXY` | `add_proxy(msg, true)` | New proxy joining the system |
| `REJOIN_PROXY` | `add_proxy(msg, false)` | Previously failed proxy rejoining |
| `REGISTER_SUBSCRIBER` | `introduce_subscriber` | New subscriber joining |

### Proxy Registration Flow
1. Proxy sends `REGISTER_PROXY` with its `ip:port` as payload
2. Zipper broadcasts `INCLUDE_PROXY` to all servers and waits for quorum acks
3. On quorum — proxy is queued in `joining_proxies_`
4. At next epoch boundary — proxy is admitted, added to registry, sent `INCLUDE_PROXY`

### Proxy Failure & Reconfiguration
Managed by `ReconfigManager`:

1. Servers detect a failed proxy and send `REPORT` to the Zipper
2. Once a quorum of servers have reported the same proxy as failed:
   - Proxy status set to `RECONFIGURING`
   - Zipper broadcasts `FREEZE` to all servers
3. Servers respond with `FREEZE_RESPONSE` containing the last sequence number they saw from the proxy
4. If quorum responses **agree** on last sequence number:
   - Sends `SKIP` for any allocated-but-unused sequence numbers above last seen
   - Broadcasts `FREEZE_COMPLETE` to all servers
   - Proxy status set to `BLOCKED`
5. If quorum responses **disagree** → start a new freeze round with incremented round number
6. Proxy remains `BLOCKED` until it sends `REJOIN_PROXY`, which resets its state to `ACTIVE`

### Proxy State
Each proxy tracked in `ProxyRegistry` with a `ProxyState` struct:

| Field | Description |
|---|---|
| `address` | Proxy's `ip:port` |
| `status` | `ACTIVE`, `RECONFIGURING`, or `BLOCKED` |
| `estimate` | Slot count requested for current epoch |
| `allocated_sequences` | Sequences assigned this epoch (used for SKIP detection) |
| `freeze_round` | Current freeze round number |
| `freeze_responders` | Servers that have responded to current freeze |
| `reporters` | Servers that have reported this proxy as failed |
| `last_sequences` | Last sequence numbers reported by servers during freeze |

---

## Proxy

### Responsibility
A Proxy accepts append requests from clients, batches them, and replicates each batch to a quorum of storage servers in the globally ordered sequence assigned by the Zipper.

### Configuration (`ProxyConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `zipper` | `Address` | Zipper address |
| `epoch_duration_ms` | `Timestamp` | Must match Zipper's epoch duration |
| `max_epoch_history` | `size_t` | Epochs of history used for slot estimate |
| `shard_id` | `ShardId` | Shard this proxy belongs to |

### Lifecycle
```cpp
// Pre-registered proxy — starts immediately
ProxyConfig cfg = ...;
Proxy proxy(cfg);

// Joining proxy — must register with Zipper first
Proxy proxy(cfg, false);
// epoch timer starts automatically after INCLUDE_PROXY is received
```

### Epoch Cycle
Managed by `ProxyEpochTimer` on its own thread with two trigger conditions:

**Every `epoch_duration_ms`** — `update_slot_estimate()`:
- Computes rolling average of `request_count` over last `max_epoch_history` epochs
- Resets `request_count` to 0
- Sends `ZIP_REQUEST` to Zipper with the computed estimate

**When `now_ms() >= next_send`** — `send_out_batch()`:
- Pops next `(seq, timestamp)` pair from `SlotScheduler`
- Drains client buffers round-robin up to 60KB via `ClientBufferManager::drain_batch`
- If no pending data → message type set to `SKIP`, empty payload
- If data available → message type set to `APPEND`, serialized `CommandBatch` as payload
- Replicates via `BatchReplicator` — blocks until f+1 server acks received
- Sends `SUCCESS` or `FAILURE` back to all participating client sockets

### Messages Handled
| Type | Action |
|---|---|
| `APPEND` | Push command into client's `CircularBuffer`, increment request count |
| `ZIP_RESPONSE` | Load `[timestamp, seq, ...]` pairs into `SlotScheduler` |
| `INCLUDE_PROXY` | Set `registered_ = true`, start epoch timer |
| `FREEZE` | Set `registered_ = false`, pause epoch timer, call `attempt_join(false)` |

### Components

#### `SlotScheduler`
Manages allocated sequence numbers, send timestamps, and rolling slot estimates.

| Method | Description |
|---|---|
| `load_slots(ordering_values)` | Loads interleaved `[timestamp, seq, ...]` from `ZIP_RESPONSE` |
| `record_request()` | Increments request count for current epoch |
| `compute_estimate()` | Rolls history, returns new estimate, resets count |
| `pop_next_slot(seq, time)` | Pops next `(seq, timestamp)` pair, returns false if empty |
| `next_send()` | Returns timestamp of next scheduled send, 0 if no slots |

#### `BatchReplicator`
Maintains one persistent thread and socket per storage server.

| Method | Description |
|---|---|
| `start()` | Spawns one `ServerWorker` thread per server, establishes persistent connections |
| `replicate(msg)` | Pushes message to all worker queues, blocks until f+1 acks received |
| `shutdown()` | Signals all workers to stop, joins threads |

Each `ServerWorker`:
- Holds a persistent TCP connection to its assigned server
- Waits on a queue for `WorkItem`s
- Sends message, waits for ack, signals shared `ReplicationState`
- On send/recv failure — marks itself as shutdown

#### `ClientBufferManager`
Per-socket `CircularBuffer<PendingRequest>` with blocking push.

| Method | Description |
|---|---|
| `push(socket, cmd)` | Blocking push into socket's buffer — blocks if buffer full |
| `remove(socket)` | Removes buffer when client disconnects |
| `drain_batch()` | Round-robin drain across all buffers up to 60KB, returns `(CommandBatch, set<int>)` |
| `buffer_size(socket)` | Returns current size of a socket's buffer |

#### `ProxyEpochTimer`
Runs on its own thread, triggers epoch callbacks independently of message handling.

| Method | Description |
|---|---|
| `start()` | Starts the timer thread |
| `stop()` | Stops and joins the timer thread |
| `pause()` | Suppresses callbacks without stopping the thread (used during FREEZE) |
| `resume()` | Re-enables callbacks after rejoin |

---

## Server

### Responsibility
A Server is the durable storage layer for a shard. It receives replicated batches from Proxies, maintains an ordered message log, broadcasts committed entries to Subscribers, and participates in proxy failure recovery by tracking liveness and coordinating freeze/reconfiguration with the Zipper.

### Configuration (`ServerConfig`)
| Field | Type | Description |
|---|---|---|
| `zipper` | `Address` | Zipper address (for sending REPORT) |
| `proxies` | `vector<Address>` | Initial proxy addresses |
| `servers` | `vector<Address>` | Other server addresses (for TRANSFER_REQUEST during freeze) |
| `subscribers` | `vector<Address>` | Initial subscriber addresses |
| `epoch_duration_ms` | `Timestamp` | Used to set liveness timeout windows |
| `shard_id` | `ShardId` | Shard this server belongs to |

### Lifecycle
```cpp
ServerConfig cfg = ...;
Server server(cfg);   // starts listening automatically
// ...
server.shutdown();
```

### Messages Handled
| Type | Handler | Description |
|---|---|---|
| `APPEND` | store + broadcast + ack | Stores batch, removes liveness timeout, broadcasts to subscribers |
| `SKIP` | store + remove_timeout + ack | Stores gap-filler, removes liveness timeout |
| `ZIP_RESPONSE` | `liveness_.update_timeouts` | Records which sequences a proxy was assigned this epoch |
| `FREEZE` | `freeze_handler_.handle_freeze` | Participates in reconfiguration protocol |
| `TRANSFER_REQUEST` | `freeze_handler_.handle_transfer_request` | Shares stored messages with peer servers during freeze |
| `FREEZE_COMPLETE` | `liveness_.block_proxy` | Marks proxy as blocked, stops expecting its sequences |
| `INCLUDE_PROXY` | `introduce_proxy` + ack | Registers a new or rejoining proxy, unblocks it in liveness tracker |
| `INCLUDE_SUBSCRIBER` | `introduce_subscriber` + ack | Registers a new subscriber for broadcast |

### Components

#### `MessageStore`
Append-only per-proxy message log stored in memory.

| Method | Description |
|---|---|
| `store(proxy_id, msg)` | Appends message to proxy's deque |
| `get(proxy_id)` | Returns all stored messages for a proxy |
| `snapshot()` | Returns full copy of the log map |
| `has(proxy_id)` | Returns true if any messages stored for proxy |
| `mutex()` | Exposes internal mutex for coordinated access |

#### `ProxyLivenessTracker`
Runs a background failure detection thread. Tracks expected sequence arrivals and reports overdue proxies to the Zipper.

| Method | Description |
|---|---|
| `update_timeouts(proxy_id, ordering_values)` | On `ZIP_RESPONSE` — records assigned sequences and their expected arrival times |
| `remove_timeout(proxy_id, seq)` | On `APPEND`/`SKIP` — clears a sequence from the pending timeout set |
| `block_proxy(proxy_id)` | On `FREEZE_COMPLETE` — stops tracking sequences for this proxy |
| `unblock_proxy(proxy_id)` | On `INCLUDE_PROXY` rejoin — resumes tracking |
| `set_reconfiguring(proxy_id)` | Suppresses reporting while freeze is in progress |
| `is_blocked(proxy_id)` | Returns true if proxy is in BLOCKED state |
| `last_seq(proxy_id)` | Returns last sequence number seen from a proxy |

The detection thread wakes every `epoch_duration_ms`, checks all pending timeouts, and sends `REPORT` to the Zipper for any proxy whose sequences are overdue.

#### `SubscriberBroadcaster`
Fire-and-forget per-subscriber worker threads.

| Method | Description |
|---|---|
| `start(subscribers)` | Spawns one worker thread per subscriber |
| `broadcast(msg)` | Pushes message to all worker queues |
| `add_subscriber(idx, addr)` | Adds a late-joining subscriber |
| `shutdown()` | Signals all workers to stop, joins threads |

Each worker holds a persistent connection and sends messages in order from its queue. Delivery is best-effort — a failed subscriber does not block others.

#### `FreezeHandler`
Coordinates the server's role in proxy reconfiguration.

| Method | Description |
|---|---|
| `handle_freeze(msg, from_zipper)` | Marks proxy as reconfiguring, collects missing messages from peer servers via `TRANSFER_REQUEST`, then sends `FREEZE_RESPONSE` to Zipper |
| `handle_transfer_request(socket, msg)` | Sends stored messages above `req_last_seq` to requesting peer, acks |

Freeze flow per server:
1. Receives `FREEZE` from Zipper with proxy ID and freeze round
2. Calls `liveness_.set_reconfiguring(proxy_id)`
3. Sends `TRANSFER_REQUEST` to all peer servers to collect any messages it may have missed
4. Once transfers complete, sends `FREEZE_RESPONSE` to Zipper with its last known sequence for that proxy

---

## Subscriber

### Responsibility
A Subscriber reads the committed log by receiving broadcast messages from all Servers and assembling them into a globally ordered, deduplicated log. It waits for a quorum of servers to deliver each sequence number before applying it.

### Configuration (`SubscriberConfig`)
| Field | Type | Description |
|---|---|---|
| `zipper` | `Address` | Zipper address (for registration) |
| `servers` | `vector<Address>` | Server addresses to connect to |
| `shard_id` | `ShardId` | Shard this subscriber reads from |

### Lifecycle
```cpp
SubscriberConfig cfg = ...;
Subscriber sub(cfg);          // connects to servers, starts listening

sub.wait_for_log_size(10);    // blocks until 10 entries committed
const LogTracker& log = sub.log();
```

### Components

#### `LogTracker`
Tracks received messages across servers and assembles the final log once quorum is reached.

| Method | Description |
|---|---|
| `observe(sender, seq, data, quorum)` | Records a message from one server; applies it once `quorum` distinct servers have delivered the same seq |
| `wait_for_size(n)` | Blocks until `n` entries have been committed to the log |
| `wait_for_seq(seq)` | Blocks until a specific sequence number has been committed |
| `expand_unraveled()` | Returns `vector<vector<Command>>` — each inner vector is one committed batch, SKIPs excluded |
| `raw()` | Returns the raw committed log as `vector<Command>` including SKIP entries |
| `size()` | Returns number of committed entries |
| `next_seq()` | Returns the next expected sequence number |

Internally, `LogTracker` maintains:
- `log_` — committed entries in order
- `out_of_order_` — entries received out of order, applied when gap is filled
- `pending_quorum_` — per-sequence server delivery tracking
- `applied_` — set of already-committed sequence numbers (deduplication)

### Log Shape
After calling `expand_unraveled()`, the result is indexed by sequence number. Each entry is a batch (a `vector<Command>`) from one proxy's `send_out_batch()` call. SKIPs are excluded — they appear as empty inner vectors and are filtered out.

```cpp
vector<vector<Command>> log = sub.log().expand_unraveled();
// log[0] = first committed batch (one or more commands)
// log[1] = second committed batch
// ...
```

---

## Key Invariants

- A server never acks an `APPEND` or `SKIP` until it has durably stored the message
- A server only broadcasts to subscribers after storing — subscribers never see uncommitted data
- `ProxyLivenessTracker` only reports a proxy after it has missed sequences past the timeout window — transient delays do not trigger false positives
- `LogTracker` never applies a sequence number twice — `applied_` provides idempotent deduplication across duplicate server broadcasts
- Out-of-order delivery is handled transparently — `LogTracker` buffers early arrivals and applies them in order as gaps are filled
- A subscriber never reads a sequence until a quorum of servers have confirmed it — this provides the same consistency guarantee as the replication layer