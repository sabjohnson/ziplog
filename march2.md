# Ziplog: Zipper & Proxy Specifications

## Overview

Ziplog is a shared log system. Clients send append requests to **Proxies**, which batch and replicate them to storage servers in a globally consistent order. The **Zipper** is a central sequencer that assigns sequence numbers and send timestamps to proxies each epoch.

---

## Zipper
# Ziplog: Zipper & Proxy Specifications

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
- A proxy only sends a batch when its assigned timestamp is reached — this enforces global ordering
- A proxy in `BLOCKED` state will not receive new slot allocations until it rejoins
- `SKIP` messages fill gaps for allocated-but-unused sequence numbers so servers never stall waiting for a missing entry
- Each proxy maintains exactly one persistent connection per storage server via `BatchReplicator`
- Client buffers block on push when full — this provides natural backpressure from servers to clients
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
- A proxy only sends a batch when its assigned timestamp is reached — this enforces global ordering
- A proxy in `BLOCKED` state will not receive new slot allocations until it rejoins
- `SKIP` messages fill gaps for allocated-but-unused sequence numbers so servers never stall waiting for a missing entry
- Each proxy maintains exactly one persistent connection per storage server via `BatchReplicator`
- Client buffers block on push when full — this provides natural backpressure from servers to clients