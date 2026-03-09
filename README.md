# Running Ziplog

## Building

```bash
make          # build main executable (./ziplog)
make test     # build and run test suite
make clean    # remove obj/ and executables
```
---

## Test Suite

Tests live in `tests/` and use GoogleTest. The test binary is `test_runner`.

```bash
make test                          # build + run all tests
./test_runner                      # run all tests directly
./test_runner --gtest_filter="E2ETest.Setup1_SingleAppend"   # run one test
./test_runner --gtest_filter="E2ETest.*"                     # run a suite
```

### Test Configurations
| Config | File | Proxies | Servers | Subscribers |
|---|---|---|---|---|
| setup1 | `config/setup1.json` | 1 | 1 | 1 |
| setup3 | `config/servers.json` | 3 | 3 | 3 |
| setup4 | `config/servers_test.json` | 1 | 3 | 1 |

Tests inherit from `ZiplogTestBase` (defined in `tests/test_utils.h`), which handles `StartSystem`, `TearDown`, and utilities like `send_append`, `wait_for_propagation`, and `expand_log`.

---

## Interactive Mode

The main binary (`./ziplog`) runs a single system component from a config file. Below is an example invocation.

```bash
./ziplog zipper config/servers.json
./ziplog proxy config/servers.json 0
```

To send appends interactively, use the client mode.

### Config File Format

```json
{
  "f": 1,
  "shard_id": 0,
  "epoch_duration_ms": 500,
  "max_epoch_history": 10,
  "zipper":      { "ip": "127.0.0.1", "port": 8000 },
  "proxies":    [{ "ip": "127.0.0.1", "port": 8001 }],
  "servers":    [{ "ip": "127.0.0.1", "port": 8010 }],
  "subscribers":[{ "ip": "127.0.0.1", "port": 8020 }]
}
```

All nodes must be on reachable addresses. For local development everything runs on `127.0.0.1` with different ports.

---

## Tuning

| Parameter | Location | Effect |
|---|---|---|
| `epoch_duration_ms` | config JSON | Shorter = lower latency, higher overhead. Minimum ~100ms for local dev |
| `max_epoch_history` | `ProxyConfig` | More history = smoother estimates, slower adaptation to load spikes |
| `f` | `NodeConfig` | Fault tolerance — quorum is `f+1`. Must have at least `f+1` servers |

---

## Notes

- All nodes log to stdout.
- Currently, only tests/single_proxy are uncommented for this most recent version.