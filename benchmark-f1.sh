#!/bin/bash

# ─── config ───────────────────────────────────────────────────────────────────
ZIPLOG=/home/saj225/ziplog/ziplog
CONFIG=/home/saj225/ziplog/config/fractus_f1.json
NUM_COMMANDS=${1:-1000}
NUM_CLIENTS=${2:-10}

C16=saj225@compute16.fractus.cs.cornell.edu  # proxy, subscriber, client
C17=saj225@compute17.fractus.cs.cornell.edu  # server 0
C20=saj225@compute20.fractus.cs.cornell.edu  # server 1
C21=saj225@compute21.fractus.cs.cornell.edu  # server 2

ALL_HOSTS="$C16 $C17 $C20 $C21"

LOGDIR=/home/saj225/ziplog/logs

# ─── ensure log dirs exist on all machines ────────────────────────────────────
for host in $ALL_HOSTS; do
    ssh $host "mkdir -p $LOGDIR"
done

# ─── helpers ──────────────────────────────────────────────────────────────────
kill_all() {
    echo "[*] killing all ziplog processes..."
    for host in $ALL_HOSTS; do
        ssh $host "pkill -SIGTERM -f ziplog 2>/dev/null; true"
    done
    sleep 2
}

wait_for_port() {
    local host=$1
    local port=$2
    local ip=$3
    local retries=20
    echo "[*] waiting for $host:$port..."
    for i in $(seq 1 $retries); do
        if ssh $host "nc -z $ip $port 2>/dev/null"; then
            echo "[+] $host:$port ready"
            return 0
        fi
        sleep 0.3
    done
    echo "[!] timeout waiting for $host:$port"
    return 1
}

trap kill_all EXIT

# ─── start infrastructure ─────────────────────────────────────────────────────
kill_all

echo "[*] starting subscriber on $C16..."
ssh $C16 "nohup $ZIPLOG subscriber $CONFIG 0 < /dev/null > $LOGDIR/subscriber0.log 2>&1 & disown"
wait_for_port $C16 8030 128.84.139.10 || exit 1

echo "[*] starting server 0 on $C17..."
ssh $C17 "nohup $ZIPLOG server $CONFIG 0 < /dev/null > $LOGDIR/server0.log 2>&1 & disown"
wait_for_port $C17 8020 128.84.139.11 || exit 1

echo "[*] starting server 1 on $C20..."
ssh $C20 "nohup $ZIPLOG server $CONFIG 1 < /dev/null > $LOGDIR/server1.log 2>&1 & disown"
wait_for_port $C20 8020 128.84.139.14 || exit 1

echo "[*] starting server 2 on $C21..."
ssh $C21 "nohup $ZIPLOG server $CONFIG 2 < /dev/null > $LOGDIR/server2.log 2>&1 & disown"
wait_for_port $C21 8020 128.84.139.15 || exit 1

echo "[*] starting proxy on $C16..."
ssh $C16 "nohup $ZIPLOG proxy $CONFIG 0 < /dev/null > $LOGDIR/proxy0.log 2>&1 & disown"
wait_for_port $C16 8010 128.84.139.10 || exit 1

echo "[*] sleeping 1s for connections to stabilize..."
sleep 1

# ─── run benchmark ────────────────────────────────────────────────────────────
echo "[*] running benchmark: $NUM_CLIENTS clients x $NUM_COMMANDS commands each..."
START=$(date +%s%N)

PIDS=()
for i in $(seq 0 $(( NUM_CLIENTS - 1 ))); do
    ssh $C16 "$ZIPLOG benchmark $CONFIG 0 $NUM_COMMANDS > $LOGDIR/client${i}.log 2>&1" &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait $pid
done

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
TOTAL_COMMANDS=$(( NUM_CLIENTS * NUM_COMMANDS ))

# ─── collect results ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo " benchmark complete (f=1, 3 servers)"
echo " clients:      $NUM_CLIENTS"
echo " commands:     $TOTAL_COMMANDS ($NUM_COMMANDS per client)"
echo " total time:   ${ELAPSED_MS}ms"
echo " throughput:   $(( TOTAL_COMMANDS * 1000 / ELAPSED_MS )) cmd/s"
echo "════════════════════════════════════════"

echo ""
echo "[*] per-client results:"
for i in $(seq 0 $(( NUM_CLIENTS - 1 ))); do
    ssh $C16 "cat $LOGDIR/client${i}.log" 2>/dev/null
done

sleep 2

echo ""
echo "[*] latency stats (µs):"
ssh $C16 "grep '\[latency\]' $LOGDIR/subscriber0.log \
    | grep -oP 'latency=\K[0-9]+' \
    | sort -n \
    | awk '
        BEGIN { count=0; sum=0 }
        { vals[count++]=\$1; sum+=\$1 }
        END {
            if (count==0) { print \"  no latency data\"; exit }
            print \"  count: \" count
            print \"  min:   \" vals[0]
            print \"  p50:   \" vals[int(count*0.50)]
            print \"  p99:   \" vals[int(count*0.99)]
            print \"  max:   \" vals[count-1]
            print \"  mean:  \" int(sum/count)
        }
    '"