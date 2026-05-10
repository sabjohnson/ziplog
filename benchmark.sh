#!/bin/bash

# ─── config ───────────────────────────────────────────────────────────────────
ZIPLOG=/home/saj225/ziplog/ziplog
CONFIG=/home/saj225/ziplog/config/fractus.json
NUM_COMMANDS=${1:-1000}

C16=saj225@compute16.fractus.cs.cornell.edu
C17=saj225@compute17.fractus.cs.cornell.edu

LOGDIR=/home/saj225/ziplog/logs

# ─── ensure log dirs exist on remote machines ─────────────────────────────────
for host in $C16 $C17; do
    ssh $host "mkdir -p $LOGDIR"
done

# ─── helpers ──────────────────────────────────────────────────────────────────
kill_all() {
    echo "[*] killing all ziplog processes..."
    for host in $C16 $C17; do
        ssh $host "pkill -f 'ziplog' 2>/dev/null; true"
    done
    sleep 1
}

wait_for_port() {
    local host=$1
    local port=$2
    local retries=20
    echo "[*] waiting for $host:$port..."
    for i in $(seq 1 $retries); do
        if ssh $host "nc -z localhost $port 2>/dev/null"; then
            echo "[+] $host:$port ready"
            return 0
        fi
        sleep 0.3
    done
    echo "[!] timeout waiting for $host:$port"
    return 1
}

trap kill_all EXIT

# ─── start ────────────────────────────────────────────────────────────────────
kill_all() {
    echo "[*] killing all ziplog processes..."
    for host in $C16 $C17; do
        ssh $host "pkill -SIGTERM -f ziplog 2>/dev/null; true"
    done
    sleep 2  # give time for clean shutdown and log flush
}

echo "[*] starting subscriber on $C16..."
ssh $C16 "nohup $ZIPLOG subscriber $CONFIG 0 < /dev/null > $LOGDIR/subscriber0.log 2>&1 &"
wait_for_port $C16 8030 || exit 1

echo "[*] starting server on $C17..."
ssh $C17 "nohup $ZIPLOG server $CONFIG 0 < /dev/null > $LOGDIR/server0.log 2>&1 &"
wait_for_port $C17 8020 || exit 1

echo "[*] starting proxy on $C16..."
ssh $C16 "nohup $ZIPLOG proxy $CONFIG 0 < /dev/null > $LOGDIR/proxy0.log 2>&1 &"
wait_for_port $C16 8010 || exit 1

echo "[*] sleeping 1s for connections to stabilize..."
sleep 1

# ─── run benchmark ────────────────────────────────────────────────────────────
echo "[*] running benchmark: $NUM_COMMANDS commands..."
START=$(date +%s%N)

ssh $C16 "$ZIPLOG benchmark $CONFIG 0 $NUM_COMMANDS"

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))

# ─── collect results ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo " benchmark complete"
echo " commands:     $NUM_COMMANDS"
echo " total time:   ${ELAPSED_MS}ms"
echo " throughput:   $(( NUM_COMMANDS * 1000 / ELAPSED_MS )) cmd/s"
echo "════════════════════════════════════════"
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