#!/bin/bash

user=$1
passwd=$2
CMD_DIR="/home/$user/RCHMS/build"
SUDO="echo $passwd | sudo -S"

IP_MN="192.168.200.51"
PORT_MN=31850
# IP_DNs=("192.168.200.51" "192.168.201.52" "192.168.201.33" "192.168.201.89")
# IP_CNs=(${IP_DNs[0]} ${IP_DNs[1]} ${IP_DNs[2]} ${IP_DNs[3]})
IP_DNs=("192.168.200.51")
IP_CNs=(${IP_DNs[0]})

kill_all() {
    echo "kill all"

    for ((i=0; i<${#IP_CNs[@]}; i++))
    do
        sshpass -p $passwd ssh $user@${IP_CNs[i]} "echo $passwd | sudo -S killall rw" &
    done

    sleep 2

    for ((i=0; i<${#IP_DNs[@]}; i++))
    do
        sshpass -p $passwd ssh $user@${IP_DNs[i]} "echo $passwd | sudo -S killall rchms_daemon" &
        sleep 2
    done

    sshpass -p $passwd ssh $user@$IP_MN "echo $passwd | sudo -S killall rchms_master" &

    sleep 2
}

test_run() {
    MN_CMD="echo $passwd | sudo -S $CMD_DIR/rchms_master --master_ip=$IP_MN --master_port=$PORT_MN"

    echo "[exec] $MN_CMD"
    sshpass -p $passwd ssh $user@$IP_MN "echo $passwd | sudo -S $MN_CMD" &

    sleep 5

    for ((i=0; i<${#IP_DNs[@]}; i++))
    do
        port=$(($PORT_MN+1+$i))

        DN_CMD="echo $passwd | sudo -S numactl -N 0 $CMD_DIR/rchms_daemon --master_ip=$IP_MN --master_port=$PORT_MN --daemon_ip=${IP_DNs[i]} --daemon_port=$port --rack_id=$i --cxl_devdax_path=/dev/shm/cxlsim$i --cxl_memory_size=$CXL_MEM_SZ --hot_decay=$HOT_DECAY --hot_swap_watermark=$WATERMARK"

        echo "[exec] $DN_CMD"
        sshpass -p $passwd ssh $user@${IP_DNs[i]} "echo $passwd | sudo -S $DN_CMD" &

        sleep 5
    done

    PIDS=()

    for ((i=0; i<${#IP_CNs[@]}; i++))
    do
        port=$((14800+$i))

        NODES=${#IP_CNs[@]}
        NID=$i
        ALLOC_PAGE_CNT=$(($ADDR_RANGE/2/1024/1024/$NODES))

        CN_CMD="echo $passwd | sudo -S numactl -N 0 $CMD_DIR/test/rw --client_ip=${IP_CNs[i]} --client_port=$port --rack_id=$i --cxl_devdax_path=/dev/shm/cxlsim$i --cxl_memory_size=$CXL_MEM_SZ --iteration=$IT --payload_size=$payload --addr_range=$ADDR_RANGE --thread=$THREAD --thread_all=1 --no_node=$NODES --node_id=$NID --read_ratio=0 --redis_server_ip=192.168.201.52:6379"

        echo "[exec] $CN_CMD"
        sshpass -p $passwd ssh $user@${IP_CNs[i]} "echo $passwd | sudo -S $CN_CMD" &
        PIDS+=($!)

        sleep 4
    done

    wait ${PIDS[@]}

    kill_all
}

kill_all

$CMD_DIR/../script/scp-src.sh $user $passwd

echo "Start ..."

port=$((14800+0))

# reserve 2GB, data 8GB(include swap 100MB)
CXL_MEM_SZ=$((10*1024*1024*1024))
ADDR_RANGE=$(((8*1024-100)*1024*1024))
ALLOC_PAGE_CNT=$(($ADDR_RANGE/2/1024/1024))
HOT_DECAY=0.04
WATERMARK=3
THREAD=8
IT=1000000
SA=$((2*1024*1024))

for payload in 64
do
    test_run
done
