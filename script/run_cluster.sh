#!/bin/bash

user=$1
passwd=$2
CMD_DIR="/home/$user/RCHMS/build"
SUDO="echo $passwd | sudo -S"

MN=51
CN0=51
DN0=51
DN1=89

kill_all() {
    echo "kill all"
    sshpass -p $passwd ssh $user@192.168.1.$CN0 "$SUDO killall rw" &
    sshpass -p $passwd ssh $user@192.168.1.$DN0 "$SUDO killall rchms_daemon" &
    sshpass -p $passwd ssh $user@192.168.1.$DN1 "$SUDO killall rchms_daemon" &
    sshpass -p $passwd ssh $user@192.168.1.$MN "$SUDO killall rchms_master" &

    sleep 3
}

test_run() {
    kill_all

    MN_CMD="$SUDO $CMD_DIR/rchms_master --master_ip=192.168.1.$MN --master_rdma_ip=192.168.200.$MN --master_port=31850 >> result_rchms_mn.log 2>&1"

    port=$((31851+0))
    DN0_CMD="$SUDO numactl -N 0 $CMD_DIR/rchms_daemon --master_ip=192.168.1.$MN --master_port=31850 --daemon_ip=192.168.1.$DN0 --daemon_port=$port --daemon_rdma_ip=192.168.200.$DN0 --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=$CXL_MEM_SZ >> result_rchms_dn0.log 2>&1"
    port=$((31851+1))
    DN1_CMD="$SUDO numactl -N 0 $CMD_DIR/rchms_daemon --master_ip=192.168.1.$MN --master_port=31850 --daemon_ip=192.168.1.$DN1 --daemon_port=$port --daemon_rdma_ip=192.168.201.$DN1 --rack_id=1 --cxl_devdax_path=/dev/shm/cxlsim1 --cxl_memory_size=$CXL_MEM_SZ >> result_rchms_dn1.log 2>&1"

    echo "[exec] $MN_CMD"
    sshpass -p $passwd ssh $user@192.168.1.$MN $MN_CMD &

    sleep 3

    echo "[exec] $DN0_CMD"
    sshpass -p $passwd ssh $user@192.168.1.$DN0 $DN0_CMD &

    sleep 3

    echo "[exec] $DN1_CMD"
    sshpass -p $passwd ssh $user@192.168.1.$DN1 $DN1_CMD &

    sleep 3

    echo "[exec] $SUDO $*"
    # sshpass -p $passwd ssh $user@192.168.1.$CN0 "$SUDO $*"
    sudo $*

    return $?
}

test_retry() {
  while
    echo $*
    test_run $*
    [ $? != 0 ]
  do
    :
  done
}

echo "Start ..."

port=$((14800+0))
CXL_MEM_SZ=$((2*1024*1024*1024+212*2*1024*1024))

#64 256 512 1024 2048 4096
for payload in 256 512 1024 2048 4096
do
    iter=10000000
    test_retry numactl -N 0 $CMD_DIR/test/rw --client_ip=192.168.1.51 --client_port=$port --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=$CXL_MEM_SZ --iteration=$iter --payload_size=$payload --start_addr=2097152 --alloc_page_cnt=200 --addr_range=419430400 --read_ratio=50

done

kill_all
