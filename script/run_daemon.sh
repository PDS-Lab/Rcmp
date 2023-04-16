#!/bin/bash

port=$((31851+$1))

sudo ../build/daemon --master_ip=192.168.1.51 --master_port=31850 --daemon_ip=192.168.1.51 --daemon_port=$port --daemon_rdma_ip=192.168.200.51 --rack_id=$1 --cxl_devdax_path=/dev/shm/cxlsim$1 --cxl_memory_size=4294967296