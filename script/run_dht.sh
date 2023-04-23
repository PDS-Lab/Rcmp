#!/bin/bash

port=$((14800+$1))

sudo gdb --args ../build/test/dht --client_ip=192.168.1.51 --client_port=$port --rack_id=$1 --cxl_devdax_path=/dev/shm/cxlsim$1 --cxl_memory_size=4294967296 --iteration=1000000 --read_ratio=100