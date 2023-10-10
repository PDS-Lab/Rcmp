#!/bin/bash

user=`whoami`
port=$((14800+$1))

sudo /home/$user/Rcmp/build/test/client_shell --client_ip=192.168.1.51 --client_port=$port --rack_id=$1 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=4294967296