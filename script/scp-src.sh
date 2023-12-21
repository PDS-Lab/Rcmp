#!/bin/bash

user=$1
passwd=$2

CMD_DIR="/home/$user/Rcmp/build"

for ip in 192.168.1.52 # 192.168.1.33 192.168.1.89
do
    sshpass -p $passwd scp $CMD_DIR/test/rw $user@$ip:$CMD_DIR/test
    sshpass -p $passwd scp $CMD_DIR/librcmp.so $CMD_DIR/rcmp_daemon $user@$ip:$CMD_DIR/
done