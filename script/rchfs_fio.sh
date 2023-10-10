#!/bin/bash

user=`whoami`

fio -filename=/home/$user/tmp_rchfs_fs/fio_test -direct=1 -iodepth=1 -thread -rw=randwrite -ioengine=psync -bs=16k -size=2G -numjobs=10 -runtime=60 -group_reporting -name=mytest