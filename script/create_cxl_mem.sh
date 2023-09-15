#!/bin/bash

NODE=1
DEV_NO=$1
SIZE=$2

numactl --membind=$NODE dd if=/dev/zero of=/dev/shm/cxlsim$DEV_NO bs=1M count=$SIZE