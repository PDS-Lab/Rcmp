#!/bin/bash

SIZE=$1

numactl --membind=1 dd if=/dev/zero of=/dev/shm/cxlsim0 bs=1M count=$SIZE