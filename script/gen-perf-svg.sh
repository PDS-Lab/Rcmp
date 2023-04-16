#!/bin/bash

t=$(date "+%Y-%m-%d_%H-%M-%S")
sudo perf script | stackcollapse-perf.pl | flamegraph.pl > perf_$t.svg