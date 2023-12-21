# Rcmp: A hybrid memory pooling system based on RDMA and CXL

Rcmp is a user-layer library for a distributed memory pooling system that mixes CXL and RDMA. Rcmp deploys large memory pools in separate racks, using CXL for coherent memory access within racks and RDMA for remote one-side access across racks. The CXL memory devices used within the rack have sub-microsecond latency, which can greatly accelerate remote memory access. And RDMA can scale the capacity of the memory pool well. However, since RDMA cannot do memory coherent access by raw verbs API, Rcmp introduces Remote Direct IO and Remote Page Swap policys in combination with RDMA to achieve coherent access across racks.

Rcmp currently supports the following features:

* **Memory Allocation and Release**: Clients allocate and release page-sized memory space via the AllocPage and FreePage APIs.

* **Consistent Memory Read/Write**: Users can access memory data using the global address GAddr and access data through Read/Write/CAS API. According to memory access hotspot is divided into CXL load/store access and RDMA one-side verb operation.

# How to use

* Using the Rcmp dynamic library

    The interfaces are defined in `include/rcmp.hpp`, and their use can be found in the `test/client_shell.cc`. This test launches a memory pool operations program and uses Rcmp's API to perform various operations on the memory pool.

1. Dependencies

    * gcc(>=4.6)

    * numactl

    * boost-fiber-dev

    * boost-coroutine-dev

    * boost-context-dev

    * asio

    * redis-plus-plus

    * fuse3

2. Compile

    ```shell
    mkdir -p build
    cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make
    ```

3. Run Cluster

* Start Master (MN)

    The MN process will start `ERPC`, please **apply for huge pages with 2GB granularity** in advance.

    ```shell
    sudo /home/user/Rcmp/build/rcmp_master --master_ip=192.168.200.51 --master_port=31850
    ```

* Start Rack Daemon (DN)

    The DN process will start `ERPC`, please **apply for huge pages with 2GB granularity** in advance.

    This project currently uses shared memory across NUMA to simulate CXL access. Run `script/create_cxl_mem.sh` to create shared memory on NUMA 1.

    All other processes run on NUMA 0.

    ```shell
    # Add rack 0 on 192.168.200.51 with CXL size 2.19GB
    sudo numactl -N 0 /home/user/Rcmp/build/rcmp_daemon --master_ip=192.168.200.51 --master_port=31850 --daemon_ip=192.168.200.51 --daemon_port=31851 --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=2357198848 --hot_decay=0.04 --hot_swap_watermark=3
    ```

    ```shell
    # Add rack 1 on 192.168.201.89 with CXL size 18GB
    sudo numactl -N 0 /home/user/Rcmp/build/rcmp_daemon --master_ip=192.168.200.51 --master_port=31850 --daemon_ip=192.168.201.89 --daemon_port=31852 --rack_id=1 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=19327352832 --hot_decay=0.04 --hot_swap_watermark=3
    ```

* Launching the client test program (CN)

    To simulate CXL, launch CN in the same rack on the same server as DN.

    `test/rw.cc` is a micro-benchmark. Use redis for cross-rack test synchronisation.

    ```shell
    sudo numactl -N 0 /home/user/Rcmp/build/test/rw --client_ip=192.168.200.51 --client_port=14800 --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=2357198848 --iteration=10000000 --payload_size=64 --addr_range=17179869184 --thread=32 --thread_all=1 --no_node=1 --node_id=0 --redis_server_ip=192.168.201.52:6379
    ```

# Application

After starting the necessary memory pool cluster environment (start MNs with DNs), use the Rcmp dynamic library to create the memory pool application. The following gives the implementation of the application that already exists in the project.

* Distributed hash table

    The distributed hash table uses the Rcmp interface to implement a linearly probed two-tier hash table. For simplicity, the hash table is fixed-sized. We will implement dynamic scaling added later (similar to CCEH).

    Location: `test/dht.hpp`.

* rchfs

    rchfs uses the FUSE API to implement a simple high-capacity in-memory file system. File metadata is stored on the client, file data blocks are allocated using Rcmp's AllocPage, and write/read system calls are redirected to Rcmp's Write/Read API. Later, we will add file metadata sharing in memory pool.

    Location: `fs/rchfs.cc`.

# Paper

Zhonghua Wang, Yixing Guo, Kai Lu*, Jiguang Wan, Daohui Wang, Ting Yao, Huatao Wu. Rcmp: Reconstructing RDMA-based Memory Disaggregation via CXL. ACM Transactions on Architecture and Code Optimization (TACO) 2023. (Just Accepted)