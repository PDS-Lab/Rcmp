# Rcmp: RDMA-CXL Memory Pool

Rcmp是一个CXL与RDMA混合的分布式内存池系统的用户态库。Rcmp采用分机柜部署大型内存池，在机柜内部使用CXL进行一致性的内存访问，跨机柜采用RDMA进行远程单边访问。在机柜内部使用的CXL内存设备的时延达到亚微秒级别，能够极大加速内存访问。而RDMA能够良好地扩展内存池的容量。但由于RDMA无法做到内存的一致性访问，Rcmp在结合RDMA时引入Remote Direct IO以及Remote Page Swap机制实现了跨机柜的一致性访问。更多信息可参考我们的论文[xxx](#paper)。

Rcmp目前支持以下功能：

* **内存分配与释放**：客户端通过AllocPage和FreePage API分配与释放页大小的内存空间。

* **一致性内存读写**：用户可使用全局地址GAddr访问内存数据，通过Read/Write/CAS API访问数据。根据内存访问热点分为CXL load/store访问和RDMA one-side verb操作。

# How to use

* 使用Rcmp动态库

    在`include/rcmp.hpp`定义了相关的接口，使用方法可见`test/client_shell.cc`文件。该测试启动一个内存池操作窗口，使用Rcmp的API进行内存池的各项操作。

1. 依赖

    * gcc(>=4.6)

    * numactl

    * boost-fiber

2. 编译

    ```shell
    mkdir -p build
    cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make
    ```

3. 运行集群

* 启动Master (MN)

    Master进程会启动ERPC，请**提前申请2GB粒度的大页**。

    ```shell
    sudo /home/user/Rcmp/build/rcmp_master --master_ip=192.168.200.51 --master_port=31850
    ```

* 启动Rack Daemon (DN)

    Daemon进程会启动ERPC，请**提前申请2GB粒度的大页**。

    本项目目前采用跨NUMA共享内存的方式模拟CXL访问。运行`script/create_cxl_mem.sh`在NUMA 1上创建共享内存。

    其他所有进程均运行在NUMA 0上。

    ```shell
    # 在192.168.200.51上加入rack 0，CXL大小2.19GB
    sudo numactl -N 0 /home/user/Rcmp/build/rcmp_daemon --master_ip=192.168.200.51 --master_port=31850 --daemon_ip=192.168.200.51 --daemon_port=31851 --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=2357198848 --hot_decay=0.04 --hot_swap_watermark=3
    ```

    ```shell
    # 在192.168.201.89上加入rack 1，CXL大小18GB
    sudo numactl -N 0 /home/gyx/Rcmp/build/rcmp_daemon --master_ip=192.168.200.51 --master_port=31850 --daemon_ip=192.168.201.89 --daemon_port=31852 --rack_id=1 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=19327352832 --hot_decay=0.04 --hot_swap_watermark=3
    ```

* 启动客户端测试程序 (CN)

    为了模拟CXL，将相同机架内的CN部署在与DN相同的服务器上。

    `test/rw.cc`是一个micro-benchmark。使用redis进行机架间测试同步。

    ```shell
    sudo numactl -N 0 /home/user/Rcmp/build/test/rw --client_ip=192.168.200.51 --client_port=14800 --rack_id=0 --cxl_devdax_path=/dev/shm/cxlsim0 --cxl_memory_size=2357198848 --iteration=10000000 --payload_size=64 --addr_range=17179869184 --thread=32 --thread_all=1 --no_node=1 --node_id=0 --redis_server_ip=192.168.201.52:6379
    ```

# 参考

<span id="paper"></span>[1] xxx