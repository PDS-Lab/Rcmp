# RDMA-CXL混合异构内存池

RCHMS应用程序在单个计算节点（CN）上运行，可以访问一个或多个数据节点（DN）。管理节点（MN）是集群的管理者，监控和管理这个集群的运行状态，储存RCHMS集群的元数据信息，并且完成内存分配。内存基于page粒度分配，每个数据节点都必须向管理节点注册提供给应用程序的内存量。

在物理层面构建内存池系统时，通常将计算物理机与存储物理机（包括CXL DRAM/AEP/SSD）分区放置，并通过CXL Switch在同一机柜内链接，这在该系统中视为一个机柜的管理。机柜内守护进程（Daemon）用于处理访问请求。各个机柜之间通过RDMA连接在一起，组成更大的网络结构。同时保留传统机柜（非CXL互联，包括DRAM/AEP/SSD/HDD）。

## 依赖库

* asio

## 主要部件

### 管理节点（MN）
* 维护集群的机柜状态
* 分配大块内存page
* 维护page与机柜的地址映射
* 作为page交换的协调者

### 守护进程（Daemon）
* 作为跨机柜访问的代理，接管本机柜内的CN向其他机柜访问的请求
* 维护机柜内的slab分配器，对page细粒度分块
* 维护page到本机柜内CXL的地址映射
* 维护page的访问热度，以进行page的交换到本地机柜
* 接收来自其他机柜的直写请求

### 计算节点（CN）
* 应用端通过API访问本地CXL设备与其他机柜的内存

### 数据节点（DN）
* 提供数据的持久化服务，包括CXL设备和传统设备

## 模块设计

### 管理节点MN
* Cluster Manager：维护集群系统元数据
* Page Allocator：page分配器
* Page Directory：维护page到机柜的映射目录
* Migration Handler：page迁移的协调调度
* RRPC：RDMA RPC通信模块

### CXL Daemon
* Msg Queue：采用CXL内存实现的与CN进程间通信的消息队列
* Page Table：维护page到CXL内存的映射
* Slab Allocator：slab分配器，细粒度划分page
* Swap Handler：page swap的调度实现
* CXL Proxy：接收来自其他机柜的数据访问，读取/写入CXL
* Hot Statistic：page访问热度统计
* RRPC：RDMA RPC通信模块

### CN包括应用程序库 Lib
* Page Table Cache：本地page缓存
* Msg Queue：与Daemon通信的消息队列
* RRPC：RDMA RPC通信模块
