# (Modern) Designing Distributed RDMA B-Trees (WIP) 
This is a (modern) implementation of our paper: 
```
@inproceedings{DBLP:conf/sigmod/0001VBFK19,
  author    = {Tobias Ziegler and
               Sumukha Tumkur Vani and
               Carsten Binnig and
               Rodrigo Fonseca and
               Tim Kraska},
  title     = {Designing Distributed Tree-based Index Structures for Fast RDMA-capable
               Networks},
  booktitle = {{SIGMOD} Conference 2019},
  year      = {2019},
}
```
## Motivation
The old code was based on old hardware, drivers, and interfaces. 


## Differences
There are notable differences compared to the original implementation: 

1. Two-sided version uses one-one sided RDMA for message passing which tremendously improved the performance compared to the original implementation that used Shared Receive Queues.
The reason is that the SRQ needed to be synchronized among multiple message handler, which caused quite a bit of contention.
We implemented a time-tested one-sided mailbox design, as found in our other paper Scalestore and TU Munich L5 paper.

2. We use a conventional B+-Tree instead of a B-Link Tree. 
Although the B-Link Tree can be better in high contention scenarios since it separates the splits into two phases, a standard implementation has some advantages.
First, since splits are now entirely installed at once, we can use the inner nodes to prefetch instead of dedicated prefetch pages.
The prefetch pages tend to become outdated frequently, requiring expensive updates, and, in fact, the performance suffered.
Second, we use fence keys to perform the scan, which would additionally enable caching.
That is, the inner nodes can be easily cached as they are usually below 1% of all B-Tree pages. To detect concurrent modifications, we could rely on the technique proposed in FaRM.
FaRM uses the fence keys on the leaf-level to see if those match the cached ranges; otherwise, the cache is refreshed. 
This would give us a better "hybrid" version which (1) improves performance tremendously (2) load balances as the pure one-sided (3) no CPU load on the storage.

## TODOs 
- [ ] Prefetch scan
- [ ] Hybrid implementation (not sure if needed)

## Setup

### Cluster Setup
All experiments were conducted on a 5-node cluster running Ubuntu 18.04.1 LTS, with Linux 4.15.0 kernel.
Each node is equipped with two Intel(R) Xeon(R) Gold 5120 CPUs (14 cores), 512 GB main-memory split between both sockets.
The cluster nodes are connected with an InfiniBand network using one Mellanox ConnectX-5 MT27800 NICs (InfiniBand EDR 4x, 100 Gbps) per node.
   
### Mellanox RDMA
We used the following Mellanox OFED installation:
   
#### ofed_info
```shell
MLNX_OFED_LINUX-5.1-2.5.8.0 (OFED-5.1-2.5.8):
Installed Packages:
-------------------
ii  ar-mgr                                        1.0-0.3.MLNX20200824.g8577618.51258     amd64        Adaptive Routing Manager
ii  dapl2-utils                                   2.1.10.1.mlnx-OFED.51258                amd64        Utilities for use with the DAPL libraries
ii  dpcp                                          1.1.0-1.51258                           amd64        Direct Packet Control Plane (DPCP) is a library to use Devx
ii  dump-pr                                       1.0-0.3.MLNX20200824.g8577618.51258     amd64        Dump PathRecord Plugin
ii  hcoll                                         4.6.3125-1.51258                        amd64        Hierarchical collectives (HCOLL)
ii  ibacm                                         51mlnx1-1.51258                         amd64        InfiniBand Communication Manager Assistant (ACM)
ii  ibdump                                        6.0.0-1.51258                           amd64        Mellanox packets sniffer tool
ii  ibsim                                         0.9-1.51258                             amd64        InfiniBand fabric simulator for management
ii  ibsim-doc                                     0.9-1.51258                             all          documentation for ibsim
ii  ibutils2                                      2.1.1-0.126.MLNX20200721.gf95236b.51258 amd64        OpenIB Mellanox InfiniBand Diagnostic Tools
ii  ibverbs-providers:amd64                       51mlnx1-1.51258                         amd64        User space provider drivers for libibverbs
ii  ibverbs-utils                                 51mlnx1-1.51258                         amd64        Examples for the libibverbs library
ii  infiniband-diags                              51mlnx1-1.51258                         amd64        InfiniBand diagnostic programs
ii  iser-dkms                                     5.1-OFED.5.1.2.5.3.1                    all          DKMS support fo iser kernel modules
ii  isert-dkms                                    5.1-OFED.5.1.2.5.3.1                    all          DKMS support fo isert kernel modules
ii  kernel-mft-dkms                               4.15.1-100                              all          DKMS support for kernel-mft kernel modules
ii  knem                                          1.1.4.90mlnx1-OFED.5.1.2.5.0.1          amd64        userspace tools for the KNEM kernel module
ii  knem-dkms                                     1.1.4.90mlnx1-OFED.5.1.2.5.0.1          all          DKMS support for mlnx-ofed kernel modules
ii  libdapl-dev                                   2.1.10.1.mlnx-OFED.51258                amd64        Development files for the DAPL libraries
ii  libdapl2                                      2.1.10.1.mlnx-OFED.51258                amd64        The Direct Access Programming Library (DAPL)
ii  libibmad-dev:amd64                            51mlnx1-1.51258                         amd64        Development files for libibmad
ii  libibmad5:amd64                               51mlnx1-1.51258                         amd64        Infiniband Management Datagram (MAD) library
ii  libibnetdisc5:amd64                           51mlnx1-1.51258                         amd64        InfiniBand diagnostics library
ii  libibumad-dev:amd64                           51mlnx1-1.51258                         amd64        Development files for libibumad
ii  libibumad3:amd64                              51mlnx1-1.51258                         amd64        InfiniBand Userspace Management Datagram (uMAD) library
ii  libibverbs-dev:amd64                          51mlnx1-1.51258                         amd64        Development files for the libibverbs library
ii  libibverbs1:amd64                             51mlnx1-1.51258                         amd64        Library for direct userspace use of RDMA (InfiniBand/iWARP)
ii  libibverbs1-dbg:amd64                         51mlnx1-1.51258                         amd64        Debug symbols for the libibverbs library
ii  libopensm                                     5.7.3.MLNX20201102.e56fd90-0.1.51258    amd64        Infiniband subnet manager libraries
ii  libopensm-devel                               5.7.3.MLNX20201102.e56fd90-0.1.51258    amd64        Developement files for OpenSM
ii  librdmacm-dev:amd64                           51mlnx1-1.51258                         amd64        Development files for the librdmacm library
ii  librdmacm1:amd64                              51mlnx1-1.51258                         amd64        Library for managing RDMA connections
ii  mlnx-ethtool                                  5.4-1.51258                             amd64        This utility allows querying and changing settings such as speed,
ii  mlnx-iproute2                                 5.6.0-1.51258                           amd64        This utility allows querying and changing settings such as speed,
ii  mlnx-ofed-kernel-dkms                         5.1-OFED.5.1.2.5.8.1                    all          DKMS support for mlnx-ofed kernel modules
ii  mlnx-ofed-kernel-utils                        5.1-OFED.5.1.2.5.8.1                    amd64        Userspace tools to restart and tune mlnx-ofed kernel modules
ii  mpitests                                      3.2.20-5d20b49.51258                    amd64        Set of popular MPI benchmarks and tools IMB 2018 OSU benchmarks ver 4.0.1 mpiP-3.3 IPM-2.0.6
ii  mstflint                                      4.14.0-3.51258                          amd64        Mellanox firmware burning application
ii  openmpi                                       4.0.4rc3-1.51258                        all          Open MPI
ii  opensm                                        5.7.3.MLNX20201102.e56fd90-0.1.51258    amd64        An Infiniband subnet manager
ii  opensm-doc                                    5.7.3.MLNX20201102.e56fd90-0.1.51258    amd64        Documentation for opensm
ii  perftest                                      4.4+0.5-1                               amd64        Infiniband verbs performance tests
ii  rdma-core                                     51mlnx1-1.51258                         amd64        RDMA core userspace infrastructure and documentation
ii  rdmacm-utils                                  51mlnx1-1.51258                         amd64        Examples for the librdmacm library
ii  sharp                                         2.2.2.MLNX20201102.b26a0fd-1.51258      amd64        SHArP switch collectives
ii  srp-dkms                                      5.1-OFED.5.1.2.5.3.1                    all          DKMS support fo srp kernel modules
ii  srptools                                      51mlnx1-1.51258                         amd64        Tools for Infiniband attached storage (SRP)
ii  ucx                                           1.9.0-1.51258                           amd64        Unified Communication X
```

### Libraries 
- gflags
- ibverbs
- tabulate
- rdma cm
  
