
# Modern Designing Distributed RDMA B-Trees 

This repository contains the modernized implementation of our paper:

```bibtex
@inproceedings{DBLP:conf/sigmod/0001VBFK19,
  author    = {Tobias Ziegler and
               Sumukha Tumkur Vani and
               Carsten Binnig and
               Rodrigo Fonseca and
               Tim Kraska},
  title     = {Designing Distributed Tree-based Index Structures for Fast RDMA-capable Networks},
  booktitle = {{SIGMOD} Conference 2019},
  year      = {2019},
}
```

## Motivation
The motivation to revisit and modernize this project stemmed from the need to adapt to current hardware, drivers, and interfaces which have substantially evolved since the original work.

## Features & Improvements

Compared to the original implementation, the modern version introduces the following enhancements:

1. **Improved Message Passing:** The two-sided version now employs one-sided RDMA for message passing. This has greatly boosted performance over the initial approach that used Shared Receive Queues (SRQ). This change was made since SRQ synchronization across multiple message handlers introduced contention. We've adopted the proven one-sided mailbox design from our other works, such as the Scalestore and TU Munich L5 paper.

2. **Conventional B+-Tree Implementation:** Instead of the B-Link Tree, we now utilize a standard B+-Tree. While the B-Link Tree might fare better in high contention scenarios due to its two-phase splits, the standard implementation brings distinct advantages:
   - Splits are now executed in a single phase, enabling the use of inner nodes for prefetching.
   - We've eliminated dedicated prefetch pages, which often became outdated, thereby improving performance.
   - The system now leverages fence keys for scans, which further facilitates caching. Inner nodes, which comprise less than 1% of all B-Tree pages, can be cached. Concurrent modifications can be identified using techniques from FaRM, enhancing the "hybrid" design.

## To-Do List

- [ ] Implement Prefetch Scanning.
- [ ] Evaluate the need for and possibly implement a Hybrid version.

## Setup

### Cluster Configuration
- **Nodes**: 5-node cluster.
- **OS**: Ubuntu 18.04.1 LTS.
- **Kernel**: Linux 4.15.0.
- **Hardware**: Each node has:
   - Two Intel(R) Xeon(R) Gold 5120 CPUs (14 cores each).
   - 512 GB main memory (distributed across both CPU sockets).
   - Mellanox ConnectX-5 MT27800 NICs (InfiniBand EDR 4x, 100 Gbps).

### Mellanox RDMA Version

For detailed information on the Mellanox OFED installation used, here is the `ofed_info` command output:

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

### Required Libraries 

Ensure these libraries are available for successful compilation and execution:
- `gflags`
- `ibverbs`
- `tabulate`
- `rdma cm`
  
