#include "Compute.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace dtree {
Compute::Compute() {
   // -------------------------------------------------------------------------------------
   // // find node id
   // if (FLAGS_nodes != 1) {
   //    for (; nodeId < FLAGS_nodes; nodeId++) {
   //       if (FLAGS_ownIp == NODES[FLAGS_nodes][nodeId]) break;
   //    }
   // } else {
   //    nodeId = 0;  // fix to allow single node use on all nodes
   // }

   // -------------------------------------------------------------------------------------
   // order of construction is important
   cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
   rdmaCounters = std::make_unique<profiling::RDMACounters>();
}

Compute::~Compute() {
   stopProfiler();
   workerPool.reset();  // important clients need to disconnect first
}
}  // namespace nam
