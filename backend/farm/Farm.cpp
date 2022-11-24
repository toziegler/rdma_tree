#include "Farm.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace farm {
FaRM::FaRM() {
   // -------------------------------------------------------------------------------------
   // find node id
   if (FLAGS_nodes != 1) {
      for (; nodeId < FLAGS_nodes; nodeId++) {
         if (FLAGS_ownIp == NODES[FLAGS_nodes][nodeId]) break;
      }
   } else {
      nodeId = 0;  // fix to allow single node use on all nodes
   }
   ensure(nodeId < FLAGS_nodes);
   // -------------------------------------------------------------------------------------
   // order of construction is important
   cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
   rdmaCounters = std::make_unique<profiling::RDMACounters>();
   barrier = (uint64_t*)cm->getGlobalBuffer().allocate(sizeof(uint64_t),64);
   *barrier=0;
}

FaRM::~FaRM() {
   stopProfiler();
   workerPool.reset();  // important clients need to disconnect first
   mh.reset();
}
}  // namespace nam
