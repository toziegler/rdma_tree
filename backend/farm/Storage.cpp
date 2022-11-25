#include "Storage.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace farm {
Storage::Storage() {
   // -------------------------------------------------------------------------------------
   // find node id
   if (FLAGS_storage_nodes != 1) {
      for (; nodeId < FLAGS_storage_nodes; nodeId++) {
         if (FLAGS_ownIp == NODES[FLAGS_storage_nodes][nodeId]) break;
      }
   } else {
      nodeId = 0;  // fix to allow single node use on all nodes
   }
   ensure(nodeId < FLAGS_storage_nodes);
   // -------------------------------------------------------------------------------------
   // order of construction is important
   cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
   rdmaCounters = std::make_unique<profiling::RDMACounters>();
   barrier = (uint64_t*)cm->getGlobalBuffer().allocate(sizeof(uint64_t),64);
   *barrier=0;
}

Storage::~Storage() {
   stopProfiler();
   mh.reset();
}
}  // namespace nam
