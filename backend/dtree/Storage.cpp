#include "Storage.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

#include "dtree/db/onesidedBtree.hpp"
// -------------------------------------------------------------------------------------
namespace dtree {
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
   barrier = (uint64_t*)cm->getGlobalBuffer().allocate(sizeof(uint64_t), 64);
   md = (onesided::MetadataPage*)cm->getGlobalBuffer().allocate(sizeof(onesided::MetadataPage), 64);
   auto iptr = reinterpret_cast<std::uintptr_t>(md);
   if ((iptr % 64) != 0) { throw std::runtime_error("not aligned"); }
   onesided::allocateInRDMARegion<onesided::MetadataPage>(md);
   ensure(md->type == onesided::PType_t::METADATA);
   *barrier = 0;
}

Storage::~Storage() {
   stopProfiler();
   mh.reset();
}
}  // namespace dtree
