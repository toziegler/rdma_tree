#include "Storage.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#include <cstdint>

#include "Defs.hpp"
#include "dtree/Config.hpp"
#include "dtree/db/OneSidedBTree.hpp"
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
   cache_counter = (uint64_t*)cm->getGlobalBuffer().allocate(sizeof(uint64_t), 64);
   md = (onesided::MetadataPage*)cm->getGlobalBuffer().allocate(sizeof(onesided::MetadataPage), 64);
   uint64_t number_nodes =  static_cast<uint64_t>(((FLAGS_dramGB * 0.8) * 1024 * 1024 * 1024) / BTREE_NODE_SIZE );
   std::cout << "number nodes " << number_nodes << std::endl;
   node_buffer = (uint8_t*)cm->getGlobalBuffer().allocate(BTREE_NODE_SIZE * number_nodes, 64);
   for(uint64_t b_i =0; b_i < BTREE_NODE_SIZE * number_nodes; b_i++){
      if(node_buffer[b_i] != 0) throw;
   }
   auto iptr = reinterpret_cast<std::uintptr_t>(md);
   if ((iptr % 64) != 0) { throw std::runtime_error("not aligned"); }
   onesided::allocateInRDMARegion<onesided::MetadataPage>(md);
   ensure(md->type == onesided::PType_t::METADATA);
   *barrier = 0;
   *cache_counter = 0; 
}

Storage::~Storage() {
   stopProfiler();
   mh.reset();
   std::cout << "Destructing storage " << md->getRootPtr().offset << "\n";
}
}  // namespace dtree
