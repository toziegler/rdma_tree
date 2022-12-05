#include <immintrin.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <csignal>
#include <cstring>
#include <iostream>
#include <vector>

#include "farm/threads/Worker.hpp"
#include "Defs.hpp"
//=== One-sided B-Tree ===//

namespace onesided {

//=== Locking  ===//

static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;

using Version = uint64_t;
struct AbstractLatch{
   RemotePtr remote_ptr;
   Version version {0};
   explicit AbstractLatch(RemotePtr remote_address) : remote_ptr(remote_address){}
};


struct ExclusiveLatch : public AbstractLatch
{
   explicit ExclusiveLatch(RemotePtr remote_ptr) : AbstractLatch(remote_ptr){}
   // returns true successfully

   template<typename FN>
   bool try_latch(FN&& sync_data_retrieval){
      farm::threads::Worker::my().compareSwap(UNLOCKED, EXCLUSIVE_LOCKED, remote_ptr, farm::rdma::completion::unsignaled);
      sync_data_retrieval();
      // issue compare and swap
      // compare and swap to remote address
      // do not read version but extract it from the node!
      // check if latched
      return false;
   }

   void unlatch(){
      // unlatch increments thread local buffer to avoid memory corruption
   };

   
   bool upgrade(Version version){
      // latch lock and check version
      // and read version
      return false;
   }
   
};

struct OptimisticLatch : public AbstractLatch
{
   OptimisticLatch(RemotePtr remote_ptr) : AbstractLatch(remote_ptr){}

   bool try_latch(){
      // read version and lock check if it is latched already
      // must be CL aligned 
      return false;
   };
   bool check_or_restart(){
      // throw if we could not latch it 
      return false;
   }   
};

//=== B-Tree Nodes ===//
// what do we need to have here
// we have an uint64_t which serves as the atomic 
//=== B-Tree ===//


}  // onesided
