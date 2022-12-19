#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "dtree/Compute.hpp"
#include "dtree/Config.hpp"
#include "dtree/Storage.hpp"
#include "dtree/db/OneSidedBTree.hpp"
#include "dtree/db/OneSidedLatches.hpp"
#include "dtree/db/OneSidedTypes.hpp"
#include "dtree/profiling/ProfilingThread.hpp"
#include "dtree/profiling/counters/WorkerCounters.hpp"
#include "dtree/threads/Concurrency.hpp"
#include "dtree/threads/Worker.hpp"
#include "dtree/utils/RandomGenerator.hpp"
#include "dtree/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
// -------------------------------------------------------------------------------------

DEFINE_uint32(run_for_seconds, 5, "");

//=== Storage Logic ===//
void storage_node() {
   using namespace dtree;
   Storage store;
   // profiling::EmptyWorkloadInfo wl;
   // store.startProfiler(wl);
   store.startMessageHandler();
   {
      while (store.getConnectedClients() == 0)
         ;
      while (true) { std::cout << *store.cache_counter << std::endl; }
      [[maybe_unused]] dtree::RemoteGuard rguard(store.getConnectedClients());
   }
   std::cout << "Stopped Profiler" << std::endl;
   store.stopProfiler();
}

//=== Main ===//
int main(int argc, char* argv[]) {
   using namespace dtree;
   gflags::SetUsageMessage("Dtree Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
      using Leaf = onesided::BTreeLeaf<Key, Value>;
   if (FLAGS_storage_node) {
      std::cout << "started storage node "
                << "\n";
      storage_node();
   } else {
      std::cout << "started compute node" << std::endl;
      Compute<threads::onesided::Worker> comp;
      comp.startAndConnect();
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         {
            RemotePtr test_ptr;
            RemotePtr test_ptr2;
            {
               onesided::AllocationLatch<Leaf> ph;
               test_ptr = ph.remote_ptr;
               ph.unlatch();
               onesided::AllocationLatch<Leaf> ph2;
               test_ptr2 = ph2.remote_ptr;
               ph2.unlatch();
            }
            onesided::GuardO<Leaf> o_guard(test_ptr);
            onesided::GuardO<Leaf> o_guard2(std::move(o_guard)); // should not make allocation since we move 
            onesided::GuardO<Leaf> o_guard3(test_ptr2); // this should allocate though 
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES - 2);
            o_guard2 = std::move(o_guard3); // this should not allocate here we ensure that o_guard returns the previous allocated memory   
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES - 1);
         }  // destructor
         std::cout << " [Optmistic Test]concurrent latches memory " <<   threads::onesided::Worker::my().local_rmemory.get_size() << std::endl;
         ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
      });
      // exclusive test section 
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         {
            RemotePtr test_ptr;
            RemotePtr test_ptr2;
            {
               onesided::AllocationLatch<Leaf> ph;
               test_ptr = ph.remote_ptr;
               ph.unlatch();
               onesided::AllocationLatch<Leaf> ph2;
               test_ptr2 = ph2.remote_ptr;
               ph2.unlatch();
            }
            onesided::GuardX<Leaf> x_guard(test_ptr);
            onesided::GuardX<Leaf> x_guard2(test_ptr2); // should not make allocation since we move 
            x_guard2 = std::move(x_guard);
            //onesided::GuardX<Leaf> x_guard2(test_ptr); // should not make allocation since we move 
            //onesided::GuardO<Leaf> o_guard3(test_ptr2); // this should allocate though 
            //ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES - 2);
            //o_guard2 = std::move(o_guard3); // this should not allocate here we ensure that o_guard returns the previous allocated memory   
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES - 1);
         }  // destructor
         std::cout << "concurrent latches memory " <<   threads::onesided::Worker::my().local_rmemory.get_size() << std::endl;
         ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
      });
   }
   return 0;
}
