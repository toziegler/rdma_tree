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
            std::cout << "Allocation done " << std::endl;
            for (uint64_t p_i = 0; p_i < 100; p_i++) {
               try {
                  onesided::GuardX<Leaf> o_guard(test_ptr);
                  // now force version mistmatch?
                  std::cout << "version" << o_guard->version << std::endl;
                  std::cout << "version" << o_guard.latch.version << std::endl;
                  ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES - 1);
               } catch (const onesided::OLCRestartException&) {
                  ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
                  std::cout << "Exception catched " << std::endl;
               }
            }
         }  // destructor
         std::cout << "concurrent latches memory " << threads::onesided::Worker::my().local_rmemory.get_size()
                   << std::endl;
         ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
      });
   }
   return 0;
}
