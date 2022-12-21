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
#include <unistd.h>
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
      while (true) {
         sleep(1);
         std::cout << "root  latch " << store.root->remote_latch << " version " << store.root->version
                   << " first value " << store.root->value_at(0) << " count " << store.root->count << std::endl;
         std::cout << "metadataPage root " << store.md->getRootPtr() << std::endl;
         // auto* nodes = static_cast<onesided::BTreeLeaf<uint64_t, uint64_t>*>(static_cast<void*>(store.node_buffer));
         // for (size_t i = 0; i < 200; i++) {
         // std::cout << "Node " << i << " latch " << nodes[i].remote_latch << " version " << nodes[i].version << "
         // first value " << nodes[i].value_at(0) << std::endl;
         //}
      }
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

   if (FLAGS_storage_node) {
      std::cout << "started storage node "
                << "\n";
      storage_node();
   } else {
      std::cout << "started compute node" << std::endl;
      Compute<threads::onesided::Worker> comp;
      comp.startAndConnect();
      //=== Barrier ===//
      uint64_t barrier_stage = 1;
      auto barrier_wait = [&]() {
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            comp.getWorkerPool().scheduleJobAsync(
                t_i, [&, t_i]() { threads::onesided::Worker::my().rdma_barrier_wait(barrier_stage); });
         }
         comp.getWorkerPool().joinAll();
         barrier_stage++;
      };
      //=== build tree ===//
      // get compute node partition
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         onesided::BTree<Key, Value> tree(threads::onesided::Worker::my().metadataPage);
         std::cout << "BTree created " << std::endl;
         for (Key k_i = 1; k_i < 500; k_i++) {
            usleep(250);
            tree.insert(k_i, k_i);
            std::cout << "ki " << k_i << std::endl;
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
         }
      });
      std::cout << "BEFORE BARRIER " << std::endl;
      barrier_wait();
      //=== Benchmark ===//
      profiling::EmptyWorkloadInfo wl;
      comp.startProfiler(wl);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            while (keep_running) {}
            running_threads_counter--;
         });
      }
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) _mm_pause();
      comp.getWorkerPool().joinAll();
      comp.stopProfiler();
   }
   return 0;
}
