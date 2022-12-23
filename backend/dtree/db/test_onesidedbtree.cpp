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
#include <numeric>
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
         std::cout << "space consumption " << *store.cache_counter << std::endl;
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
   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> std::pair<size_t, size_t> {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1) end = N;
      return {begin, end};
   };
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
      size_t KEYS = 1e6;
      std::vector<Key> keys(KEYS);
      std::iota(std::begin(keys), std::end(keys), 1);
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(std::begin(keys), std::end(keys), g);
      //=== build tree ===//
      // get compute node partition
      comp.getWorkerPool().scheduleJobSync(0, [&]() {});
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
            onesided::BTree<Key, Value> tree(threads::onesided::Worker::my().metadataPage);
            auto p = partition(t_i, FLAGS_worker, keys.size());
            while (keep_running) {
               for (size_t p_i = p.first; p_i < p.second; p_i++) {
                  tree.insert(keys[p_i], keys[p_i]);
                  threads::onesided::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                  Value retValue;
                  ensure(tree.lookup(keys[p_i], retValue));
                  ensure(retValue == keys[p_i]);
               }
            }
            running_threads_counter--;
         });
      }
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) _mm_pause();
      comp.getWorkerPool().joinAll();
      comp.stopProfiler();
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         onesided::BTree<Key, Value> tree(threads::onesided::Worker::my().metadataPage);
         for (Key k = 1; k < 100000; k++) {
            Value retValue;
            ensure(tree.lookup(k, retValue));
            ensure(k == retValue);
         }
         Key current_key = 1;
         tree.range_scan(1, 100000, [&](Key& key, Value value) {
            std::cout << "Key " << key << " Value " << value << std::endl;
            ensure(current_key == key);
            current_key++;
         });
      });
      std::cout << "Validation [OK]" << std::endl;
   }
   return 0;
}
