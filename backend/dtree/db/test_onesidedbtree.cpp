#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "dtree/Compute.hpp"
#include "dtree/Config.hpp"
#include "dtree/Storage.hpp"
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
   //profiling::EmptyWorkloadInfo wl;
   //store.startProfiler(wl);
   store.startMessageHandler();
   {
      while (store.getConnectedClients() == 0)
         ;
      while(true){
         std::cout << *store.cache_counter << std::endl;
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
      Compute comp;
      comp.startAndConnect();
      //=== Barrier ===//
      uint64_t barrier_stage = 1;
      auto barrier_wait = [&]() {
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            comp.getWorkerPool().scheduleJobAsync(
                t_i, [&, t_i]() { threads::Worker::my().rdma_barrier_wait(barrier_stage); });
         }
         comp.getWorkerPool().joinAll();
         barrier_stage++;
      };
      //=== build tree ===//
      // get compute node partition
      std::mutex remote_ht_mtx;
      std::vector<RemotePtr> remote_ht;
      using myt = threads::Worker;
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            myt::my().refresh_caches();
            myt::my().remote_pages.shuffle();
            while (!myt::my().remote_pages.empty()) {
               RemotePtr node;
               ensure(myt::my().remote_pages.try_pop(node));
               onesided::ExclusiveLatch<onesided::BTreeLeaf<uint64_t, uint64_t>> xlatch(node);
               auto latched = xlatch.try_latch();
               if (!latched) throw std::logic_error("init latch failed ") ;
               xlatch.local_copy->entries[0].key =
                   (node.plainOffset() - myt::my().remote_caches[node.getOwner()].begin_offset) / BTREE_NODE_SIZE;
               xlatch.local_copy->entries[0].value = 0;
               xlatch.unlatch();
               remote_ht_mtx.lock();
               remote_ht.push_back(node);
               remote_ht_mtx.unlock();
            }
         });
      }
      std::cout << "BEFORE BARRIER " << std::endl;
      barrier_wait();
      //=== Benchmark ===//
      profiling::EmptyWorkloadInfo wl;
      comp.startProfiler(wl);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::atomic<u64> page_updates{0};
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            using myt = threads::Worker;
            running_threads_counter++;
            auto random_page = remote_ht[utils::RandomGenerator::getRandU64(0, remote_ht.size())];
            for (; keep_running; threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p)) {
               onesided::ExclusiveLatch<onesided::BTreeLeaf<uint64_t, uint64_t>> xlatch(random_page);
               auto latched = xlatch.try_latch();
               if (!latched) continue;
               uint64_t expected_key =
                   (random_page.plainOffset() - myt::my().remote_caches[random_page.getOwner()].begin_offset) /
                   BTREE_NODE_SIZE;
               ensure(expected_key == xlatch.local_copy->entries[0].key);
               xlatch.local_copy->entries[0].value++;
               xlatch.unlatch();
               page_updates++;
            }
            running_threads_counter--;
         });
      }
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) _mm_pause();
      comp.getWorkerPool().joinAll();
      comp.stopProfiler();
      barrier_wait();
      std::cout << "starting validation " << std::endl;
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         uint64_t sum = 0;
         for (auto page_ptr : remote_ht) {
            onesided::ExclusiveLatch<onesided::BTreeLeaf<uint64_t, uint64_t>> xlatch(page_ptr);
            auto latched = xlatch.try_latch();
            if (!latched) throw std::logic_error("not latchable in validation");
            uint64_t expected_key =
                (page_ptr.plainOffset() - myt::my().remote_caches[page_ptr.getOwner()].begin_offset) / BTREE_NODE_SIZE;
            ensure(expected_key == xlatch.local_copy->entries[0].key);
            sum += xlatch.local_copy->entries[0].value;
            xlatch.unlatch();
         }
         std::cout << " sum " << sum << " vs " << page_updates << std::endl;
         ensure(sum == page_updates);
      });
   }
   return 0;
}
