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
      std::mutex remote_ht_mtx;
      std::vector<RemotePtr> remote_ht;
      using Leaf = onesided::BTreeLeaf<Key, Value>;
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         for (size_t i = 0; i < 1000; i++) {
            onesided::AllocationLatch<Leaf> leaf;
            remote_ht_mtx.lock();
            ensure(i == remote_ht.size());
            leaf.local_copy->insert(i, 0);  // initialize index
            leaf.unlatch();
            remote_ht.push_back(leaf.remote_ptr);
            remote_ht_mtx.unlock();
         }
      });
      std::cout << "BEFORE BARRIER " << std::endl;
      barrier_wait();
      //=== Benchmark ===//
      profiling::EmptyWorkloadInfo wl;
      comp.startProfiler(wl);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::atomic<u64> page_updates{0};
      std::atomic<u64> latch_contentions{0};
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&]() {
            running_threads_counter++;
            while (keep_running) {
               const uint64_t rnd_idx = utils::RandomGenerator::getRandU64(0, remote_ht.size());
               auto rptr_page = remote_ht[rnd_idx];
               try {
                  // test move constructor 

               std::cout << "" << std::endl;
               std::cout << "begin" << std::endl;
                  onesided::GuardO<Leaf> leafO(rptr_page);
                  auto idx = leafO->lower_bound(rnd_idx);
                  if (idx == leafO->end() && leafO->key_at(idx) != rnd_idx)
                     throw std::logic_error("key  + std::to_string(rnd_idx)");
                  Value value = leafO->value_at(idx) + 1;
                  onesided::GuardX<Leaf> latch_to_fail(rptr_page); 
                  ensure(latch_to_fail.latch.latched);

                  std::cout << "latch fail " <<  latch_to_fail.latch.local_copy << "  " << std::endl;;
                  onesided::GuardX<Leaf> leaf2(std::move(leafO)); 
                  ensure(leaf2.latch.local_copy != latch_to_fail.latch.local_copy);
                  std::cout << leaf2.latch.local_copy << " " <<  latch_to_fail.latch.local_copy << std::endl;;
                  leaf2->update(rnd_idx, value);
                  page_updates++;
                  threads::onesided::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                  /*onesided::GuardO<Leaf> leafO(rptr_page);
                  auto idx = leafO->lower_bound(rnd_idx);
                  if (idx == leafO->end() && leafO->key_at(idx) != rnd_idx)
                     throw std::logic_error("key  + std::to_string(rnd_idx)");
                  Value value = leafO->value_at(idx) + 1;
                  onesided::GuardX<Leaf> leaf2(std::move(leafO)); 
                  leaf2->update(rnd_idx, value);
                  page_updates++;
                  ffreads::onesided::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);*/
               } catch (const onesided::OLCRestartException&) {
                  latch_contentions++;
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
      barrier_wait();
      std::cout << "starting validation " << std::endl;
      comp.getWorkerPool().scheduleJobSync(0, [&]() {
         uint64_t current_idx = 0;
         uint64_t sum = 0;
         std::for_each(std::begin(remote_ht), std::end(remote_ht), [&](RemotePtr rptr) {
            onesided::ExclusiveLatch<Leaf> leaf(rptr);
            if (!leaf.try_latch()) throw std::logic_error("should not be latched");
            auto idx = leaf.local_copy->lower_bound(current_idx);
            if (idx == leaf.local_copy->end() && leaf.local_copy->key_at(idx) != current_idx)
               throw std::logic_error("key not found");
            Value value = leaf.local_copy->value_at(idx);
            sum += value;
            current_idx++;
            leaf.unlatch();
         });
         std::cout << "sum " << sum << " updates " << page_updates << std::endl;
         std::cout << "latch_contentions " << latch_contentions << std::endl;
      });
   }
   return 0;
}
