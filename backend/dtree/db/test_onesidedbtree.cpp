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
<<<<<<< HEAD
      while (true) {
         sleep(1);

         auto* nodes = static_cast<onesided::BTreeLeaf<uint64_t, uint64_t>*>(static_cast<void*>(store.node_buffer));
         for (size_t i = 0; i < 10; i++) {
            std::cout << "latched " << nodes[i].remote_latch << " version  "  << nodes[i].version 
               << " value at 0 " << nodes[i].value_at(0) 
               << std::endl;
         }
         std::cout << " " << std::endl;
      }
=======
      while (true) { std::cout << *store.cache_counter << std::endl; }
>>>>>>> parent of 93a0d75 (version bug)
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
         for (size_t i = 0; i < 100; i++) {
            onesided::AllocationLatch<Leaf> leaf;
            remote_ht_mtx.lock();
            ensure(i == remote_ht.size());
<<<<<<< HEAD
            for (Key k_i = 0; k_i < 100; k_i++) { leaf->insert(k_i, 0); }
            std::cout << "allocation version " << leaf.version << std::endl;
=======
            leaf->insert(i, 0);  // initialize index
>>>>>>> parent of 93a0d75 (version bug)
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
<<<<<<< HEAD
                  // if (utils::RandomGenerator::getRandU64(0, 100) >= 50) {
                  if (true) {
                     onesided::GuardX<Leaf> leafX(rptr_page);
                     for (Key k_i = 0; k_i < 100; k_i++) {
                        Value value = leafX->value_at(k_i) + 1;
                        leafX->update(k_i, value);
                     }
                     page_updates++;
                  } else {
                     onesided::GuardO<Leaf> leafO(rptr_page);
                     read = true;
                     reads++;
                     Value zero_value = leafO->value_at(0);
                     for (Key k_i = 0; k_i < 100; k_i++) {
                        Value value = leafO->value_at(k_i);
                        if (value != zero_value) {
                           std::cout << t_i << " threads value " << value << " zero_value " << zero_value << " k_i "
                                     << k_i << " in page " << rptr_page << std::endl;
                           consistent = false;
                        }
                     }
                  }
=======
                  // test move constructor 
                  onesided::GuardO<Leaf> leafO(rptr_page);
                  auto idx = leafO->lower_bound(rnd_idx);
                  if (idx == leafO->end() && leafO->key_at(idx) != rnd_idx)
                     throw std::logic_error("key  + std::to_string(rnd_idx)");
                  Value value = leafO->value_at(idx) + 1;
                  onesided::GuardX<Leaf> leaf2(std::move(leafO)); 
                  leaf2->update(rnd_idx, value);
                  page_updates++;
                  threads::onesided::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
>>>>>>> parent of 93a0d75 (version bug)
               } catch (const onesided::OLCRestartException&) {
                  latch_contentions++;
                  ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
               }
<<<<<<< HEAD
               threads::onesided::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               if (!consistent) { throw std::logic_error("inconcistency not caught " + std::to_string(t_i)); }
=======
>>>>>>> parent of 93a0d75 (version bug)
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
<<<<<<< HEAD
         [[maybe_unused]] uint64_t sum = 0;
         [[maybe_unused]] uint64_t current_idx = 0;
         std::for_each(std::begin(remote_ht), std::begin(remote_ht) + USED_PAGES, [&](RemotePtr rptr) {
            current_idx++;
            std::cout << "page id " << current_idx << std::endl;
            onesided::GuardX<Leaf> leaf(rptr);
            [[maybe_unused]] Value value = leaf->value_at(0);
            // sum += value;
            std::cout << "page Version " << leaf.latch.version << std::endl;
            std::cout << "page Version inside leaf " << leaf->version << std::endl;
            std::cout << "page Version inside leaf casted " << leaf.latch.rdma_mem.local_copy->version << std::endl;
            std::cout << "value " << value << std::endl;
=======
         uint64_t current_idx = 0;
         uint64_t sum = 0;
         std::for_each(std::begin(remote_ht), std::end(remote_ht), [&](RemotePtr rptr) {
            onesided::GuardX<Leaf> leaf(rptr);
            auto idx = leaf->lower_bound(current_idx);
            if (idx == leaf->end() && leaf->key_at(idx) != current_idx)
               throw std::logic_error("key not found");
            Value value = leaf->value_at(idx);
            sum += value;
            current_idx++;
>>>>>>> parent of 93a0d75 (version bug)
         });
         std::cout << "sum " << sum << " updates " << page_updates << std::endl;
         std::cout << "latch_contentions " << latch_contentions << std::endl;
      });
   }
   return 0;
}
