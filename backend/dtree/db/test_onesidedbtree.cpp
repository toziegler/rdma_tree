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
#include <fstream>
#include <iostream>
#include <random>
// -------------------------------------------------------------------------------------

DEFINE_uint32(run_for_seconds, 5, "");

//=== Storage Logic ===//
void storage_node() {
   using namespace dtree;
   Storage store;
   profiling::EmptyWorkloadInfo wl;
   store.startProfiler(wl);
   store.startMessageHandler();
   {
      while (store.getConnectedClients() == 0)
         ;
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
      barrier_wait();
      //=== Benchmark ===//
      profiling::EmptyWorkloadInfo wl;
      comp.startProfiler(wl);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::atomic<u64> latch_updates {0};
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            for (; keep_running; threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p)) {
               // read metadat page and update
               // read again to see if updates get persisted
               auto metadata_ptr = threads::Worker::my().metadataPage;

               onesided::OptimisticLatch<onesided::MetadataPage> olatch(metadata_ptr);
               auto latched = olatch.try_latch();
               if(!latched){
                  std::cout << "not latched" << "\n";
               }
               auto* md = olatch.local_copy;
               ensure(md->type == onesided::PType_t::METADATA);
               std::cout << md->getRootPtr() << std::endl;
               
               if(!olatch.validate()){
                  std::cout << "Validation failed " << "\n";
               }
               

               /*
               onesided::ExclusiveLatch<onesided::MetadataPage> xlatch (metadata_ptr);
               auto latched = xlatch.try_latch();
               if(!latched) continue;
               latch_updates++;
               auto* md = xlatch.local_copy;
               ensure(md->type == onesided::PType_t::METADATA);
               md->setRootPtr({0, md->getRootPtr().offset + 1});
               xlatch.unlatch();
               */
            }
            running_threads_counter--;
         });
      }
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) _mm_pause();
      comp.getWorkerPool().joinAll();
      comp.stopProfiler();
      std::cout << "latch updates" << latch_updates << "\n";
   }
   return 0;
}
