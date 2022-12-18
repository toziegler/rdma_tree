#pragma once
// -------------------------------------------------------------------------------------
#include "dtree/utils/RandomGenerator.hpp"
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace dtree {

// -------------------------------------------------------------------------------------
template <typename WorkerType>
class Compute {
  public:
   Compute() {
      cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
      rdmaCounters = std::make_unique<profiling::RDMACounters>();
   }

   ~Compute() {
      stopProfiler();
      workerPool.reset();  // important clients need to disconnect first
   }
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   Compute(const Compute& other) = delete;
   //! Move constructor
   Compute(Compute&& other) noexcept = delete;
   //! Copy assignment operator
   Compute& operator=(const Compute& other) = delete;
   //! Move assignment operator
   Compute& operator=(Compute&& other) noexcept = delete;
   // -------------------------------------------------------------------------------------
   threads::WorkerPool<WorkerType>& getWorkerPool() { return *workerPool; }
   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& getCM() { return *cm; }
   // -------------------------------------------------------------------------------------
   NodeID getNodeID() { return nodeId; }
   // -------------------------------------------------------------------------------------
   void startProfiler(profiling::WorkloadInfo& wlInfo) {
      pt.running = true;
      profilingThread.emplace_back(&profiling::ProfilingThread::profile, &pt, nodeId, std::ref(wlInfo));
   }
   // -------------------------------------------------------------------------------------
   void stopProfiler() {
      if (pt.running == true) {
         pt.running = false;
         for (auto& p : profilingThread) p.join();
         profilingThread.clear();
      }
      std::locale::global(std::locale("C"));  // hack to restore locale which is messed up in tabulate package
   };

   // -------------------------------------------------------------------------------------
   void startAndConnect() { workerPool = std::make_unique<threads::WorkerPool<threads::Worker>>(*cm, nodeId); };

  private:
   NodeID nodeId = 0;
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<threads::WorkerPool<WorkerType>> workerPool;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;
};
// -------------------------------------------------------------------------------------
}  // namespace dtree
