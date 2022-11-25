#pragma once
// -------------------------------------------------------------------------------------
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
#include "db/PartitionedTable.hpp"
#include "farm/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace farm
{
// -------------------------------------------------------------------------------------
// avoids destruction of objects before remote side finished
struct RemoteGuard{
   std::atomic<uint64_t>& numberRemoteConnected;
   RemoteGuard(std::atomic<uint64_t>& numberRemoteConnected) : numberRemoteConnected(numberRemoteConnected){};
   ~RemoteGuard(){ while(numberRemoteConnected);}
};

// -------------------------------------------------------------------------------------
class FaRM
{
  public:
   //! Default constructor
   FaRM();
   //! Destructor
   ~FaRM();
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   FaRM(const FaRM& other) = delete;
   //! Move constructor
   FaRM(FaRM&& other) noexcept = delete;
   //! Copy assignment operator
   FaRM& operator=(const FaRM& other) = delete;
   //! Move assignment operator
   FaRM& operator=(FaRM&& other) noexcept = delete;
   // -------------------------------------------------------------------------------------
   threads::WorkerPool& getWorkerPool() { return *workerPool; }
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
   void stopProfiler()
   {
      if (pt.running == true) {
         pt.running = false;
         for (auto& p : profilingThread)
            p.join();
         profilingThread.clear();
      }
      std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
   };
      
   // -------------------------------------------------------------------------------------
   void startAndConnect() {
      mh = std::make_unique<rdma::MessageHandler>(*cm, *this, nodeId);
      workerPool = std::make_unique<threads::WorkerPool>(*cm, nodeId);
   };

   template<class Record>
   void registerTable(std::string name, uint64_t begin, uint64_t end){
      if (catalog.count(name)) throw;
      catalog[name] = new db::PartitionedTable(Record::id, ((db::numberFaRMCachelines<Record>())*64),begin,end,nodeId,*cm);
   }

   db::PartitionedTable& getTable(std::string name){
      if (!catalog.count(name)) throw;
      return *catalog[name];
   }

   uint64_t* barrier;
   std::unordered_map<std::string,db::PartitionedTable*> catalog;
   
  private:
   NodeID nodeId = 0;
   std::unique_ptr<rdma::MessageHandler> mh;
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<threads::WorkerPool> workerPool;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;


};
// -------------------------------------------------------------------------------------
}  // namespace scalestore
