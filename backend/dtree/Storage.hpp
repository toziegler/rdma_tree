#pragma once
// -------------------------------------------------------------------------------------
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
#include "db/btree.hpp"
#include "db/OneSidedTypes.hpp"
#include "dtree/utils/RandomGenerator.hpp"
#include "dtree/db/OneSidedBTree.hpp"
#include "dtree/db/OneSidedTypes.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace dtree
{
// -------------------------------------------------------------------------------------
// avoids destruction of objects before remote side finished
struct RemoteGuard{
   std::atomic<uint64_t>& numberRemoteConnected;
   RemoteGuard(std::atomic<uint64_t>& numberRemoteConnected) : numberRemoteConnected(numberRemoteConnected){};
   ~RemoteGuard(){ while(numberRemoteConnected);}
};

// -------------------------------------------------------------------------------------
class Storage
{
  public:
   //! Default constructor
   Storage();
   //! Destructor
   ~Storage();
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   Storage(const Storage& other) = delete;
   //! Move constructor
   Storage(Storage&& other) noexcept = delete;
   //! Copy assignment operator
   Storage& operator=(const Storage& other) = delete;
   //! Move assignment operator
   Storage& operator=(Storage&& other) noexcept = delete;
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
   void startMessageHandler() {
      mh = std::make_unique<rdma::MessageHandler>(*cm, *this, nodeId);
   };

   std::atomic<uint64_t>& getConnectedClients(){
      return mh->connectedClients;
   }

   auto& getTree(){
      return tree;
   }
   
   uint64_t* barrier;
   uint64_t* cache_counter;
   onesided::MetadataPage* md;
   uint8_t *node_buffer {nullptr};
   dtree::onesided::BTreeLeaf<Key, Value>* root ;
  private:
   NodeID nodeId = 0;
   twosided::BTree<Key,Value> tree;
   std::unique_ptr<rdma::MessageHandler> mh;
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;
   

};
// -------------------------------------------------------------------------------------
}  // namespace scalestore
