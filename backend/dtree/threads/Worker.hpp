#pragma once
#include <alloca.h>
#include <sys/types.h>

#include <cstdint>
#include <iostream>
#include <span>
#include <stdexcept>

#include "Defs.hpp"
#include "dtree/db/OneSidedTypes.hpp"
#include "dtree/profiling/counters/CPUCounters.hpp"
#include "dtree/profiling/counters/WorkerCounters.hpp"
#include "dtree/rdma/CommunicationManager.hpp"
#include "dtree/rdma/messages/Messages.hpp"
#include "dtree/utils/BatchQueue.hpp"
#include "dtree/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
namespace dtree {
namespace threads {
using namespace rdma;
struct AbstractWorker {
   // -------------------------------------------------------------------------------------
   // static thread_local twosided::Worker* tlsPtr;
   // static inline twosided::Worker& my() { return *twosided::Worker::tlsPtr; }
   // -------------------------------------------------------------------------------------
   uint64_t workerId;
   std::string name;
   // -------------------------------------------------------------------------------------
   profiling::CPUCounters cpuCounters;
   // -------------------------------------------------------------------------------------
   profiling::WorkerCounters counters;
   // -------------------------------------------------------------------------------------
   // RDMA
   // -------------------------------------------------------------------------------------
   // context for every connection
   struct ConnectionContext {
      uintptr_t mbOffset;
      uintptr_t plOffset;
      rdma::Message* incoming;
      rdma::Message* outgoing;
      rdma::RdmaContext* rctx;
      KVPair* result_buffer;
      uint64_t wqe;  // wqe currently outstanding
   };
   // -------------------------------------------------------------------------------------
   // should be in onesided version but is easier
   struct RemoteCacheInfo {
      RemotePtr counter;
      uintptr_t begin_offset;
   };
                                                 // 
   rdma::CM<rdma::InitMessage>& cm;
   NodeID nodeId_;
   std::vector<ConnectionContext> cctxs;
   std::vector<RemoteCacheInfo> remote_caches;   // counter addr
   uint64_t* barrier_buffer{nullptr};
   // -------------------------------------------------------------------------------------
   uintptr_t barrier;  // barrier address
   RemotePtr metadataPage;
   // -------------------------------------------------------------------------------------
   AbstractWorker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
   virtual ~AbstractWorker();
   //=== barrier ===//
   void rdma_barrier_wait(uint64_t stage) {
      {
         auto* old = barrier_buffer;
         rdma::postFetchAdd(1, old, *(cctxs[0].rctx), rdma::completion::signaled, barrier);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[0].rctx->id->qp->send_cq, 1, &wcReturn);
            if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
         }
      }

      volatile auto* barrier_value = barrier_buffer;
      uint64_t expected = (FLAGS_compute_nodes * FLAGS_worker) * stage;

      while (*barrier_value != expected) {
         rdma::postRead(const_cast<uint64_t*>(barrier_value), *(cctxs[0].rctx), rdma::completion::signaled, barrier);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[0].rctx->id->qp->send_cq, 1, &wcReturn);
            if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
         }
      }
   }

   template <typename MSG>
   void writeMsg(NodeID nodeId, MSG& msg) {
      rdma::completion signal = rdma::completion::signaled;
      uint8_t flag = 1;
      // -------------------------------------------------------------------------------------
      rdma::postWrite(&msg, *(cctxs[nodeId].rctx), rdma::completion::unsignaled, cctxs[nodeId].plOffset);
      rdma::postWrite(&flag, *(cctxs[nodeId].rctx), signal, cctxs[nodeId].mbOffset);
      // -------------------------------------------------------------------------------------
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) { comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn); }
   }

   template <typename RESPONSE, typename MSG>
   RESPONSE& writeMsgSync(NodeID nodeId, MSG& msg) {
      // -------------------------------------------------------------------------------------
      auto& response = *static_cast<RESPONSE*>(cctxs[nodeId].incoming);
      response.receiveFlag = 0;
      volatile uint8_t& received = response.receiveFlag;
      // -------------------------------------------------------------------------------------
      writeMsg(nodeId, msg);
      // -------------------------------------------------------------------------------------
      while (received == 0) { _mm_pause(); }
      return response;
   }
};
namespace twosided {
// -------------------------------------------------------------------------------------
struct Worker : public AbstractWorker {
   // -------------------------------------------------------------------------------------
   static thread_local twosided::Worker* tlsPtr;
   static inline twosided::Worker& my() { return *twosided::Worker::tlsPtr; }
   // -------------------------------------------------------------------------------------
   using super = AbstractWorker;
   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId)
       : AbstractWorker(workerId, name, cm, nodeId) {}
   ~Worker() = default;
   // ------------------------------------------------------------------------------------- 
   //=== two-sided tree stub ===//
   bool insert(NodeID nodeId, Key key, Value value) {
      auto& request = *MessageFabric::createMessage<InsertRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.key = key;
      request.value = value;
      auto& response = writeMsgSync<rdma::InsertResponse>(nodeId, request);
      if (response.rc == rdma::RESULT::ABORTED) { return false; }
      return true;
   }

   bool lookup(NodeID nodeId, Key key, Value& returnValue) {
      auto& request = *MessageFabric::createMessage<LookupRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.key = key;
      auto& response = writeMsgSync<rdma::LookupResponse>(nodeId, request);
      if (response.rc == rdma::RESULT::ABORTED) { return false; }
      returnValue = response.value;
      return true;
   }

   std::span<KVPair> scan(NodeID nodeId, Key from, Key to) {
      auto& request = *MessageFabric::createMessage<ScanRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.from = from;
      request.to = to;
      auto& response = writeMsgSync<rdma::ScanResponse>(nodeId, request);
      if (response.rc == rdma::RESULT::ABORTED) { return std::span<KVPair>(); }
      return std::span<KVPair>(cctxs[nodeId].result_buffer, response.length);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace twosided
namespace onesided{
using namespace rdma;
   using namespace ::dtree::onesided;
struct Worker : public AbstractWorker{
   static thread_local onesided::Worker* tlsPtr;
   static inline onesided::Worker& my() { return *onesided::Worker::tlsPtr; }
   utils::Stack<RemotePtr, TL_CACHE_SIZE> remote_pages;
   utils::Stack<RDMAMemoryInfo, CONCURRENT_LATCHES> local_rmemory; // local rdma memory used by the latches not really nicely encapsulated but fine

   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
  ~Worker() = default;
 
   template <typename T>
   void remote_write(RemotePtr remote_ptr, /*must be RDMA memory*/ T* local_copy, rdma::completion wc) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postWrite(const_cast<T*>(local_copy), *(cctxs[nodeId].rctx), wc, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (wc == rdma::completion::signaled && comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }

   void read_latch(RemotePtr remote_ptr, onesided::PageHeader* /*RDMA memory*/ local_copy) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postRead(const_cast<onesided::PageHeader*>(local_copy), *(cctxs[nodeId].rctx), rdma::completion::signaled,
                     addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }

   template <typename T>
   void remote_read(RemotePtr remote_ptr, T* local_copy) {
      ensure(sizeof(T) <= THREAD_LOCAL_RDMA_BUFFER);
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postRead(const_cast<T*>(local_copy), *(cctxs[nodeId].rctx), rdma::completion::signaled, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
   }

   void refresh_caches() {
      uint64_t per_node_cache = TL_CACHE_SIZE / fLU64::FLAGS_storage_nodes;
      for (size_t i = 0; i < FLAGS_storage_nodes; i++) {
         auto begin_idx = fetchAdd(per_node_cache, remote_caches[i].counter, rdma::completion::signaled, barrier_buffer);
         for (auto p_idx = begin_idx; p_idx < begin_idx + per_node_cache; p_idx++) {
            RemotePtr addr(i, (p_idx * BTREE_NODE_SIZE) + remote_caches[i].begin_offset);
            ensure(remote_pages.try_push(addr));
         }
      }
   }

   // returns old value; before increment
   uint64_t fetchAdd(uint64_t increment, RemotePtr remote_ptr, rdma::completion wc, uint64_t* /*RDMA Memory*/ cas_buffer ) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postFetchAdd(increment, cas_buffer, *(cctxs[nodeId].rctx), wc, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (wc == rdma::completion::signaled && comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
      return *cas_buffer;
   }
   // note should be the same cas_buffer as compareSwapAsync has been called with 
   bool pollCompletionAsyncCAS(RemotePtr remote_ptr, uint64_t expected, uint64_t* cas_buffer) {
      int comp{0};
      ibv_wc wcReturn;
      auto nodeId = remote_ptr.getOwner();

      while (comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
      auto* old = reinterpret_cast<uint64_t*>(cas_buffer);
      return (*old == expected);
   }
   void compareSwapAsync(uint64_t expected, uint64_t desired, RemotePtr remote_ptr, rdma::completion wc, uint64_t* /*RDMA Memory*/ cas_buffer ) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postCompareSwap(expected, desired, cas_buffer, *(cctxs[nodeId].rctx), wc, addr);
   }
//    returns true if succeeded
   bool compareSwap(uint64_t expected, uint64_t desired, RemotePtr remote_ptr, rdma::completion wc, uint64_t* /*RDMA Memory*/ cas_buffer  ) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postCompareSwap(expected, desired, cas_buffer, *(cctxs[nodeId].rctx), wc, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (wc == rdma::completion::signaled && comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
      return (*cas_buffer == expected);
   }
};
 //-------------------------------------------------------------------------------------
}  // namespace twosided*/
}  // namespace threads
}  // namespace dtree
