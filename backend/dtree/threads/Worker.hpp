#pragma once
#include <sys/types.h>

#include <cstdint>
#include <span>

#include "Defs.hpp"
#include "ThreadContext.hpp"
#include "dtree/profiling/counters/CPUCounters.hpp"
#include "dtree/profiling/counters/WorkerCounters.hpp"
#include "dtree/rdma/CommunicationManager.hpp"
#include "dtree/rdma/messages/Messages.hpp"
#include "dtree/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
namespace dtree {
namespace threads {
using namespace rdma;
constexpr static bool OPTIMIZED_COMPLETION = true;
// -------------------------------------------------------------------------------------
struct Worker {
   // -------------------------------------------------------------------------------------
   static thread_local Worker* tlsPtr;
   static inline Worker& my() { return *Worker::tlsPtr; }
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
   rdma::CM<rdma::InitMessage>& cm;
   NodeID nodeId_;
   std::vector<ConnectionContext> cctxs;
   std::unique_ptr<ThreadContext> threadContext;
   // -------------------------------------------------------------------------------------
   uintptr_t barrier;
   RemotePtr metadataPage;
   // -------------------------------------------------------------------------------------
   uint8_t* tl_rdma_buffer;
   uint64_t* cas_buffer; // cache line sized 
   // -------------------------------------------------------------------------------------
   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
   ~Worker();
   // -------------------------------------------------------------------------------------
   inline void backoff() {
      auto p_ops = dtree::utils::RandomGenerator::getRandU64(0, 128);
      for (uint64_t i = 0; i < p_ops; i++) _mm_pause();
   }

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

   //=== low-level RDMA wrappers ===//

   
   
   template <typename T>
   void remote_write(RemotePtr remote_ptr, /*must be RDMA memory*/ T* local_copy) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postWrite(const_cast<T*>(local_copy), *(cctxs[nodeId].rctx), rdma::completion::signaled, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = rdma::pollCompletion(cctxs[0].rctx->id->qp->send_cq, 1, &wcReturn);
            if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
        }
   }

   template <typename T>
   T* remote_read(RemotePtr remote_ptr) {
      ensure(sizeof(T) <= THREAD_LOCAL_RDMA_BUFFER);
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      volatile T* remote_copy = reinterpret_cast<T*>(tl_rdma_buffer);

      rdma::postRead(const_cast<T*>(remote_copy), *(cctxs[nodeId].rctx), rdma::completion::signaled, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         comp = rdma::pollCompletion(cctxs[0].rctx->id->qp->send_cq, 1, &wcReturn);
            if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
        }

      return const_cast<T*>(remote_copy);
   }

   // returns old value; before increment
   uint64_t fetchAdd(uint64_t increment, RemotePtr remote_ptr, rdma::completion wc) {
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
      rdma::postFetchAdd(increment, old, *(cctxs[nodeId].rctx), wc, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (wc == rdma::completion::signaled && comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
      return *old;
   }
   bool pollCompletionCS(RemotePtr remote_ptr, uint64_t expected) {
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
   void compareSwapAsync(uint64_t expected, uint64_t desired, RemotePtr remote_ptr, rdma::completion wc) {
      auto* old = reinterpret_cast<uint64_t*>(cas_buffer);
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();
      rdma::postCompareSwap(expected, desired, old, *(cctxs[nodeId].rctx), wc, addr);
   }
   // returns true if succeeded
   bool compareSwap(uint64_t expected, uint64_t desired, RemotePtr remote_ptr, rdma::completion wc) {
      auto* old = reinterpret_cast<uint64_t*>(cas_buffer);
      auto nodeId = remote_ptr.getOwner();
      auto addr = remote_ptr.plainOffset();

      rdma::postCompareSwap(expected, desired, old, *(cctxs[nodeId].rctx), wc, addr);
      int comp{0};
      ibv_wc wcReturn;
      while (wc == rdma::completion::signaled && comp == 0) {
         comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
         if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
      }
      return (*old == expected);
   }

   //=== barrier ===//

   void rdma_barrier_wait(uint64_t stage) {
      {
         auto* old = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
         rdma::postFetchAdd(1, old, *(cctxs[0].rctx), rdma::completion::signaled, barrier);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[0].rctx->id->qp->send_cq, 1, &wcReturn);
            if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
         }
      }

      volatile auto* barrier_value = reinterpret_cast<uint64_t*>(tl_rdma_buffer);
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
      std::cout << " barrier " << *barrier_value << " == " << expected << std::endl;
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
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace dtree