#pragma once
#include <span>
#include "Defs.hpp"
#include "ThreadContext.hpp"
#include "farm/profiling/counters/CPUCounters.hpp"
#include "farm/profiling/counters/WorkerCounters.hpp"
#include "farm/rdma/CommunicationManager.hpp"
#include "farm/utils/FarmHelper.hpp"
#include "farm/utils/RandomGenerator.hpp"
#include "farm/rdma/messages/Messages.hpp"
// -------------------------------------------------------------------------------------
namespace farm {
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
   struct PartitionInfo {
      uintptr_t offset;
      uint64_t begin;
      uint64_t end;
      NodeID nodeId;
   };

   struct Tables {
      std::vector<PartitionInfo> partitions;
      void addPartition(PartitionInfo pInfo) { partitions.push_back(pInfo); }
      // -------------------------------------------------------------------------------------
      PartitionInfo& getPartition(uint64_t t_id) {
         for (auto& p : partitions) {
            if (t_id >= p.begin && t_id < p.end) return p;
         }
         throw;
      }
   };

   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& cm;
   NodeID nodeId_;
   std::vector<ConnectionContext> cctxs;
   std::unique_ptr<ThreadContext> threadContext;
   std::vector<Tables> tables;
   // -------------------------------------------------------------------------------------
   uintptr_t barrier;
   // -------------------------------------------------------------------------------------
   uint8_t* tl_rdma_buffer;
   // -------------------------------------------------------------------------------------
   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
   ~Worker();
   // -------------------------------------------------------------------------------------
   inline void backoff() {
      auto p_ops = farm::utils::RandomGenerator::getRandU64(0, 128);
      for (uint64_t i = 0; i < p_ops; i++)
         _mm_pause();
   }

   bool insert(NodeID nodeId, Key key, Value value){
      auto& request = *MessageFabric::createMessage<InsertRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.key = key;
      request.value = value;
      auto& response = writeMsgSync<rdma::InsertResponse>(nodeId, request);
      if(response.rc == rdma::RESULT::ABORTED){
         return false;
      }
      return true;
   }

   bool lookup(NodeID nodeId, Key key, Value& returnValue){
      auto& request = *MessageFabric::createMessage<LookupRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.key = key;
      auto& response = writeMsgSync<rdma::LookupResponse>(nodeId, request);
      if(response.rc == rdma::RESULT::ABORTED){
         return false;
      }
      returnValue = response.value;
      return true;
   }

   std::span<KVPair> scan(NodeID nodeId, Key from, Key to){
      auto& request = *MessageFabric::createMessage<ScanRequest>(cctxs[nodeId].outgoing);
      request.nodeId = nodeId_;
      request.from = from;
      request.to = to;
      auto& response = writeMsgSync<rdma::ScanResponse>(nodeId, request);
      if(response.rc == rdma::RESULT::ABORTED){
         return std::span<KVPair>();
      }
      std::cout << "Length " << response.length << "\n";
      return std::span<KVPair>(cctxs[nodeId].result_buffer, response.length);      
   }
   
   // -------------------------------------------------------------------------------------  
   // template <class Record>
   // Record latchfreeReadRecord(NodeID nodeId, uintptr_t addr) {
   //    using namespace farm::utils;
   //    TypedFaRMTuple<Record>* f = reinterpret_cast<TypedFaRMTuple<Record>*>(tl_rdma_buffer);
   //    while (true) {
   //       if (nodeId == nodeId_) {
   //          std::memcpy(tl_rdma_buffer,(uint8_t*)addr, sizeof(TypedFaRMTuple<Record>));
   //       } else {
   //          // rdma read
   //          rdma::postRead(f, *(cctxs[nodeId].rctx), rdma::completion::signaled, addr);
   //          // -------------------------------------------------------------------------------------
   //          int comp{0};
   //          ibv_wc wcReturn;
   //          while (comp == 0) {
   //             comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
   //             if (comp > 0 && wcReturn.status != IBV_WC_SUCCESS) throw;
   //          }
   //       }
   //       ensure(f);
   //       if (f->consistent()) break;
   //       backoff(); // retry
   //    }

   //    Record record;
   //    utils::fromFaRM(f->cachelines,record);
   //    return record;
   // }

   // -------------------------------------------------------------------------------------
   // template <class Record>
   // bool writeRecord(Record& record, NodeID nodeId, uintptr_t addr) {
   //    using namespace farm::utils;
   //    TypedFaRMTuple<Record>* f = reinterpret_cast<TypedFaRMTuple<Record>*>(tl_rdma_buffer);
   //    if(!f->latch()) throw std::logic_error("thread local buffer should be latchable");
   //    // -------------------------------------------------------------------------------------
   //    // copy new record to tl buffer
   //    utils::toFaRM(record,f); 
   //    // -------------------------------------------------------------------------------------
   //    if (nodeId == nodeId_) {
   //      TypedFaRMTuple<Record>* target = reinterpret_cast<TypedFaRMTuple<Record>*>(addr);
   //      if (!target->latch()) return false;
   //      if(!f->compareVersions(*target)){
   //         target->unlatch();
   //         backoff();
   //         return false;
   //      }
   //      *target = *f;
   //      target->unlatch();
   //    } else {
   //       // send {addr,f} to remote node node id and wait for response
   //       auto& request = *MessageFabric::createMessage<RemoteWriteRequest>(cctxs[nodeId].outgoing);
   //       request.nodeId = nodeId_;
   //       request.addr = addr;
   //       std::memcpy(&request.buffer, f, sizeof(TypedFaRMTuple<Record>));
   //       auto& response = writeMsgSync<rdma::RemoteWriteResponse>(nodeId, request);
   //       ensure(request.addr == response.addr);
   //       if(response.rc == rdma::RESULT::ABORTED){
   //          return false;
   //       }
   //    }
   //    return true;
   // }


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
      std::cout << " barrier " << *barrier_value << " == "<<  expected << std::endl;
   }
   
   template <typename MSG>
   void writeMsg(NodeID nodeId, MSG& msg)
      {
         rdma::completion  signal =  rdma::completion::signaled;
         uint8_t flag = 1;
         // -------------------------------------------------------------------------------------
         rdma::postWrite(&msg, *(cctxs[nodeId].rctx), rdma::completion::unsignaled,cctxs[nodeId].plOffset);
         rdma::postWrite(&flag, *(cctxs[nodeId].rctx), signal ,cctxs[nodeId].mbOffset);
         // -------------------------------------------------------------------------------------
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0) {
               comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
            }
      }
   
   template <typename RESPONSE, typename MSG>
   RESPONSE& writeMsgSync(NodeID nodeId, MSG& msg)
      {
         // -------------------------------------------------------------------------------------
         auto& response = *static_cast<RESPONSE*>(cctxs[nodeId].incoming);
         response.receiveFlag = 0;
         volatile uint8_t& received = response.receiveFlag;
         // -------------------------------------------------------------------------------------
         writeMsg(nodeId, msg);
         // -------------------------------------------------------------------------------------
         while (received == 0) {
            _mm_pause();
         }
         return response;
      }


   
};
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace farm
