#pragma once
// -------------------------------------------------------------------------------------
#include "CommunicationManager.hpp"
#include "messages/Messages.hpp"
// -------------------------------------------------------------------------------------
#include <bitset>
#include <iostream>
#include <arpa/inet.h>
#include <libaio.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace dtree {
struct Storage;
namespace rdma {
// -------------------------------------------------------------------------------------
struct MessageHandler {

   struct MHEndpoint {
      rdma::RdmaContext* rctx = nullptr;
      uint64_t wqe = 0;
   };
   // -------------------------------------------------------------------------------------
   struct alignas(CACHE_LINE) ConnectionContext {
      uintptr_t plOffset {0};       // does not have mailbox just payload with flag
      uintptr_t result_buffer {0};
      KVPair* scan_buffer {nullptr};
      rdma::Message* request {nullptr};   // rdma pointer
      rdma::Message* response {nullptr};  // in current protocol only one message can be outstanding per client
      rdma::RdmaContext* rctx {nullptr};
      uint64_t wqe {0};  // wqe currently outstanding
      NodeID bmId {0};   // id to which incoming client connection belongs
      uint64_t retries = 0;

      // remote mailboxes for each remote MH in order to allow MH to delegate requests
      std::vector<uintptr_t> remoteMbOffsets;
      std::vector<uintptr_t> remotePlOffsets;
   };
   // -------------------------------------------------------------------------------------
   // Mailbox partition per thread
   struct MailboxPartition {
      MailboxPartition() = default;
      MailboxPartition(uint8_t* mailboxes, uint64_t numberMailboxes, uint64_t beginId)
          : mailboxes(mailboxes), numberMailboxes(numberMailboxes), beginId(beginId){};
      uint8_t* mailboxes = nullptr;
      uint64_t numberMailboxes;
      uint64_t beginId;
   };
   // -------------------------------------------------------------------------------------
   MessageHandler(rdma::CM<InitMessage>& cm, Storage& db, NodeID nodeId);
   ~MessageHandler();
   // -------------------------------------------------------------------------------------
   void startThread();
   void stopThread();
   void init();
   // -------------------------------------------------------------------------------------
   std::atomic<bool> threadsRunning = true;
   std::atomic<size_t> threadCount = 0;
   rdma::CM<InitMessage>& cm;
   Storage& db;
   // -------------------------------------------------------------------------------------
   NodeID nodeId;
   std::vector<ConnectionContext> cctxs;
   std::vector<MailboxPartition> mbPartitions;
   std::atomic<uint64_t> connectedClients = 0;
   std::atomic<bool> finishedInit = false;
   // -------------------------------------------------------------------------------------   

   template <typename MSG>
   void writeMsg(NodeID clientId, MSG& msg)
   {
      auto& wqe = cctxs[clientId].wqe;
      uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
      rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      rdma::postWrite(&msg, *(cctxs[clientId].rctx), signal, cctxs[clientId].plOffset);

      if ((wqe & SIGNAL_) == SIGNAL_) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[clientId].rctx->id->qp->send_cq, 1, &wcReturn);
         }
         if (wcReturn.status != IBV_WC_SUCCESS)
            throw;
      }
      wqe++;
   }
   // // pointer
   // template <typename MSG>
   // void writeMsg(NodeID clientId, MSG* msg, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   // {
   //    auto& wqe = cctxs[clientId].wqe;
   //    uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
   //    rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
   //    rdma::postWrite(msg, *(cctxs[clientId].rctx), signal, cctxs[clientId].plOffset);

   //    if ((wqe & SIGNAL_) == SIGNAL_) {
   //       int comp{0};
   //       ibv_wc wcReturn;
   //       while (comp == 0) {
   //          comp = rdma::pollCompletion(cctxs[clientId].rctx->id->qp->send_cq, 1, &wcReturn);
   //       }
   //       if (wcReturn.status != IBV_WC_SUCCESS)
   //          throw;

   //       // reclaim invalidation
   //       reclaimInvalidations(cctxs[clientId], page_handle);
   //       std::swap(cctxs[clientId].activeInvalidationBatch, cctxs[clientId].passiveInvalidationBatch);
   //    }
   //    wqe++;
   // }
};  // namespace rdma
}  // namespace rdma
}  // namespace dtree
