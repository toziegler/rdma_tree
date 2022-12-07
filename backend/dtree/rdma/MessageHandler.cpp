#include "MessageHandler.hpp"
#include "Defs.hpp"
#include "dtree/Storage.hpp"
#include "dtree/threads/CoreManager.hpp"
#include "dtree/threads/ThreadContext.hpp"
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>

namespace dtree {
namespace rdma {
MessageHandler::MessageHandler(rdma::CM<InitMessage>& cm, Storage& db, NodeID nodeId)
    : cm(cm), db(db), nodeId(nodeId), mbPartitions(FLAGS_messageHandlerThreads) {
   // partition mailboxes
   size_t n = (FLAGS_worker) * (FLAGS_compute_nodes);
   if (n > 0) {
      ensure(FLAGS_messageHandlerThreads <= n);  // avoid over subscribing message handler threads
      const uint64_t blockSize = n / FLAGS_messageHandlerThreads;
      ensure(blockSize > 0);
      for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++) {
         auto begin = t_i * blockSize;
         auto end = begin + blockSize;
         if (t_i == FLAGS_messageHandlerThreads - 1) end = n;

         // parititon mailboxes
         uint8_t* partition = (uint8_t*)cm.getGlobalBuffer().allocate(end - begin, CACHE_LINE);  // CL aligned
         ensure(((uintptr_t)partition) % CACHE_LINE == 0);
         // cannot use emplace because of mutex
         mbPartitions[t_i].mailboxes = partition;
         mbPartitions[t_i].numberMailboxes = end - begin;
         mbPartitions[t_i].beginId = begin;
      }
      std::cout << "Message handler started " << "\n";
      startThread();
   };
}
// -------------------------------------------------------------------------------------
void MessageHandler::init() {
   InitMessage* initServer = (InitMessage*)cm.getGlobalBuffer().allocate(sizeof(InitMessage));
   // -------------------------------------------------------------------------------------
   size_t numConnections = (FLAGS_worker) * (FLAGS_compute_nodes);
   connectedClients = numConnections;
   while (cm.getNumberIncomingConnections() != (numConnections))
      ;  // block until client is connected
   // -------------------------------------------------------------------------------------
   std::cout << "Number connections " << numConnections << std::endl;
   // wait until all workers are connected
   std::vector<RdmaContext*> rdmaCtxs;  // get cm ids of incomming

   while (true) {
      std::vector<RdmaContext*> tmp_rdmaCtxs(cm.getIncomingConnections());  // get cm ids of incomming
      uint64_t workers = 0;
      for (auto* rContext : tmp_rdmaCtxs) {
         if (rContext->type != Type::WORKER) continue;
         workers++;
      }
      if (workers == numConnections) {
         rdmaCtxs = tmp_rdmaCtxs;
         break;
      }
   }
   // -------------------------------------------------------------------------------------
   // shuffle worker connections
   // -------------------------------------------------------------------------------------
   auto rng = std::default_random_engine{};
   std::shuffle(std::begin(rdmaCtxs), std::end(rdmaCtxs), rng);

   uint64_t counter = 0;
   uint64_t partitionId = 0;
   uint64_t partitionOffset = 0;

   for (auto* rContext : rdmaCtxs) {
      // -------------------------------------------------------------------------------------
      if (rContext->type != Type::WORKER) {
         continue;  // skip no worker connection
      }

      // partially initiallize connection connectxt
      ConnectionContext cctx;
      cctx.request = (Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctx.response = (Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctx.scan_buffer = (KVPair*)cm.getGlobalBuffer().allocate(sizeof(KVPair) * MAX_SCAN_RESULT, CACHE_LINE);
      cctx.rctx = rContext;
      // -------------------------------------------------------------------------------------
      // find correct mailbox in partitions
      if ((counter >= (mbPartitions[partitionId].beginId + mbPartitions[partitionId].numberMailboxes))) {
         partitionId++;
         partitionOffset = 0;
      }
      auto& mbPartition = mbPartitions[partitionId];
      ensure(mbPartition.beginId + partitionOffset == counter);
      // -------------------------------------------------------------------------------------
      // fill init message
      initServer->mbOffset = (uintptr_t)&mbPartition.mailboxes[partitionOffset];
      initServer->plOffset = (uintptr_t)cctx.request;
      initServer->nodeId = nodeId;
      initServer->barrierAddr = (uintptr_t)db.barrier;
      initServer->nodeId = nodeId;
      initServer->metadataOffset = (uintptr_t)db.md;
      initServer->threadId = 1000;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctx.rctx), initServer);
      // -------------------------------------------------------------------------------------
      // finish initialization of cctx
      cctx.plOffset = (reinterpret_cast<InitMessage*>((cctx.rctx->applicationData)))->plOffset;
      cctx.bmId = (reinterpret_cast<InitMessage*>((cctx.rctx->applicationData)))->nodeId;
      cctx.result_buffer = (reinterpret_cast<InitMessage*>((cctx.rctx->applicationData)))->scanResultOffset;
      // -------------------------------------------------------------------------------------
      cctx.remoteMbOffsets.resize(FLAGS_compute_nodes);
      cctx.remotePlOffsets.resize(FLAGS_compute_nodes);
      // -------------------------------------------------------------------------------------
      cctxs.push_back(cctx);
      // -------------------------------------------------------------------------------------
      // check if ctx is needed as endpoint
      // increment running counter
      counter++;
      partitionOffset++;
   }

   ensure(counter == numConnections);
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
MessageHandler::~MessageHandler() {
   stopThread();
}
// -------------------------------------------------------------------------------------
void MessageHandler::startThread() {
   for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++) {
      std::thread t([&, t_i]() {
         // -------------------------------------------------------------------------------------
         std::unique_ptr<threads::ThreadContext> threadContext = std::make_unique<threads::ThreadContext>();
         threads::ThreadContext::tlsPtr = threadContext.get();  // init tl ptr
         // -------------------------------------------------------------------------------------
         threadCount++;
         // protect init only ont thread should do it;
         if (t_i == 0) {
            init();
            finishedInit = true;
         } else {
            while (!finishedInit)
               ;  // block until initialized
         }
         
         auto& tree = db.getTree();
         MailboxPartition& mbPartition = mbPartitions[t_i];
         uint8_t* mailboxes = mbPartition.mailboxes;
         const uint64_t beginId = mbPartition.beginId;
         uint64_t startPosition = 0;  // randomize messages
         profiling::WorkerCounters counters;  // create counters
         uint64_t mailboxIdx = 0;
         std::vector<uint64_t> latencies(mbPartition.numberMailboxes);

         while (threadsRunning || connectedClients.load()) {
            for (uint64_t m_i = 0; m_i < mbPartition.numberMailboxes; m_i++, mailboxIdx++) {
               // -------------------------------------------------------------------------------------
               if (mailboxIdx >= mbPartition.numberMailboxes) mailboxIdx = 0;

               if (mailboxes[mailboxIdx] == 0) continue;
               // -------------------------------------------------------------------------------------
               mailboxes[mailboxIdx] = 0;  // reset mailbox before response is sent
               // -------------------------------------------------------------------------------------
               // handle message
               uint64_t clientId = mailboxIdx + beginId;  // correct for partiton
               auto& ctx = cctxs[clientId];
               switch (ctx.request->type) {
                  case MESSAGE_TYPE::Finish: {
                     std::cout << "Received finish message" << "\n";
                     connectedClients--;
                     break;
                  }
                  case MESSAGE_TYPE::Insert:{
                     auto& request = *reinterpret_cast<rdma::InsertRequest*>(ctx.request);
                     auto& response = *MessageFabric::createMessage<rdma::InsertResponse>(ctx.response);
                     response.rc = rdma::RESULT::ABORTED;
                     tree.insert(request.key, request.value);
                     response.rc = rdma::RESULT::COMMITTED;
                     writeMsg(clientId, response);
                     break;
                  }
                  case MESSAGE_TYPE::Lookup:{
                     auto& request = *reinterpret_cast<rdma::LookupRequest*>(ctx.request);
                     auto& response = *MessageFabric::createMessage<rdma::LookupResponse>(ctx.response);
                     response.rc = rdma::RESULT::ABORTED;
                     Value result = 0;
                     if(tree.lookup(request.key, result))
                        response.rc = rdma::RESULT::COMMITTED;
                     response.value = result;
                     writeMsg(clientId, response);
                     break;
                  }
                  case MESSAGE_TYPE::Scan:{
                     auto& request = *reinterpret_cast<rdma::ScanRequest*>(ctx.request);
                     auto& response = *MessageFabric::createMessage<rdma::ScanResponse>(ctx.response);
                     response.rc = rdma::RESULT::ABORTED;
                     uint64_t length {0};

                     tree.scan<twosided::BTree<Key,Value>::ASC_SCAN>(request.from, [&](Key key, Value value){
                        // copy value to buffer
                        if(key <= request.to){
                           ensure(length < MAX_SCAN_RESULT);
                           ctx.scan_buffer[length].key = key;
                           ctx.scan_buffer[length++].value = value;
                           return true;
                        }
                        return false;
                     });
                     response.rc = rdma::RESULT::COMMITTED;
                     response.length = length;
                     rdma::postWrite(ctx.scan_buffer, *(cctxs[clientId].rctx), rdma::completion::unsignaled, cctxs[clientId].result_buffer, sizeof(KVPair) * length);
                     writeMsg(clientId, response);
                     break;
                  }                     
                  default:
                     throw std::runtime_error("Unexpected Message in MB " + std::to_string(mailboxIdx) + " type " +
                                              std::to_string((size_t)ctx.request->type));

               }
               counters.incr(profiling::WorkerCounters::mh_msgs_handled);
            }
            mailboxIdx = ++startPosition;
         }
         threadCount--;
      });

      // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      if ((t_i % 2) == 0)
         threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      else
         threads::CoreManager::getInstance().pinThreadToHT(t.native_handle());
      t.detach();
   }
}
// -------------------------------------------------------------------------------------
void MessageHandler::stopThread() {
   threadsRunning = false;
   while (threadCount)
      ;  // wait
};

}  // namespace rdma
}  // namespace dtree
