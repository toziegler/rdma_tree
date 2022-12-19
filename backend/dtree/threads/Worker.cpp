#include "Worker.hpp"

#include <stdexcept>

#include "Defs.hpp"
// -------------------------------------------------------------------------------------
namespace dtree {
namespace threads {
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
AbstractWorker::AbstractWorker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId)
    : workerId(workerId),
      name(name),
      cpuCounters(name),
      cm(cm),
      nodeId_(nodeId),
      cctxs(FLAGS_storage_nodes),
      remote_caches(FLAGS_storage_nodes) {
   barrier_buffer = (uint64_t*)cm.getGlobalBuffer().allocate(64, 64);
   // -------------------------------------------------------------------------------------
   // Connection to MessageHandler
   // -------------------------------------------------------------------------------------
   // First initiate connection
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      auto& ip = NODES[FLAGS_storage_nodes][n_i];
      cctxs[n_i].rctx = &(cm.initiateConnection(ip, rdma::Type::WORKER, workerId, nodeId));
      // -------------------------------------------------------------------------------------
      cctxs[n_i].incoming = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].outgoing = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].result_buffer = (KVPair*)cm.getGlobalBuffer().allocate(sizeof(KVPair) * MAX_SCAN_RESULT, CACHE_LINE);
      cctxs[n_i].wqe = 0;
      // -------------------------------------------------------------------------------------
   }

   // -------------------------------------------------------------------------------------
   // Second finish connection
   rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      // fill init messages
      init->mbOffset = 0;  // No MB offset
      init->plOffset = (uintptr_t)cctxs[n_i].incoming;
      init->nodeId = nodeId;
      init->threadId = workerId + (nodeId * FLAGS_worker);
      init->scanResultOffset = (uintptr_t)cctxs[n_i].result_buffer;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------
      cctxs[n_i].plOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->plOffset;
      cctxs[n_i].mbOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->mbOffset;
      ensure((reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->nodeId == n_i);
      auto& msg = *reinterpret_cast<InitMessage*>((cctxs[n_i].rctx->applicationData));
      remote_caches[n_i] = {.counter = RemotePtr(n_i, msg.remote_cache_counter),
                            .begin_offset = msg.remote_cache_offset};
      if (msg.nodeId == 0) {
         barrier = msg.barrierAddr;
         metadataPage = RemotePtr(msg.nodeId, msg.metadataOffset);
      }
   }
   std::cout << "Connection established"
             << "\n";
}

// -------------------------------------------------------------------------------------
AbstractWorker::~AbstractWorker() {
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      auto& request = *MessageFabric::createMessage<FinishRequest>(cctxs[n_i].outgoing);
      assert(request.type == MESSAGE_TYPE::Finish);
      writeMsg(n_i, request);
      // -------------------------------------------------------------------------------------
   }
}
// -------------------------------------------------------------------------------------
namespace twosided {
thread_local Worker* Worker::tlsPtr = nullptr;
}  // namespace twosided
namespace onesided {
thread_local Worker* Worker::tlsPtr = nullptr;

Worker::Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId)
    : AbstractWorker(workerId, name, cm, nodeId) {
   for (uint64_t r_i = 0; r_i < CONCURRENT_LATCHES; r_i++) {
      RDMAMemoryInfo rmem;
      rmem.local_copy = (PageHeader*)cm.getGlobalBuffer().allocate(BTREE_NODE_SIZE + PADDING, 8);
      rmem.latch_buffer = (PageHeader*)cm.getGlobalBuffer().allocate(64, 64);
      if (!local_rmemory.try_push(rmem)) { throw std::logic_error("local rmemory failed"); }
   }
}
}  // namespace onesided
}  // namespace threads
}  // namespace dtree
