#include "Worker.hpp"
// -------------------------------------------------------------------------------------
namespace farm {
namespace threads {
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tlsPtr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId)
    : workerId(workerId),
      name(name),
      cpuCounters(name),
      cm(cm),
      nodeId_(nodeId),
      cctxs(FLAGS_storage_nodes),
      threadContext(std::make_unique<ThreadContext>()) {
   ThreadContext::tlsPtr = threadContext.get();
   tl_rdma_buffer = (uint8_t*)cm.getGlobalBuffer().allocate(THREAD_LOCAL_RDMA_BUFFER,64); 
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
      init->threadId = workerId + (nodeId*FLAGS_worker);
      init->scanResultOffset = (uintptr_t)cctxs[n_i].result_buffer;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------
      cctxs[n_i].plOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->plOffset;
      cctxs[n_i].mbOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->mbOffset;
      ensure((reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->nodeId == n_i);
      auto& msg = *reinterpret_cast<InitMessage*>((cctxs[n_i].rctx->applicationData));
      auto num_tables = msg.num_tables;
      for(uint64_t t_i = 0; t_i < num_tables; t_i++){
         if (tables.size() <= t_i) tables.emplace_back();
         tables.back().addPartition({.offset = msg.tables[t_i].offset, .begin = msg.tables[t_i].begin, .end= msg.tables[t_i].end, .nodeId = msg.nodeId});
      }
      if(msg.nodeId == 0)
         barrier = msg.barrierAddr;
   }

   std::cout << "Connection established" << "\n";

}

// -------------------------------------------------------------------------------------
Worker::~Worker() {
   for (uint64_t n_i = 0; n_i < FLAGS_storage_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      auto& request = *MessageFabric::createMessage<FinishRequest>(cctxs[n_i].outgoing);
      assert(request.type == MESSAGE_TYPE::Finish);
      writeMsg(n_i, request);
      // -------------------------------------------------------------------------------------
   }
}
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace farm
