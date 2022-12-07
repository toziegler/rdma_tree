#include "RDMACounters.hpp"

#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------

namespace dtree {
namespace profiling {

RDMACounters::RDMACounters() : rdmaRecv(rdmaPathRecv), rdmaSent(rdmaPathXmit) {
   CounterRegistry::getInstance().registerRDMACounter(this);
};
// -------------------------------------------------------------------------------------
RDMACounters::~RDMACounters() { CounterRegistry::getInstance().deregisterRDMACounter(this); }
// -------------------------------------------------------------------------------------
double RDMACounters::getSentGB() {
   return (static_cast<double>(rdmaSent()) / static_cast<double>(1024*1024*1024));
}
// -------------------------------------------------------------------------------------
double RDMACounters::getRecvGB() {
   return (static_cast<double>(rdmaRecv()) / static_cast<double>(1024*1024*1024));
}
}  // namespace profiling
}  // namespace dtree
