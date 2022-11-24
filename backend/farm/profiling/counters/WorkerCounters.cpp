#include "WorkerCounters.hpp"
#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace farm {
namespace profiling {
// -------------------------------------------------------------------------------------
WorkerCounters::WorkerCounters(){
   CounterRegistry::getInstance().registerWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
WorkerCounters::~WorkerCounters(){
   CounterRegistry::getInstance().deregisterWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
}  // profiling
}  // nam
