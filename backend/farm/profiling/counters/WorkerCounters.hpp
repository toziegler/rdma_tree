#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "farm/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
#include <string_view>

// -------------------------------------------------------------------------------------

/*
// #define INC_WORKER_COUNTER(COUNTER)                                                                     \
//    {                                                                                                    \
//       auto local = profiling::WorkerCounter::getCounter().counters[COUNTER].load();                     \
//       local++;                                                                                          \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(local, std::memory_order_relaxed); \
//    }


// #define INC_WORKER_COUNTER_BY(COUNTER, value)                                    \
//    {                                                                                                    \
//       auto local = profiling::WorkerCounter::getCounter().counters[COUNTER].load();                     \
//       local+= value;                                                                                    \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(local, std::memory_order_relaxed); \
//    }


// #define ASSIGN_WORKER_COUNTER(COUNTER, value)                                                           \
//    {                                                                                                    \
//       profiling::WorkerCounter::getCounter().counters[COUNTER].store(value, std::memory_order_relaxed); \
//    }

*/
namespace farm
{
namespace profiling
{
struct WorkerCounters {
   // -------------------------------------------------------------------------------------
   enum Name {
      tx_p,
      latency,
      COUNT,
   };
   // -------------------------------------------------------------------------------------
   static const constexpr inline std::array<std::string_view, COUNT> workerCounterTranslation{
       "tx/sec",
       "latency",
   };
   static_assert(workerCounterTranslation.size() == COUNT);
   // -------------------------------------------------------------------------------------
   struct LOG_ENTRY{
      const std::string_view name;
      const LOG_LEVEL level;
   };

   static const constexpr inline std::array<LOG_ENTRY, COUNT> workerCounterLogLevel{{
       {"tx/sec", LOG_LEVEL::RELEASE},
       {"latency", LOG_LEVEL::RELEASE},
   }};
   // -------------------------------------------------------------------------------------
   
   WorkerCounters();
   ~WorkerCounters();
   // -------------------------------------------------------------------------------------

   __attribute__((always_inline)) void incr(const Name& name)
   {
      if( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
         return;
      
      auto local = counters[name].load();
      local++;
      counters[name].store(local, std::memory_order_relaxed);
   }



   __attribute__((always_inline)) uint64_t getTimePoint_for(const Name& name){
        if( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
           return 0;
        
        return utils::getTimePoint();
   }

   
   __attribute__((always_inline)) void incr_by(const Name& name, uint64_t increment)
   {
      if ( workerCounterLogLevel[name].level > ACTIVE_LOG_LEVEL)
         return;
            
      auto local = counters[name].load();
      local+= increment;
      counters[name].store(local, std::memory_order_relaxed);
   }
   
   std::atomic<uint64_t> counters[COUNT] = {0};
};

}  // namespace profiling
}  // namespace farm
