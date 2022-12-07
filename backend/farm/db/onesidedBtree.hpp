#pragma once
#include <immintrin.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <type_traits>
#include <vector>

#include "Defs.hpp"
#include "farm/threads/Worker.hpp"
//=== One-sided B-Tree ===//

namespace onesided {

static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;

using Version = uint64_t;

enum class PType_t : uint8_t {
   UNINIT = 0,
   METADATA = 1,
};
// attention must be cacheline aligned! but alignas will negatively impact size
// therefore pay attention at alloc;
constexpr size_t BUFFER_SIZE = 15;
struct PageHeader {
   PageHeader(PType_t page_type) : type(page_type) { ensure(((uintptr_t)this & 63) == 0); }
   uint64_t remote_latch{0};
   uint64_t version{0};
   PType_t type{PType_t::UNINIT};
   struct __attribute__((packed)) PiggyBack{
      uint8_t buffer[BUFFER_SIZE - sizeof(uint64_t)];
      uint64_t payload;
   };
   
   union{
      uint8_t buffer[BUFFER_SIZE];
      PiggyBack pg;
   };  // can be used to piggy back additional information to keep size small
};
static_assert(sizeof(PageHeader) <= 32, "PageHeaer larger than 32 Byte");

struct MetadataPage : public PageHeader {
   MetadataPage() : PageHeader(PType_t::METADATA) {
      std::cout << "Meta" << std::endl;

   };
   RemotePtr getRootPtr() { return ( RemotePtr(pg.payload)); }
   void setRootPtr(RemotePtr remote_ptr) { *reinterpret_cast<RemotePtr*>(&pg.payload) = remote_ptr.offset; }
};
static_assert(sizeof(PageHeader) <= 32, "PageHeaer larger than 32 Byte");

template <class T, typename... Params>
void allocateInRDMARegion(T* ptr, Params&&... params) {
   new (ptr) T(std::forward<Params>(params)...);
}

template <typename T>
concept ConceptObject = std::is_base_of<PageHeader, T>::value;

//=== Locking  ===//

template <ConceptObject T>
struct AbstractLatch {
   RemotePtr remote_ptr;
   Version* version{nullptr};
   T* local_copy;
   explicit AbstractLatch(RemotePtr remote_address) : remote_ptr(remote_address) {}
};


template <ConceptObject T>
struct OptimisticLatch : public AbstractLatch<T> {
   using super = AbstractLatch<T>;
   using my_thread = farm::threads::Worker;
   
   OptimisticLatch(RemotePtr remote_ptr) : AbstractLatch<T>(super::remote_ptr) {}
   
   bool try_latch() {
      // read version and lock check if it is latched already
      // must be CL aligned
      return false;
   };
   bool check_or_restart() {
      // throw if we could not latch it
      return false;
   }
};

template <ConceptObject T>
struct ExclusiveLatch : public AbstractLatch<T> {
   explicit ExclusiveLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {}
   // returns true successfully
   using super = AbstractLatch<T>;
   using my_thread = farm::threads::Worker;

   explicit ExclusiveLatch(OptimisticLatch<T>&& optmisticLatch){
      // upgrade logic  
   }
   
   bool try_latch() {
      my_thread::my().compareSwapAsync(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr,
                                       farm::rdma::completion::signaled);
      // read remote data
      super::local_copy = my_thread::my().remote_read<T>(super::remote_ptr);
      super::version = &super::local_copy->version;
      bool latched = my_thread::my().pollCompletionCS(super::remote_ptr, UNLOCKED);
      return latched;
   }

    void unlatch() {
      // unlatch increments thread local buffer to avoid memory corruption
      // increment version
      (*super::version)++;
      // write back node
      my_thread::my().remote_write<onesided::MetadataPage>(super::remote_ptr, super::local_copy);
      // unlock
      my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr, farm::rdma::completion::unsignaled);
   };

};
//=== B-Tree Nodes ===//
// what do we need to have here
// we have an uint64_t which serves as the atomic
//=== B-Tree ===//

}  // namespace onesided
