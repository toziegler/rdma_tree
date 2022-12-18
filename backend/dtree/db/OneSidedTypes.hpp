#pragma once
#include <immintrin.h>
#include <sched.h>
#include <sys/types.h>

#include <array>
#include <atomic>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <type_traits>
#include <vector>

#include "Defs.hpp"
namespace onesided {

static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;

using Version = uint64_t;

struct OLCRestartException {};

enum BTreeNodeType : uint64_t {
   LEAF = 1,
   INNER = 2,
};

enum class PType_t : uint8_t {
   UNINIT = 0,
   METADATA = 1,
   BTREE_NODE = 2,
};
// attention must be cacheline aligned! but alignas will negatively impact size
// therefore pay attention at alloc;
constexpr size_t BUFFER_SIZE = 15;
struct PageHeader {
   PageHeader(PType_t page_type) : type(page_type) { 
      //ensure(((uintptr_t)this & 63) == 0);
   }
   uint64_t remote_latch{0};
   uint64_t version{0};
   PType_t type{PType_t::UNINIT};
   struct MetadataPiggyback {
      RemotePtr remote_ptr;
      uint8_t height;
   };
   struct BTreeHeaderPiggyback {
      BTreeNodeType node_type;
      uint16_t count;
   };

   union {
      BTreeHeaderPiggyback btpg;
      MetadataPiggyback mdpg;
   };
};
static_assert(sizeof(PageHeader) <= 64, "PageHeaer larger than CL");

struct MetadataPage : public PageHeader {
   MetadataPage() : PageHeader(PType_t::METADATA) { std::cout << "Meta" << std::endl; };
   RemotePtr getRootPtr() { return (RemotePtr(mdpg.remote_ptr)); }
   void setRootPtr(RemotePtr remote_ptr) { mdpg.remote_ptr = remote_ptr; }
   uint8_t getHeight() { return mdpg.height; }
   void setHeight(uint8_t height) { mdpg.height = height; }
};
static_assert(sizeof(PageHeader) <= 64, "PageHeader larger than CL");

template <class T, typename... Params>
void allocateInRDMARegion(T* ptr, Params&&... params) {
   new (ptr) T(std::forward<Params>(params)...);
}

template <typename T>
concept ConceptObject = std::is_base_of<PageHeader, T>::value;
template <ConceptObject T>
struct AbstractLatch {
   RemotePtr remote_ptr{0,0};
   Version version{0};
   bool latched {false};
   T* local_copy {nullptr};
   explicit AbstractLatch(RemotePtr remote_address) : remote_ptr(remote_address) {}
   AbstractLatch(){};
   bool isLatched(){
      return latched;
   }
};


}  // namespace onesided
