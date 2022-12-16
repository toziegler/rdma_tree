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
      ensure(((uintptr_t)this & 63) == 0); 
   }
   uint64_t remote_latch{0};
   uint64_t version{0};
   PType_t type{PType_t::UNINIT};
   struct __attribute__((packed)) PiggyBack {
      uint8_t buffer[BUFFER_SIZE - sizeof(uint64_t)];
      uint64_t payload;
   };

   union {
      uint8_t buffer[BUFFER_SIZE];
      PiggyBack pg;
   };  // can be used to piggy back additional information to keep size small
};
static_assert(sizeof(PageHeader) <= 32, "PageHeaer larger than 32 Byte");

struct MetadataPage : public PageHeader {
   MetadataPage() : PageHeader(PType_t::METADATA) { std::cout << "Meta" << std::endl; };
   RemotePtr getRootPtr() { return (RemotePtr(pg.payload)); }
   void setRootPtr(RemotePtr remote_ptr) { *reinterpret_cast<RemotePtr*>(&pg.payload) = remote_ptr.offset; }
};
static_assert(sizeof(PageHeader) <= 32, "PageHeader larger than 32 Byte");

enum BTreeNodeType: uint64_t  {
  LEAF = 1,
  INNER = 2,
};
struct BTreeHeader : public PageHeader {
   BTreeNodeType node_type; 
   static constexpr uint64_t bytes = BTREE_NODE_SIZE;
   static constexpr uint64_t available_bytes{BTREE_NODE_SIZE - sizeof(PageHeader) - sizeof(node_type)};
   BTreeHeader(BTreeNodeType node_type) : PageHeader(PType_t::BTREE_NODE), node_type(node_type) {}
};
template <typename Key, typename Value>
struct BTreeLeaf : public BTreeHeader {
   struct LeafEntry {
      Key key;
      Value value;
   };
   static constexpr uint64_t max_entries = (available_bytes) / (sizeof(LeafEntry));
   std::array<LeafEntry, max_entries> entries;
   uint8_t padding[available_bytes - (max_entries * (sizeof(LeafEntry)))];
   BTreeLeaf() : BTreeHeader(BTreeNodeType::LEAF) {
      static_assert(sizeof(BTreeLeaf) == BTREE_NODE_SIZE, "Leaf is not the size of BTREE_NODE_SIZE");
   }
};
template <typename Key>
struct BTreeInner : public BTreeHeader {
   struct InnerEntry {
      Key key;
      RemotePtr child;
   };
   static constexpr uint64_t max_entries = (available_bytes) / (sizeof(InnerEntry));
   std::array<InnerEntry, max_entries> children;
   uint8_t padding[available_bytes - (max_entries * (sizeof(InnerEntry)))];
   BTreeInner() : BTreeHeader(BTreeNodeType::INNER) {
      static_assert(sizeof(BTreeInner) == BTREE_NODE_SIZE, "Inner is not the size of BTREE_NODE_SIZE");
   }
};


template <class T, typename... Params>
void allocateInRDMARegion(T* ptr, Params&&... params) {
   new (ptr) T(std::forward<Params>(params)...);
}

}  // namespace onesided
