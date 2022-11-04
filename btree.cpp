#include <algorithm>
#include <array>
#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <limits>

#define GDB() std::raise(SIGINT);

constexpr uint64_t pageSize = 4096;

union RemotePtr {
   struct {
      uint64_t NodeId : 8, Offset : 56;
   };
   uint64_t full;
};

// todo inherits from latch
struct BTreeNodeHeader {
   static constexpr uint32_t underFullSize = pageSize / 4;
   static constexpr RemotePtr noNode{.full = std::numeric_limits<uint64_t>::max()};

   union {
      RemotePtr upperInnerNode;
      RemotePtr nextLeafNode = noNode;
   };

   bool hasRightNeighbour() { return nextLeafNode.full != noNode.full; }

   // todo are all of those needed
   uint16_t count = 0;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset = static_cast<uint16_t>(pageSize);
   bool isLeaf;
   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf){};
};

template <typename Key, typename Value>
struct BTreeNode : public BTreeNodeHeader {
   struct SlotLeaf {
      Key key;
      Value value;
   };
   static constexpr size_t numberLeafes = (pageSize - sizeof(BTreeNodeHeader)) / sizeof(SlotLeaf);

   struct SlotInner {
      Key key;
      RemotePtr ptr;
   };
   static constexpr size_t numberInner = (pageSize - sizeof(BTreeNodeHeader)) / sizeof(SlotInner);

   union {
      std::array<SlotLeaf, numberLeafes> leafes;
      std::array<SlotInner, numberInner> inner;
   };

   // used for copy
   BTreeNode(){};

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf)
   {
      static_assert(sizeof(BTreeNode<Key, Value>) == pageSize, "btree node size problem");
   }
   bool isInner() { return !isLeaf; }

   auto lowerBoundInner(Key key)
   {
      return std::lower_bound(std::begin(inner), std::begin(inner) + count, key,
                              [](const auto& a, const auto& b) { return a.key < b; });
   }

   auto lowerBoundLeaf(Key key)
   {
      return std::lower_bound(std::begin(leafes), std::begin(leafes) + count, key,
                              [](const auto& a, const auto& b) { return a.key < b; });
   }

   void insertInInner(Key key, RemotePtr offset)
   {
      if (count < numberInner)
         inner[count++] = {.key = key, .ptr = offset};
      else
         throw std::runtime_error("not enough space");
   }

   void insertInLeaf(Key key, Value value)
   {
      if (count < numberLeafes)
         leafes[count++] = {.key = key, .value = value};
      else
         throw std::runtime_error("not enough space in leaf");
   }

   void splitNode(BTreeNode* parent, uint64_t sepSlot, Key seperator){
       // todo
   };

   Key findSeperator(bool splitOrdered) {}

   RemotePtr lookupInner(Key key)
   {
      auto it = lowerBoundInner(key);
      if (it == inner.last())
         return upperInnerNode;
      return it->ptr;
   }
};

struct Metadata {
   RemotePtr root;
};

// default latched
uint8_t* buffer = new uint8_t[pageSize * 10]{};
uint64_t used_buffer_slots = 0;

template <typename T>
struct GuardX {
   RemotePtr ptr;
   T* local{nullptr};

   GuardX(){};

   GuardX(RemotePtr ptr) : ptr(ptr)
   {
      // latch remote
      // todo fix to thread local allocator
      local = new T;
      std::memcpy(local, buffer + ptr.Offset, pageSize);
   }

   ~GuardX()
   {
      // write back
      // todo onesided
      std::cout << "offset " << ptr.Offset << "\n";
      std::memcpy(buffer + ptr.Offset, local, pageSize);
      delete local;
      // unlatch
   }

   T* operator->() { return local; }
};

template <class T>
struct AllocGuard : public GuardX<T> {
   template <typename... Params>
   AllocGuard(Params&&... params)
   {
      // alloc remote buffer
      auto slot = used_buffer_slots++;
      auto offset = slot * pageSize;
      GuardX<T>::local = new T(std::forward<Params>(params)...);
      GuardX<T>::ptr = RemotePtr{.NodeId = 0, .Offset = offset};
   }
};

// todo replace with remote information
static Metadata meta;

template <typename Key, typename Value>
struct BTree {
   std::atomic<bool> splitOrdered{false};
   BTree(bool creation)
   {
      if (creation) {
         AllocGuard<BTreeNode<Key, Value>> root(true);
         // write to metadata

      } else {
         // wait until root is created
      }
   }
};

int main(int argc, char* argv[])
{
   BTree<int, int> tree(true);
   auto* root = (BTreeNode<int, int>*)(&buffer[0]);
   auto it = root->lowerBoundLeaf(100);
   std::cout << (*it).key << "\n";
   return 0;
}
