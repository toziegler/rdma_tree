// Type your code here, or load an example.
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
/*
  InnerTraversals
  - one-sided
  - two-sided stub on the client
  - local for the two-sided on the server
  LeafTraversals
  - one-sided
  - two-sided stub on the client
  - local for the two-sided server
  - hybrid on the server which just sends the last inner node

  - one-sided/ local and hybrid can share the same btree trait
*/

enum class NodeType : uint8_t { INNER = 1, LEAF = 2 };

struct BTreeNodeHeader {
   NodeType type;
   uint16_t count{0};
   BTreeNodeHeader() = default;
   BTreeNodeHeader(NodeType type) : type(type) {}
};

template <typename Key, typename Value, uint64_t NODE_SIZE, NodeType TYPE>
struct BTreeNodeTrait : BTreeNodeHeader {
   static constexpr uint32_t underFullSize = NODE_SIZE / 4;
   static constexpr size_t availableSize = NODE_SIZE - sizeof(BTreeNodeHeader);

   struct SeparatorInfo {
      Key seperator;
      BTreeNodeHeader* rightNode;
   };

   BTreeNodeTrait() : BTreeNodeHeader(TYPE){};
   bool is_leaf() { return (TYPE == NodeType::LEAF); }
   std::optional<size_t> lower_bound();
   bool upsert(const Key& key, const Value& value);
   SeparatorInfo split();
   SeparatorInfo find_separator();
   inline Key& key_at(size_t idx);
   inline Value& value_at(size_t idx);
   size_t begin() const;
   size_t end() const;
};

// split into leaf and inner
template <typename Key, typename Value, size_t NODE_SIZE = 4096ul>
struct BTreeNodeLeaf : public BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::LEAF> {
   using super_t = BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::LEAF>;

   struct EntryLeaf {
      Key key;
      Value value;
   } __attribute__((packed));

   static constexpr size_t numberLeaves = super_t::availableSize / sizeof(EntryLeaf);
   uint8_t padding[super_t::availableSize - (numberLeaves * sizeof(EntryLeaf))];
   std::array<EntryLeaf, numberLeaves> leafEntries;

   BTreeNodeLeaf()
   {
      static_assert(sizeof(BTreeNodeLeaf) == NODE_SIZE, "btree node size problem");
      std::cout << "Leaf size " << sizeof(BTreeNodeLeaf) << "\n";
      std::cout << "Leaf payload  " << sizeof(EntryLeaf) * numberLeaves << "\n";
      std::cout << "Number Leaf  " << numberLeaves << "\n";
   };
};

// split into leaf and inner
template <typename Key, typename Value, size_t NODE_SIZE = 4096ul>
struct BTreeNodeInner : public BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::INNER> {
   using super_t = BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::INNER>;

   struct EntryInner {
      Key key;
      Value child;
   } __attribute__((packed));

   BTreeNodeHeader* outerChildNode;
   static constexpr size_t availableSizeInner = super_t::availableSize - sizeof(BTreeNodeHeader*);
   static constexpr size_t numberInners = availableSizeInner / sizeof(EntryInner);
   std::array<EntryInner, numberInners> innerEntries;

   BTreeNodeInner()
   {
      static_assert(sizeof(BTreeNodeInner) == NODE_SIZE, "btree node size problem");
      std::cout << "Inner size " << sizeof(BTreeNodeInner) << "\n";
      std::cout << "Inner payload  " << sizeof(EntryInner) * numberInners << "\n";
      std::cout << "Number inner  " << numberInners << "\n";
   }

   size_t lower_bound(Key key)
   {
      auto begin_inner = std::begin(innerEntries);
      auto idx = std::distance(begin_inner, std::lower_bound(begin_inner, begin_inner + super_t::count, key,
                                                             [](const auto& a, const auto& b) { return a.key < b; }));
      return idx;
   }
   // does not handle splitting, i.e., there has to be space
   // inserts or upserts
   void upsert(const Key& key, const Value& value)
   {
      if (super_t::count >= numberInners) throw std::runtime_error("not enough space in inner");
      auto idx = lower_bound(key);
      if (innerEntries[idx].key == key) {
         innerEntries[idx].child = value;
         return;
      }
      auto begin_inner = std::begin(innerEntries);
      std::copy(begin_inner + idx, begin_inner + super_t::count, begin_inner + idx + 1);
      innerEntries[idx].key = key;
      innerEntries[idx].child = value;
      super_t::count++;
   }
   // inner node are split in the middle 
   size_t find_separator(){
      size_t slotId = super_t::count / 2;
      return slotId;
   }
   
   typename super_t::SeparatorInfo split() {
      // create new right node
      auto* rightNode = new BTreeNodeInner();
      auto sep = find_separator();
      auto begin_inner = std::begin(innerEntries);
      std::copy(begin_inner + sep, begin_inner + super_t::count, std::begin(rightNode->innerEntries));
      super_t::count = super_t::count - sep;
      rightNode->super_t::count = sep;
      return {sep, rightNode};
   }
   size_t begin() const { return 0; }
   size_t end() const { return super_t::count; }
};

#include <random>

int main()
{
   BTreeNodeInner<int, BTreeNodeHeader*> inner;
   std::random_device dev;
   std::mt19937 rng(dev());
   std::uniform_int_distribution<std::mt19937::result_type> dist6(1, 1e6);  // distribution in range [1, 6]

   for (int i = 0; i < BTreeNodeInner<int, BTreeNodeHeader*>::numberInners; ++i) {
      inner.upsert(dist6(rng), &inner);
      std::cout << "i " << i << std::endl;
   }
   std::cout << "insert" << std::endl;
   std::cout << "max " << BTreeNodeInner<int, BTreeNodeHeader*>::numberInners << "\n";

   auto idx = inner.lower_bound(dist6(rng));
   if (idx != inner.end()) {
      std::cout << "idx " << idx << "\n";
      std::cout << "value " << inner.innerEntries[idx].key << "\n";
   }

   int prev = 0;
   for (auto it = inner.begin(); it < inner.end(); it++) {
      std::cout << inner.innerEntries[it].key << "\n";
      if (prev > inner.innerEntries[it].key) throw std::logic_error("cannot be bigger");
   }

   auto sepInfo = inner.split();
   for (auto it = inner.begin(); it < inner.end(); it++) {
      std::cout << inner.innerEntries[it].key << "\n";
   }
   std::cout << "right " << "\n";

   auto rightNode = (decltype(inner)*)sepInfo.rightNode;
   for (auto it = rightNode->begin(); it < rightNode->end(); it++) {
      std::cout << rightNode->innerEntries[it].key << "\n";
   }

   return 0;
}
