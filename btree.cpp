// Type your code here, or load an example.
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
#include <csignal>
#define GDB() std::raise(SIGINT);

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
   bool is_leaf() { return (type == NodeType::LEAF); }
   BTreeNodeHeader() = default;
   BTreeNodeHeader(NodeType type) : type(type) {}
};

template <typename Key, typename Value, uint64_t NODE_SIZE, NodeType TYPE>
struct BTreeNodeTrait : BTreeNodeHeader {
   static constexpr uint32_t underFullSize = NODE_SIZE / 4;
   static constexpr size_t availableSize = NODE_SIZE - sizeof(BTreeNodeHeader);

   struct SeparatorInfo {
      Key sep;
      BTreeNodeHeader* rightNode;
   };

   BTreeNodeTrait() : BTreeNodeHeader(TYPE){};
   std::optional<size_t> lower_bound();
   bool upsert(const Key& key, const Value& value);
   SeparatorInfo split();
   SeparatorInfo find_separator();
   bool has_space();
   inline Key& key_at(size_t idx);
   inline Value& value_at(size_t idx);
   size_t begin() const;
   size_t end() const;
};

// split into leaf and inner
template <typename Key, typename Value, size_t NODE_SIZE = 4096ul>
struct BTreeNodeLeaf : public BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::LEAF> {
   using super_t = BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::LEAF>;
   using key_t = Key;
   using value_t = Value;
   
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
   };

   auto lower_bound(Key key)
   {
      auto begin_inner = std::begin(leafEntries);
      auto it = std::lower_bound(begin_inner, begin_inner + super_t::count, key,
                                 [](const auto& a, const auto& b) { return a.key < b; });
      return it;
   }

   bool has_space(){
      return (super_t::count < numberLeaves);
   };

   void upsert(const Key& key, const Value& value)
   {
      if (super_t::count >= numberLeaves) throw std::runtime_error("not enough space in inner");
      auto begin_inner = std::begin(leafEntries);
      auto idx = std::distance(begin_inner, std::lower_bound(begin_inner, begin_inner + super_t::count, key,
                                                             [](const auto& a, const auto& b) { return a.key < b; }));

      if (leafEntries[idx].key == key)
      {
         leafEntries[idx].value = value;
         return;
      }
      std::copy(begin_inner + idx, begin_inner + super_t::count, begin_inner + idx + 1);
      leafEntries[idx].key = key;
      leafEntries[idx].value = value;
      super_t::count++;
   }
   // inner node are split in the middle
   size_t find_separator()
   {
      // order split optimization 
      size_t slotId = super_t::count / 2;
      return slotId;
   }

   void move_kv_range(BTreeNodeLeaf* dst, uint16_t dstStartSlot, uint16_t srcStartSlot, uint16_t numberSlots)
   {
      auto begin_inner = std::begin(leafEntries);
      std::copy(begin_inner + srcStartSlot, begin_inner + srcStartSlot + numberSlots,
                std::begin(dst->leafEntries) + dstStartSlot);
      dst->super_t::count += numberSlots;
      super_t::count = super_t::count - (numberSlots);
   }

   // split inner removes the separator
   typename super_t::SeparatorInfo split()
   {
      // create new right node
      auto* rightNode = new BTreeNodeLeaf();
      auto sep = find_separator();
      // copy left to new right
      move_kv_range(rightNode, 0, sep, super_t::count - sep);
      return {sep, rightNode};
   }

   size_t begin() const { return 0; }
   size_t end() const { return super_t::count; }
   
   
};

// split into leaf and inner
template <typename Key, typename Value, size_t NODE_SIZE = 4096ul>
struct BTreeNodeInner : public BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::INNER> {
   using super_t = BTreeNodeTrait<Key, Value, NODE_SIZE, NodeType::INNER>;

   struct EntryInner {
      Key key;
      Value child;
   } __attribute__((packed));

   struct {
      BTreeNodeHeader* outerChildNode;
   } __attribute__((packed));
   
   static constexpr size_t availableSizeInner = super_t::availableSize - sizeof(BTreeNodeHeader*);
   static constexpr size_t numberInners = availableSizeInner / sizeof(EntryInner);
   uint8_t padding[availableSizeInner - ( numberInners * sizeof(EntryInner))]; 
   std::array<EntryInner, numberInners> innerEntries;

   BTreeNodeInner()
   {
      static_assert(sizeof(BTreeNodeInner) == NODE_SIZE, "btree node size problem");
   }

   Value lower_bound(Key key)
   {
      auto begin_inner = std::begin(innerEntries);
      auto it = std::lower_bound(begin_inner, begin_inner + super_t::count, key,
                                 [](const auto& a, const auto& b) { return a.key < b; });
      if (it == begin_inner + super_t::count) { return outerChildNode; }
      return it->child;
   }

   bool has_space(){
      return (super_t::count < numberInners);
   };
   
   // does not handle splitting, i.e., there has to be space
   // inserts or upserts
   void upsert(const Key& key, const Value& value)
   {
      if (super_t::count >= numberInners) throw std::runtime_error("not enough space in inner");
      auto begin_inner = std::begin(innerEntries);
      auto idx = std::distance(begin_inner, std::lower_bound(begin_inner, begin_inner + super_t::count, key,
                                                               [](const auto& a, const auto& b) { return a.key < b; }));
      
      if (innerEntries[idx].key == key)
      {
         innerEntries[idx].child = value;
         return;
      }
      std::copy(begin_inner + idx, begin_inner + super_t::count, begin_inner + idx + 1);
      innerEntries[idx].key = key;
      innerEntries[idx].child = value;
      super_t::count++;
   }
   // inner node are split in the middle
   size_t find_separator()
   {
      size_t slotId = super_t::count / 2;
      return slotId;
   }

   void move_kv_range(BTreeNodeInner* dst, uint16_t dstStartSlot, uint16_t srcStartSlot, uint16_t numberSlots)
   {
      auto begin_inner = std::begin(innerEntries);
      std::copy(begin_inner + srcStartSlot, begin_inner + srcStartSlot + numberSlots,
                std::begin(dst->innerEntries) + dstStartSlot);
      dst->super_t::count += numberSlots;
      super_t::count = super_t::count - (numberSlots);
   }

   size_t count(){
      return super_t::count;
   }
   
   // split inner removes the separator
   typename super_t::SeparatorInfo split()
   {
      // create new right node
      auto* rightNode = new BTreeNodeInner();
      auto sep = find_separator();
      // copy left to new right
      move_kv_range(rightNode, 0, sep, super_t::count - sep);
      // because we move the sep out of this node
      rightNode->outerChildNode = outerChildNode;
      // set outer child to the seperator
      outerChildNode = reinterpret_cast<BTreeNodeHeader*>(innerEntries[super_t::count].child);
      super_t::count--; // remove separator 
      return {sep, rightNode};
   }

   size_t begin() const { return 0; }
   size_t end() const { return super_t::count; }
};

template<typename InnerNode, typename LeafNode> 
struct BTreeTrait{
   using leaf_t = LeafNode;
   using key_t = typename leaf_t::key_t;
   using value_t = typename leaf_t::value_t;

   bool lookup(const key_t& key, value_t& value);
   bool insert(const key_t& key, const value_t& value);
};


template<typename InnerNode, typename LeafNode>
struct BTree : BTreeTrait<InnerNode, LeafNode>{
   using super_t = BTreeTrait<InnerNode, LeafNode>;
   using self_t = BTree<InnerNode, LeafNode>;
   using typename super_t::key_t;
   using typename super_t::value_t;

   BTreeNodeHeader* root;
   BTree() : root(new LeafNode()){};

   bool lookup(const key_t& key, value_t& value){
      BTreeNodeHeader* node = root;
      while(!node->is_leaf()){
         auto inner = static_cast<InnerNode*>(node);
         node = inner->lower_bound(key);
      }
      auto leaf = static_cast<LeafNode*>(node);
      auto kv = leaf->lower_bound(key);
      value = kv->value;
      if(kv->key == key)
         return true;
      return false;
   }


   void ensure_space(){}
   
   // calls split on the passed node, and checks that parents have space
   // otherwise we split the parents
   void try_split(BTreeNodeHeader* node, BTreeNodeHeader* parent, key_t key){
      if(node == root){
         // just creat a new root 
         parent = new InnerNode();
         root = parent;
         std::cout << "created new root " << "\n";

      }

      // must be inner 
      if(static_cast<InnerNode*>(parent)->has_space()){
         // we split the node into two
         auto sep = static_cast<LeafNode*>(node)->split();
         // use key to find the old sep
         // cannot use lowerbound search here
         auto begin_inner = std::begin(static_cast<InnerNode*>(parent)->innerEntries);
         auto count = static_cast<InnerNode*>(parent)->count();
         // this implements the shift 
         auto it = std::lower_bound(begin_inner, begin_inner + count, key,
                                 [](const auto& a, const auto& b) { return a.key < b; });

         if(it == begin_inner + count){
            std::cout << "outerchild " << "\n";
            static_cast<InnerNode*>(parent)->outerChildNode = sep.rightNode;
            // todo fix
            throw std::logic_error("implement parent split");
            // herer we overwrite the outerchild always and do not integarte it 
         }
         else{
            std::cout << "innerchild" << "\n";
            it->child = sep.rightNode;
         }
         // insert new seperator and left tree and move it to the right 
         static_cast<InnerNode*>(parent)->upsert(sep.sep, sep.rightNode);
         return;
      }

      // split parents first and retry
      // todo when we grow higher then height 1 we need to split parents 
      throw std::logic_error("implement parent split");
   }
   
   bool upsert(const key_t& key, const value_t& value){
      BTreeNodeHeader* node = root;
      BTreeNodeHeader* parent = nullptr;
      while(!node->is_leaf()){
         auto inner = static_cast<InnerNode*>(node);
         parent = node;
         node = inner->lower_bound(key);
      }

      auto leaf = static_cast<LeafNode*>(node);
      if(!leaf->has_space()){
         try_split(leaf, parent, key);
         return upsert(key, value);
      }
      leaf->upsert(key, value);
      return true;
   }
   
   
};

#include <random>

int main()
{
   using InnerNode = BTreeNodeInner<int, BTreeNodeHeader*>;
   using LeafNode = BTreeNodeLeaf<int, int>;

   BTree<InnerNode,LeafNode> tree;

   for (int i = 0; i < 100000; ++i) {
      tree.upsert(i,i);
      if((i % 1000000) == 0)
         std::cout << "i " << i << std::endl;
   }


   std::cout <<  "lookup" << "\n";

   for (int i = 0; i < 100000; ++i) {
      int val;
      if(tree.lookup(i,val)){
         std::cout << "i " << val << std::endl;
      }
   }


   
   // BTreeNodeInner<int, BTreeNodeHeader*> inner;
   // std::random_device dev;
   // std::mt19937 rng(dev());
   // std::uniform_int_distribution<std::mt19937::result_type> dist6(
   //     0, BTreeNodeInner<int, BTreeNodeHeader*>::numberInners);  // distribution in range [1, 6]

   // for (int i = 0; i < BTreeNodeInner<int, BTreeNodeHeader*>::numberInners; ++i) {
   //    inner.upsert(i, reinterpret_cast<BTreeNodeHeader*>(&inner));
   //    std::cout << "i " << i << std::endl;
   // }
   // std::cout << "insert" << std::endl;
   // std::cout << "max " << BTreeNodeInner<int, BTreeNodeHeader*>::numberInners << "\n";

   // auto idx = inner.lower_bound(static_cast<int>(dist6(rng)));

   // int prev = 0;
   // for (auto it = inner.begin(); it < inner.end(); it++) {
   //    std::cout << inner.innerEntries[it].key << "\n";
   //    if (prev > inner.innerEntries[it].key) throw std::logic_error("cannot be bigger");
   // }

   // auto sepInfo = inner.split();

   // std::cout << "sepinfo " << sepInfo.sep << "\n";

   // for (auto it = inner.begin(); it < inner.end(); it++) {
   //    std::cout << inner.innerEntries[it].key << "\n";
   //    std::cout << inner.innerEntries[it].child << "\n";
   // }

   // std::cout << " "
   //           << "\n";
   // std::cout << "right "
   //           << "\n";
   // std::cout << " "
   //           << "\n";

   // auto rightNode = (decltype(inner)*)sepInfo.rightNode;
   // for (auto it = rightNode->begin(); it < rightNode->end(); it++) {
   //    std::cout << rightNode->innerEntries[it].key << "\n";
   //    std::cout << rightNode->innerEntries[it].child << "\n";
   // }

   return 0;
}
