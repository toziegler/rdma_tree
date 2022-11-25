#include <algorithm>
#include <array>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <gflags/gflags.h> 
#define GDB() std::raise(SIGINT);

constexpr uint64_t SIZE = 4096;
enum class NodeType : uint8_t { INNER = 1, LEAF = 2 };

struct BTreeNodeHeader {
   using count_t = uint16_t;
   NodeType type;
   count_t count{0};
   bool is_leaf() { return (type == NodeType::LEAF); }
   BTreeNodeHeader() = default;
   BTreeNodeHeader(NodeType type) : type(type) {}
};

template <typename Key>
struct SeparatorInfo {
   Key sep;
   BTreeNodeHeader* rightNode;
};

template <class Key>
struct FenceKeys {
   struct FenceKey {
      bool isInfinity{true};
      Key key;
   };
   FenceKey lower;  // exclusive
   FenceKey upper;  // inclusive
   bool isLowerInfinity() { return lower.isInfinity; }
   bool isUpperInfinity() { return upper.isInfinity; }
   FenceKey getLower() { return lower; }
   FenceKey getUpper() { return upper; }
   void setFences(FenceKey lower_, FenceKey upper_) {
      lower = lower_;
      upper = upper_;
   }
};

template <typename Node, typename Key, typename Value, uint64_t NODE_SIZE, NodeType TYPE>
struct BTreeNodeTrait : BTreeNodeHeader {
   using self_t = BTreeNodeTrait;
   using node_t = Node;
   using key_t = Key;
   using value_t = Value;
   using Pos = count_t;  // index

   static constexpr NodeType type = TYPE;
   static constexpr uint64_t bytes = NODE_SIZE;
   static constexpr uint64_t available_bytes = NODE_SIZE - sizeof(BTreeNodeHeader);

   BTreeNodeTrait() : BTreeNodeHeader(TYPE) {
      static_assert(std::numeric_limits<Pos>::max() > NODE_SIZE);  // pedantic check (just in case we have 1 byte keys)
   }
};

template <typename Key, typename Value, uint64_t NODE_SIZE = SIZE>
struct BTreeLeaf : BTreeNodeTrait<BTreeLeaf<Key, Value>, Key, Value, NODE_SIZE, NodeType::LEAF> {
   using super_t = BTreeNodeTrait<BTreeLeaf<Key, Value>, Key, Value, NODE_SIZE, NodeType::LEAF>;
   using super_t::available_bytes;
   using super_t::count;
   using typename super_t::key_t;
   using typename super_t::node_t;
   using typename super_t::Pos;
   using typename super_t::value_t;

   static constexpr uint64_t max_entries =
       (available_bytes - sizeof(FenceKeys<Key>)) / (sizeof(key_t) + sizeof(value_t));
   FenceKeys<Key> fenceKeys;
   std::array<key_t, max_entries> keys;
   std::array<value_t, max_entries> values;
   uint8_t padding[available_bytes - sizeof(FenceKeys<Key>) - (max_entries * (sizeof(key_t) + sizeof(value_t)))];

   BTreeLeaf() { static_assert(sizeof(BTreeLeaf) == NODE_SIZE, "btree node size problem"); }

   Pos lower_bound(const key_t& key) {
      return static_cast<Pos>(
          std::distance(std::begin(keys), std::lower_bound(std::begin(keys), std::begin(keys) + count, key)));
   }

   void insert(const key_t& key, const value_t& value) {
      Pos position = lower_bound(key);
      if (position != end()) {
         std::move(std::begin(keys) + position, std::begin(keys) + end(), std::begin(keys) + position + 1);
         std::move(std::begin(values) + position, std::begin(values) + end(), std::begin(values) + position + 1);
      }
      keys[position] = key;
      values[position] = value;
      count++;
   }

   bool update(const key_t& key, const value_t& value) {
      Pos position = lower_bound(key);
      if ((position == end()) || (key_at(position) != key)) return false;
      values[position] = value;
      return true;
   }

   void upsert(const key_t& key, const value_t& value) {
      if (update(key, value)) return;
      insert(key, value);
   }

   SeparatorInfo<key_t> split() {
      assert(count == max_entries);  // only split if full
      SeparatorInfo<key_t> sepInfo;
      auto* rightNode = new BTreeLeaf();
      Pos sepPosition = find_separator();
      sepInfo.sep = keys[sepPosition];
      sepInfo.rightNode = rightNode;
      // move from one node to the other; keep separator key in the left child
      std::move(std::begin(keys) + sepPosition + 1, std::begin(keys) + end(), std::begin(rightNode->keys));
      std::move(std::begin(values) + sepPosition + 1, std::begin(values) + end(), std::begin(rightNode->values));
      // update counts
      rightNode->count = count - static_cast<Pos>((sepPosition + static_cast<Pos>(1)));
      count = count - rightNode->count;
      // fence
      rightNode->fenceKeys.setFences({.isInfinity = false, .key = sepInfo.sep},
                                     fenceKeys.getUpper());  // order is important
      fenceKeys.setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sepInfo.sep});
      return sepInfo;
   };

   Pos find_separator() { return count / 2; }
   bool has_space() { return (count < max_entries); }
   Pos begin() { return 0; }
   Pos end() { return count; }  // returns one index behind valid index as usual
   inline key_t key_at(Pos idx) { return keys[idx]; }
   inline value_t value_at(Pos idx) { return values[idx]; }

   // for debugging
   void print_keys() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << keys[idx] << "\n"; }
   }
   void print_values() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << values[idx] << "\n"; }
   }
};

template <typename Key, typename Value, uint64_t NODE_SIZE = SIZE>
struct BTreeInner : BTreeNodeTrait<BTreeInner<Key, Value>, Key, Value, NODE_SIZE, NodeType::INNER> {
   using super_t = BTreeNodeTrait<BTreeInner<Key, Value>, Key, Value, NODE_SIZE, NodeType::INNER>;
   using super_t::available_bytes;
   using super_t::count;
   using typename super_t::key_t;
   using typename super_t::node_t;
   using typename super_t::Pos;
   using typename super_t::value_t;

   static constexpr uint64_t max_entries =
       (available_bytes - sizeof(value_t) - sizeof(FenceKeys<Key>)) / (sizeof(key_t) + sizeof(value_t));
   FenceKeys<Key> fenceKeys;
   std::array<key_t, max_entries> keys;
   std::array<value_t, max_entries + 1> values;

   BTreeInner() { static_assert(sizeof(BTreeInner) == NODE_SIZE, "btree node size problem"); }

   Pos lower_bound(const key_t& key) {
      return static_cast<Pos>(
          std::distance(std::begin(keys), std::lower_bound(std::begin(keys), std::begin(keys) + count, key)));
   }

   Pos upper_bound(const key_t& key) {
      return static_cast<Pos>(
          std::distance(std::begin(keys), std::upper_bound(std::begin(keys), std::begin(keys) + count, key)));
   }

   value_t next_child(const key_t& key) {
      auto pos = lower_bound(key);
      return values[pos];
   }

   bool insert(const key_t& newSep, const value_t& left, const value_t& right) {
      Pos position = lower_bound(newSep);
      std::move(std::begin(keys) + position, std::begin(keys) + end(), std::begin(keys) + position + 1);
      // end() + 1 handles the n+1 childs
      std::move(std::begin(values) + position, std::begin(values) + end() + 1, std::begin(values) + position + 1);
      keys[position] = newSep;
      values[position] = left;
      values[position + 1] = right;  // this updates the old left pointer
      count++;
      return true;
   }

   SeparatorInfo<key_t> split() {
      assert(count == max_entries);  // only split if full
      SeparatorInfo<key_t> sepInfo;
      auto* rightNode = new BTreeInner();
      auto sepPosition = find_separator();
      sepInfo.sep = keys[sepPosition];
      sepInfo.rightNode = rightNode;
      // move from one node to the other; keep separator key in the left child
      std::move(std::begin(keys) + sepPosition + 1, std::begin(keys) + end(), std::begin(rightNode->keys));
      // need to copy one more
      std::move(std::begin(values) + sepPosition + 1, std::begin(values) + end() + 1, std::begin(rightNode->values));
      // update counts
      rightNode->count = count - static_cast<Pos>((sepPosition + 1));
      count = count - static_cast<Pos>(rightNode->count - 1);  // -1 removes the sep key but ptr is kept
      // set fences
      rightNode->fenceKeys.setFences({.isInfinity = false, .key = sepInfo.sep},
                                     fenceKeys.getUpper());  // order is important
      fenceKeys.setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sepInfo.sep});
      return sepInfo;
   }

   Pos find_separator() { return count / 2; }
   bool has_space() { return (count < max_entries); }
   Pos begin() { return 0; }
   Pos end() { return count; }  // returns one index behind valid index as usual
   inline key_t key_at(Pos idx) { return keys[idx]; }
   inline value_t value_at(Pos idx) { return values[idx]; }

   // for debugging
   void print_keys() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << keys[idx] << "\n"; }
   }
   void print_values() {
      auto idx = begin();
      for (; idx < end(); idx++) { std::cout << values[idx] << "\n"; }
      std::cout << values[idx] << "\n";  // print n+1 child
   }
};


// does the traversal until the leaf layer and then hands over to the leaf logic 
namespace inner{

struct InnerTraversalAbstract{};

template<typename InnerNode>
struct Traversal : public InnerTraversalAbstract{

   void traverse(/* metadata */){
      // get root and height from md
      // iterate until we reach last inner node
      // return last inner node 
   }
   // traverse // goes down 
   // ensure_space -> iterates and finds tosplit node 
   // try_split -> splits inner path if no space 
   // try_complete_leaf_split(Sep, TmpPtr) -> if fails dealloc tmpptr and ensure space on inner path
   
};
}

// template <typename InnerPolicy, typename LeafPolicy>
// struct BTreeTrait {
//    using leaf_t = LeafNode;
//    using key_t = typename leaf_t::key_t;
//    using value_t = typename leaf_t::value_t;
// };

// template <typename InnerNode, typename LeafNode>
// struct BTree : public BTreeTrait<InnerNode, LeafNode> {
//    using super_t = BTreeTrait<InnerNode, LeafNode>;
//    using self_t = BTree<InnerNode, LeafNode>;
//    using header_t = BTreeNodeHeader;
//    using leaf_t = LeafNode;
//    using inner_t = InnerNode;
//    using typename super_t::key_t;
//    using typename super_t::value_t;
//    BTreeNodeHeader* root;
//    BTree() : root(new LeafNode()) {}

//    template <typename FN>
//    std::pair<header_t*, header_t*> traverse_inner(const key_t& key, FN stop_condition) {
//       header_t* parent = nullptr;
//       header_t* node = root;
//       while (!node->is_leaf() && !stop_condition(node)) {
//          auto inner = static_cast<InnerNode*>(node);
//          parent = node;
//          node = inner->next_child(key);
//       }
//       return {parent, node};
//    }

//    template <typename FN>
//    std::pair<header_t*, header_t*> traverse_inner_upper_bound(const key_t& key, FN stop_condition) {
//       header_t* parent = nullptr;
//       header_t* node = root;
//       while (!node->is_leaf() && !stop_condition(node)) {
//          auto inner = static_cast<InnerNode*>(node);
//          parent = node;
//          auto pos = inner->upper_bound(key);
//          node = inner->values[pos];
//       }
//       return {parent, node};
//    }

//    bool lookup(const key_t& key, value_t& retValue) {
//       auto [parent, node] = traverse_inner(key, []([[maybe_unused]] header_t* currentNode) { return false; });
//       auto leaf = static_cast<LeafNode*>(node);
//       auto idx = leaf->lower_bound(key);
//       if (idx == leaf->end() && leaf->key_at(idx) != key) return false;
//       retValue = leaf->value_at(idx);
//       return true;
//    }

//    void ensure_space(header_t* toSplit, key_t key) {
//       auto [parent, node] = traverse_inner(key, [&](header_t* currentNode) { return currentNode == toSplit; });
//       while (!node->is_leaf() && node != toSplit) {
//          auto inner = static_cast<InnerNode*>(node);
//          parent = node;
//          node = inner->next_child(key);
//       }
//       if (node == toSplit) return try_split(node, parent, key);
//       throw std::logic_error("could not find node");
//    }

//    void try_split(header_t* node, header_t* parent, const key_t& key) {
//       if (node == root) {
//          parent = new InnerNode();
//          root = parent;
//       }
//       if (static_cast<InnerNode*>(parent)->has_space()) {
//          SeparatorInfo<key_t> sep;
//          if (node->is_leaf())
//             sep = static_cast<LeafNode*>(node)->split();
//          else
//             sep = static_cast<InnerNode*>(node)->split();
//          static_cast<InnerNode*>(parent)->insert(sep.sep, node, sep.rightNode);
//          return;
//       }
//       // split parent
//       ensure_space(parent, key);
//    }

//    void insert(const key_t& key, const value_t& value) {
//       for (uint64_t repeatCounter = 0;; repeatCounter++) {
//          auto [parent, node] = traverse_inner(key, []([[maybe_unused]] header_t* currentNode) { return false; });
//          auto leaf = static_cast<LeafNode*>(node);
//          if (leaf->has_space()) {
//             leaf->insert(key, value);
//             return;
//          }
//          try_split(leaf, parent, key);
//       }
//    }

//    bool update(const key_t& key, const value_t& value) {
//       auto [parent, node] = traverse_inner(key, []([[maybe_unused]] header_t* currentNode) { return false; });
//       auto leaf = static_cast<LeafNode*>(node);
//       return leaf->update(key, value);
//    }
// };

#include <random>

int main(int argc, char* argv[]) {
   using Leaf = BTreeLeaf<int, int>;
   using Inner = BTreeInner<int, BTreeNodeHeader*>;
   
   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   /*
   {
      BTree<Inner, Leaf> tree;

      constexpr int KEYS = 1e6;

      for (int k_i = 1; k_i <= KEYS; k_i++) { tree.insert(k_i, k_i); }

      for (int k_i = 1; k_i <= KEYS; k_i++) {
         int v_i;
         if (!tree.lookup(k_i, v_i)) throw std::logic_error("key not found");
      }
   }

   {  // reverse test
      BTree<Inner, Leaf> tree;

      constexpr int KEYS = 1e6;

      for (int k_i = KEYS; k_i > 0; k_i--) { tree.insert(k_i, k_i); }

      for (int k_i = KEYS; k_i > 0; k_i--) {
         int v_i;
         if (!tree.lookup(k_i, v_i)) throw std::logic_error("key not found");
      }
   }

   {
      constexpr int KEYS = 1e6;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i > 0; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(200, 1000, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 801) throw std::logic_error("range scan did not find expected key T1");
   }

   {
      // missing key range test from beginning
      constexpr int KEYS = 1e6;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i > 500; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(200, 1000, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 500) throw std::logic_error("range scan did not find expected key T2");
   }

   {
      // missing key range complete
      constexpr int KEYS = 1e6;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i > 2000; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(200, 1000, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 0) throw std::logic_error("range scan did not find expected key T3");
   }

   {
      // missing key range test from end
      constexpr int KEYS = 800;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i > 1; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(200, 1000, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 601) throw std::logic_error("range scan did not find expected key T2");
   }

   
   {
      // missing key range larger
      constexpr int KEYS = 800;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i > 1; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(1000, 100000, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 0) throw std::logic_error("range scan did not find expected key T2");
   }
   
   std::cout << "empty tree test" << std::endl;

   {
      // test scan on only one node, i.e., root
      constexpr int KEYS = 10;
      struct RangeTree : public RangeScannable<Tree>, public Tree {};
      RangeTree tree;
      for (int k_i = KEYS; k_i >= 0; k_i--) { tree.insert(k_i, k_i); }
      size_t keysRetrieved{0};
      tree.range_scan(1, 10, [&](int key, int leaf) { keysRetrieved++; });
      std::cout << "kr " << keysRetrieved << std::endl;
      if (keysRetrieved != 10) throw std::logic_error("range scan did not find expected key T4");
   }
   */
   return 0;
}
