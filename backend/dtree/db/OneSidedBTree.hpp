#pragma once
#include <immintrin.h>
#include <sched.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "Defs.hpp"
#include "OneSidedLatches.hpp"
#include "OneSidedTypes.hpp"
//=== One-sided B-Tree ===//

namespace dtree {
namespace onesided {

using Pos = uint16_t;

template <typename Key>
struct SeparatorInfo {
   Key sep;
   RemotePtr rightNode;
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

struct BTreeHeader : public PageHeader {
   using header = PageHeader;
   static constexpr uint64_t bytes = BTREE_NODE_SIZE;
   uint16_t count{0};
   void setNodeType(BTreeNodeType node_type) { header::btpg.node_type = node_type; }
   BTreeNodeType getNodeType() { return header::btpg.node_type; }
   BTreeHeader(BTreeNodeType node_type) : PageHeader(PType_t::BTREE_NODE) { setNodeType(node_type); }
};

template <typename Key, typename Value>
struct BTreeLeaf : public BTreeHeader {
   using super = BTreeHeader;
   static constexpr uint64_t leaf_size{(BTREE_NODE_SIZE - sizeof(BTreeHeader) - sizeof(FenceKeys<Key>))};
   static constexpr uint64_t max_entries{leaf_size / (sizeof(Key) + sizeof(Value))};
   static constexpr uint64_t bytes_padding{leaf_size - max_entries * (sizeof(Key) + sizeof(Value))};
   FenceKeys<Key> fenceKeys;
   std::array<Key, max_entries> keys;
   std::array<Value, max_entries> values;
   uint8_t padding[bytes_padding];

   BTreeLeaf() : BTreeHeader(BTreeNodeType::LEAF) {
      static_assert(sizeof(BTreeLeaf) == BTREE_NODE_SIZE, "btree node size problem");
   }

   Pos lower_bound(const Key& key) {
      return static_cast<Pos>(
          std::distance(std::begin(keys), std::lower_bound(std::begin(keys), std::begin(keys) + count, key)));
   }

   bool lookup(const Key& key, Value& retValue) {
      auto idx = lower_bound(key);
      if (idx == end() && key_at(idx) != key) return false;
      retValue = value_at(idx);
      return true;
   }
   void insert(const Key& key, const Value& value) {
      Pos position = lower_bound(key);
      if (position != end()) {
         std::move(std::begin(keys) + position, std::begin(keys) + end(), std::begin(keys) + position + 1);
         std::move(std::begin(values) + position, std::begin(values) + end(), std::begin(values) + position + 1);
      }
      keys[position] = key;
      values[position] = value;
      count++;
   }

   bool update(const Key& key, const Value& value) {
      Pos position = lower_bound(key);
      if ((position == end()) || (key_at(position) != key)) return false;
      values[position] = value;
      return true;
   }

   void upsert(const Key& key, const Value& value) {
      if (update(key, value)) return;
      insert(key, value);
   }

   SeparatorInfo<Key> split() {
      assert(count == max_entries);  // only split if full
      SeparatorInfo<Key> sepInfo;
      AllocationLatch<BTreeLeaf> rightNode;
      Pos sepPosition = find_separator();
      sepInfo.sep = keys[sepPosition];
      sepInfo.rightNode = rightNode.remote_ptr;
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
      rightNode.unlatch();
      return sepInfo;
   };

   Pos find_separator() { return count / 2; }
   bool has_space() { return (count < max_entries); }
   Pos begin() { return 0; }
   Pos end() { return count; }
   // returns one it behind valid it as usual inline
   Key key_at(Pos idx) { return keys[idx]; }
   inline Value value_at(Pos idx) { return values[idx]; }
   void print_keys() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << keys[idx] << "\n"; }
   }
   void print_values() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << values[idx] << "\n"; }
   }
};

template <typename Key>
struct BTreeInner : public BTreeHeader {
   using super = BTreeHeader;
   static constexpr uint64_t inner_size{
       (BTREE_NODE_SIZE - sizeof(BTreeHeader) - sizeof(RemotePtr) - sizeof(FenceKeys<Key>))};
   static constexpr uint64_t max_entries{inner_size / (sizeof(Key) + sizeof(RemotePtr))};
   static constexpr uint64_t bytes_padding{inner_size - max_entries * (sizeof(Key) + sizeof(Value))};
   FenceKeys<Key> fenceKeys;
   std::array<Key, max_entries> sep;
   std::array<RemotePtr, max_entries + 1> children;
   uint8_t padding[bytes_padding];

   BTreeInner() : BTreeHeader(BTreeNodeType::INNER) {
      static_assert(sizeof(BTreeInner) == BTREE_NODE_SIZE, "btree node size problem");
   }

   Pos lower_bound(const Key& key) {
      return static_cast<Pos>(
          std::distance(std::begin(sep), std::lower_bound(std::begin(sep), std::begin(sep) + count, key)));
   }

   Pos upper_bound(const Key& key) {
      return static_cast<Pos>(
          std::distance(std::begin(sep), std::upper_bound(std::begin(sep), std::begin(sep) + count, key)));
   }

   RemotePtr next_child(const Key& key) {
      auto pos = lower_bound(key);
      return children[pos];
   }

   bool insert(const Key& newSep, const RemotePtr& left, const RemotePtr& right) {
      Pos position = lower_bound(newSep);
      std::move(std::begin(sep) + position, std::begin(sep) + end(), std::begin(sep) + position + 1);
      // end() + 1 handles the n+1 childs
      std::move(std::begin(children) + position, std::begin(children) + end() + 1, std::begin(children) + position + 1);
      sep[position] = newSep;
      children[position] = left;
      children[position + 1] = right;  // this updates the old left pointer
      count++;
      return true;
   }

   SeparatorInfo<Key> split() {
      assert(count == max_entries);  // only split if full
      SeparatorInfo<Key> sepInfo;
      AllocationLatch<BTreeInner> rightNode;
      auto sepPosition = find_separator();
      sepInfo.sep = sep[sepPosition];
      sepInfo.rightNode = rightNode.remote_ptr;
      // move from one node to the other; keep separator key in the left child
      std::move(std::begin(sep) + sepPosition + 1, std::begin(sep) + end(), std::begin(rightNode->sep));
      // need to copy one more
      std::move(std::begin(children) + sepPosition + 1, std::begin(children) + end() + 1,
                std::begin(rightNode->children));
      // update counts
      rightNode->count = count - static_cast<Pos>((sepPosition + 1));
      count = static_cast<Pos>(count - static_cast<Pos>(rightNode->count) - static_cast<Pos>(1));  // -1 removes the sep key but ptr is kept
      // set fences
      rightNode->fenceKeys.setFences({.isInfinity = false, .key = sepInfo.sep},
                                     fenceKeys.getUpper());  // order is important
      fenceKeys.setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sepInfo.sep});
      rightNode.unlatch();
      return sepInfo;
   }

   Pos find_separator() { return count / 2; }
   bool has_space() { return (count < max_entries); }
   Pos begin() { return 0; }
   Pos end() { return count; }  // returns one it behind valid it as usual
   inline Key key_at(Pos idx) { return sep[idx]; }
   inline RemotePtr value_at(Pos idx) { return children[idx]; }

   // for debugging
   void print_keys() {
      for (auto idx = begin(); idx < end(); idx++) { std::cout << sep[idx] << "\n"; }
   }
   void print_values() {
      auto idx = begin();
      for (; idx < end(); idx++) { std::cout << children[idx] << "\n"; }
      std::cout << children[idx] << "\n";  // print n+1 child
   }
};
// this is used in the traversal as we do not know which kind of node we will retrieve
struct NodePlaceholder : public BTreeHeader {
   uint8_t padding[BTREE_NODE_SIZE - sizeof(BTreeHeader)];
   // cast to leaf or inner
   template <class T>
   T* as() {
      return (T*)(this);
   }
   BTreeHeader* operator->() { return static_cast<BTreeHeader*>(this); }
};

// client driven
template <typename Key, typename Value>
struct BTree {
   using Leaf = BTreeLeaf<Key, Value>;
   using Inner = BTreeInner<Key>;
   using SepInfo = SeparatorInfo<Key>;
   RemotePtr metadata;
   BTree(RemotePtr metadata) : metadata(metadata) {}
   // insert
   void make_new_root(GuardX<MetadataPage>& parent, Key separator, RemotePtr left, RemotePtr right) {
      AllocationLatch<Inner> new_root;
      new_root->insert(separator, left, right);
      parent->setRootPtr(new_root.remote_ptr);
      parent->setHeight(parent->getHeight());
      new_root.unlatch();
   }
   // helper functions for range scan
   template <typename FN>
   std::pair<bool, Key> initial_traversal(const Key& moving_start,
                                          FN iterate_leaf) {  // find first inner node with lower bound search
      GuardO<MetadataPage> g_metadata(metadata);
      GuardO<NodePlaceholder> parent;
      GuardO<NodePlaceholder> node(g_metadata->getRootPtr());
      g_metadata.checkVersionAndRestart();
      while (node->getNodeType() == BTreeNodeType::INNER) {
         parent = std::move(node);
         node = GuardO<NodePlaceholder>(parent->as<Inner>()->next_child(moving_start));
         parent.checkVersionAndRestart();
      }
      // handle edge case of root == leaf
      if (parent.not_used()) {
         ensure(node->getNodeType() == BTreeNodeType::LEAF);
         iterate_leaf(node->as<Leaf>());
         return {true, moving_start};  // finished scan
      }
      node.release();
      // parent can be used to prefetch should be inner node
      Pos it_inner = parent->as<Inner>()->lower_bound(moving_start);
      // iterate inner and get all leafes
      for (; it_inner <= parent->as<Inner>()->end(); it_inner++) {
         // fetch new leaf
         GuardO<NodePlaceholder> leaf(parent->as<Inner>()->value_at(it_inner));
         auto finished = iterate_leaf(leaf->as<Leaf>());
         if (finished || leaf->as<Leaf>()->fenceKeys.getUpper().isInfinity)
            return {true, moving_start};  // finished scan
      }
      // continue to scan with adjusted search method;
      return {false, parent->as<Inner>()->fenceKeys.getUpper().key};  // finished scan
   }

   // uses upper bound traversal to steer the scan
   template <typename FN>
   std::pair<bool, Key> consecutive_traversal(const Key& moving_start,
                                              FN iterate_leaf) {  // find first inner node with lower bound search
      GuardO<MetadataPage> g_metadata(metadata);
      GuardO<NodePlaceholder> parent;
      GuardO<NodePlaceholder> node(g_metadata->getRootPtr());
      g_metadata.checkVersionAndRestart();
      while (node->getNodeType() == BTreeNodeType::INNER) {
         parent = std::move(node);
         auto idx = parent->as<Inner>()->upper_bound(moving_start);
         node = GuardO<NodePlaceholder>(parent->as<Inner>()->children[idx]);
         parent.checkVersionAndRestart();
      }
      node.release();
      // we can scan from the beginning
      auto new_start = parent->as<Inner>()->fenceKeys.getLower().key;
      Pos it_inner = parent->as<Inner>()->lower_bound(new_start);
      // iterate inner and get all leafes
      for (; it_inner <= parent->as<Inner>()->end(); it_inner++) {
         // fetch new leaf
         GuardO<NodePlaceholder> leaf(parent->as<Inner>()->value_at(it_inner));
         auto finished = iterate_leaf(leaf->as<Leaf>());
         if (finished || leaf->as<Leaf>()->fenceKeys.getUpper().isInfinity)
            return {true, moving_start};  // finished scan
      }
      // continue to scan with adjusted search method;
      return {false, parent->as<Inner>()->fenceKeys.getUpper().key};  // finished scan
   }
   // this function scans one inner node and returns
   template <typename FN>
   void range_scan(const Key from, const Key to, FN scan_function, std::function<void()> undo) {
      [[maybe_unused]] bool first_traversal = true;  // need to use lower_bound search
      [[maybe_unused]] bool scan_finished = false;   // need to use lower_bound search
      auto moving_start = from;                      // is used to steer the scan
      auto iterate_leaf = [&](Leaf* leaf) -> bool {
         for (Pos it = leaf->lower_bound(moving_start); it != leaf->end(); it++) {
            auto c_key = leaf->key_at(it);
            if (c_key > to) return true;  // scan finished
            scan_function(c_key, leaf->value_at(it));
            moving_start = c_key;
         }
         return false;  // continue to scan
      };
      for ([[maybe_unused]] size_t repeat = 0;; repeat++) {
         try {
            if (first_traversal)
               std::tie(scan_finished, moving_start) = initial_traversal(moving_start, iterate_leaf);
            else if (!scan_finished) {
               std::tie(scan_finished, moving_start) = consecutive_traversal(moving_start, iterate_leaf);
            } else
               return;
            first_traversal = false;
         } catch (const OLCRestartException&) {
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
            // restart at the beginning 
            moving_start = from; 
            first_traversal = true;
            scan_finished = false;
            undo();
         }
      }
   }

   bool lookup(Key key, Value& retValue) {
      for ([[maybe_unused]] size_t repeat = 0;; repeat++) {
         try {
            GuardO<MetadataPage> g_metadata(metadata);
            GuardO<NodePlaceholder> parent;
            GuardO<NodePlaceholder> node(g_metadata->getRootPtr());
            g_metadata.checkVersionAndRestart();
            while (node->getNodeType() == BTreeNodeType::INNER) {
               parent = std::move(node);
               node = GuardO<NodePlaceholder>(parent->as<Inner>()->next_child(key));
               parent.checkVersionAndRestart();
            }
            GuardO<NodePlaceholder> leaf(std::move(node));
            return leaf->as<Leaf>()->lookup(key, retValue);
         } catch (const OLCRestartException&) {
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
         }
      }
   }

   void insert(Key key, Value value) {
      for ([[maybe_unused]] size_t repeat = 0;; repeat++) {
         try {
            GuardO<MetadataPage> g_metadata(metadata);
            GuardO<NodePlaceholder> parent;
            GuardO<NodePlaceholder> node(g_metadata->getRootPtr());
            g_metadata.checkVersionAndRestart();
            while (node->getNodeType() == BTreeNodeType::INNER) {
               // split logic
               if (!node->as<Inner>()->has_space()) {
                  // split root
                  if (parent.not_used()) {
                     GuardX<MetadataPage> md_parent(std::move(g_metadata));
                     GuardX<NodePlaceholder> x_node(std::move(node));
                     auto sepInfo = x_node->as<Inner>()->split();
                     make_new_root(md_parent, sepInfo.sep, x_node.latch.remote_ptr, sepInfo.rightNode);
                     throw OLCRestartException(); 
                  }
                  // split inner node
                  GuardX<NodePlaceholder> x_parent(std::move(parent));
                  GuardX<NodePlaceholder> x_node(std::move(node));
                  auto sepInfo = x_node->as<Inner>()->split();
                  x_parent->as<Inner>()->insert(sepInfo.sep, x_node.latch.remote_ptr, sepInfo.rightNode);
                  throw OLCRestartException();
               }
               parent = std::move(node);
               node = GuardO<NodePlaceholder>(parent->as<Inner>()->next_child(key));
               parent.checkVersionAndRestart();
            }

            if (!node->as<Leaf>()->has_space()) {
               if (parent.not_used()) {
                  GuardX<MetadataPage> md_parent(std::move(g_metadata));
                  GuardX<NodePlaceholder> leaf(std::move(node));
                  auto sepInfo = leaf->as<Leaf>()->split();
                  make_new_root(md_parent, sepInfo.sep, leaf.latch.remote_ptr, sepInfo.rightNode);
                  throw OLCRestartException();
               }
               GuardX<NodePlaceholder> x_parent(std::move(parent));
               GuardX<NodePlaceholder> leaf(std::move(node));
               auto sepInfo = leaf->as<Leaf>()->split();
               x_parent->as<Inner>()->insert(sepInfo.sep, leaf.latch.remote_ptr, sepInfo.rightNode);
               throw OLCRestartException();
            }
            GuardX<NodePlaceholder> leaf(std::move(node));
            leaf->as<Leaf>()->upsert(key, value);
            return;
         } catch (const OLCRestartException&) {
            ensure(threads::onesided::Worker::my().local_rmemory.get_size() == CONCURRENT_LATCHES);
         }
      }
   }
};
}  // namespace onesided
}  // namespace dtree
