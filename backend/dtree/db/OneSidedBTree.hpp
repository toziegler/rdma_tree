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
#include <iostream>
#include <type_traits>
#include <vector>

#include "Defs.hpp"
#include "OneSidedTypes.hpp"
//=== One-sided B-Tree ===//

namespace onesided {

using Pos = uint64_t;

template <typename Key>
struct SeparatorInfo {
   Key sep;
   RemotePtr* rightNode;
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
   Pos end() { return count; }
   //returns one index behind valid index as usual inline 
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
       std::move(std::begin(children) + position, std::begin(children) + end() + 1,
                 std::begin(children) + position + 1);
       sep[position] = newSep;
       children[position] = left;
       children[position + 1] = right;  // this updates the old left pointer
       count++;
       return true;
    }

    SeparatorInfo<Key> split() {
       assert(count == max_entries);  // only split if full
       SeparatorInfo<Key> sepInfo;
       auto* rightNode = new BTreeInner();
       auto sepPosition = find_separator();
       sepInfo.sep = sep[sepPosition];
       sepInfo.rightNode = rightNode;
       // move from one node to the other; keep separator key in the left child
       std::move(std::begin(sep) + sepPosition + 1, std::begin(sep) + end(), std::begin(rightNode->keys));
       // need to copy one more
       std::move(std::begin(children) + sepPosition + 1, std::begin(children) + end() + 1,
                 std::begin(rightNode->values));
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
         
// client driven
template <typename Key, typename Value>
struct BTree {
    RemotePtr metadata;
    BTree(RemotePtr metadata) : metadata(metadata) {}
    // insert
    void insert(Key key, Value value) {
       for (size_t repeat = 0;; repeat++) {
         try {
          // latch parent metadata page 
            
          // latch root
         } catch (const OLCRestartException&) {
         } 
             
       }
    }
    // lookup
    // scan
};
}  // namespace onesided
