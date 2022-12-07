#include <immintrin.h>
#include <sched.h>
#include <atomic>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>
#include "Defs.hpp"

//=== Two-sided B+Tree ===//
// This tree is used by the message handlers on the storage node

namespace twosided {

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

static const uint64_t pageSize = BTREE_NODE_SIZE;

struct OptLock {
   std::atomic<uint64_t> typeVersionLockObsolete{0b100};

   bool isLocked(uint64_t version) { return ((version & 0b10) == 0b10); }

   uint64_t readLockOrRestart(bool& needRestart) {
      uint64_t version;
      version = typeVersionLockObsolete.load();
      if (isLocked(version) || isObsolete(version)) {
         _mm_pause();
         needRestart = true;
      }
      return version;
   }

   void writeLockOrRestart(bool& needRestart) {
      uint64_t version;
      version = readLockOrRestart(needRestart);
      if (needRestart) return;

      upgradeToWriteLockOrRestart(version, needRestart);
      if (needRestart) return;
   }

   void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart) {
      if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
         version = version + 0b10;
      } else {
         _mm_pause();
         needRestart = true;
      }
   }

   void writeUnlock() { typeVersionLockObsolete.fetch_add(0b10); }

   bool isObsolete(uint64_t version) { return (version & 1) == 1; }

   void checkOrRestart(uint64_t startRead, bool& needRestart) const { readUnlockOrRestart(startRead, needRestart); }

   void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const { needRestart = (startRead != typeVersionLockObsolete.load()); }

   void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b11); }
};

struct NodeBase : public OptLock {
   PageType type;
   uint16_t count;
};

struct BTreeLeafBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeLeaf;
};

template <class Key>
struct FenceKey {
   bool isInfinity = true;
   Key key;
};
template <class Key>
std::ostream& operator<<(std::ostream& os, const FenceKey<Key>& c) {
   os << "is inf. " << ((c.isInfinity) ? "yes" : "no") << " " << c.key;
   return os;
}
template <class Key>
struct FenceKeys {
   FenceKey<Key> lower;  // exclusive
   FenceKey<Key> upper;  // inclusive
   bool isLowerInfinity() { return lower.isInfinity; }
   bool isUpperInfinity() { return upper.isInfinity; }
   FenceKey<Key> getLower() { return lower; }
   FenceKey<Key> getUpper() { return upper; }
};
// -------------------------------------------------------------------------------------
template <class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   using FK = FenceKeys<Key>;
   // -------------------------------------------------------------------------------------
   // iterator
   struct asc_iterator {
      BTreeLeaf& leaf;
      uint64_t current_pos = 0;

      asc_iterator(BTreeLeaf& leaf) : leaf(leaf), current_pos(0){};
      asc_iterator(BTreeLeaf& leaf, uint64_t pos) : leaf(leaf), current_pos(pos){};

      asc_iterator& operator++() {
         current_pos++;
         return *this;
      }

      uint64_t operator*() const { return current_pos; }

      friend bool operator==(const asc_iterator& lhs, const asc_iterator& rhs) {
         return (&lhs.leaf == &rhs.leaf && lhs.current_pos == rhs.current_pos);
      }
      friend bool operator!=(const asc_iterator& lhs, const asc_iterator& rhs) { return !(lhs == rhs); }
   };
   // -------------------------------------------------------------------------------------
   struct desc_iterator {
      BTreeLeaf& leaf;
      uint64_t current_pos = 0;

      desc_iterator(BTreeLeaf& leaf) : leaf(leaf), current_pos(0){};
      desc_iterator(BTreeLeaf& leaf, uint64_t pos) : leaf(leaf), current_pos(pos){};

      desc_iterator& operator++() {
         current_pos--;
         return *this;
      }

      uint64_t operator*() const { return current_pos; }

      friend bool operator==(const desc_iterator& lhs, const desc_iterator& rhs) {
         return (&lhs.leaf == &rhs.leaf && lhs.current_pos == rhs.current_pos);
      }
      friend bool operator!=(const desc_iterator& lhs, const desc_iterator& rhs) { return !(lhs == rhs); }
   };
   desc_iterator rbegin() { return desc_iterator(*this, count); }
   desc_iterator rend() { return desc_iterator(*this, 0); }
   asc_iterator begin() { return asc_iterator(*this, 0); }
   asc_iterator end() { return asc_iterator(*this, count); }
   // -------------------------------------------------------------------------------------
   static const uint64_t maxEntries = (pageSize - sizeof(NodeBase) - sizeof(FK)) / (sizeof(Key) + sizeof(Payload));
   static const uint64_t underflowSize = maxEntries / 4;
   // -------------------------------------------------------------------------------------
   FK fenceKeys;
   Key keys[maxEntries];
   Payload payloads[maxEntries];

   BTreeLeaf() {
      count = 0;
      type = typeMarker;
   }

   bool isFull() { return count == maxEntries; };
   bool isUnderflow(){return count <= underflowSize;}
   // -------------------------------------------------------------------------------------
   void setFences(FenceKey<Key> lower, FenceKey<Key> upper) {
      fenceKeys.lower = lower;
      fenceKeys.upper = upper;
   }
   // -------------------------------------------------------------------------------------
   // finds the first key which matches k if exists
   // otherwise returns next larger key
   // meaning a key which is not less than k
   unsigned lowerBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k > keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   unsigned lowerBoundBF(Key k) {
      auto base = keys;
      unsigned n = count;
      while (n > 1) {
         const unsigned half = n / 2;
         base = (base[half] < k) ? (base + half) : base;
         n -= half;
      }
      return (*base < k) + base - keys;
   }

   void insert(Key k, Payload p) {
      assert(count < maxEntries);
      if (count) {
         unsigned pos = lowerBound(k);
         if ((pos < count) && (keys[pos] == k)) {
            // Upsert
            payloads[pos] = p;
            return;
         }
         memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos));
         memmove(payloads + pos + 1, payloads + pos, sizeof(Payload) * (count - pos));
         keys[pos] = k;
         payloads[pos] = p;
      } else {
         keys[0] = k;
         payloads[0] = p;
      }
      count++;
   }

   bool remove(Key k) {
      if (count) {
         unsigned pos = lowerBound(k);
         if ((pos < count) && (keys[pos] == k)) {
            memmove(keys + pos, keys + pos + 1, sizeof(Key) * (count - (pos + 1)));
            memmove(payloads + pos, payloads + pos + 1, sizeof(Payload) * (count - (pos + 1)));
            count--;
            return true;
         }
      }
      return false;
   }

   BTreeLeaf* split(Key& sep) {
      BTreeLeaf* newLeaf = new BTreeLeaf();
      newLeaf->count = count - (count / 2);
      count = count - newLeaf->count;
      memcpy(newLeaf->keys, keys + count, sizeof(Key) * newLeaf->count);
      memcpy(newLeaf->payloads, payloads + count, sizeof(Payload) * newLeaf->count);
      sep = keys[count - 1];
      newLeaf->setFences({.isInfinity = false, .key = sep}, fenceKeys.getUpper());  // order is important
      setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sep});
      return newLeaf;
   }


};

struct BTreeInnerBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeInner;
};

template <class Key>
struct BTreeInner : public BTreeInnerBase {
   using FK = FenceKeys<Key>;
   // -------------------------------------------------------------------------------------
   static const uint64_t maxEntries = (pageSize - sizeof(NodeBase) - sizeof(FK)) / (sizeof(Key) + sizeof(NodeBase*));
   // -------------------------------------------------------------------------------------
   FK fenceKeys;
   NodeBase* children[maxEntries];
   Key keys[maxEntries];

   BTreeInner() {
      count = 0;
      type = typeMarker;
   }

   bool isFull() { return count == (maxEntries - 1); };
   // -------------------------------------------------------------------------------------
   void setFences(FenceKey<Key> lower, FenceKey<Key> upper) {
      fenceKeys.lower = lower;
      fenceKeys.upper = upper;
   }
   // -------------------------------------------------------------------------------------
   unsigned lowerBoundBF(Key k) {
      auto base = keys;
      unsigned n = count;
      while (n > 1) {
         const unsigned half = n / 2;
         base = (base[half] < k) ? (base + half) : base;
         n -= half;
      }
      return (*base < k) + base - keys;
   }

   // finds the first key which is larger than k
   unsigned upperBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k >= keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   // finds the first key which matches k if exists
   // otherwise returns next larger key
   // meaning a key which is not less than k
   unsigned lowerBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k > keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   BTreeInner* split(Key& sep) {
      BTreeInner* newInner = new BTreeInner();
      newInner->count = count - (count / 2);
      count = static_cast<uint16_t>(count - newInner->count - static_cast<uint16_t>(1));
      sep = keys[count];
      memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
      memcpy(newInner->children, children + count + 1, sizeof(NodeBase*) * (newInner->count + 1));
      newInner->setFences({.isInfinity = false, .key = sep}, fenceKeys.getUpper());  // order is important otherwise fence overwritten
      setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sep});
      return newInner;
   }

   
   bool remove(uint64_t pos) {
      if (count) {
         if ((pos < count)) {                 
            memmove(keys + pos, keys + pos+1, sizeof(Key) * (count - (pos+1)));
            memmove(children + pos + 1, children + pos + 2, sizeof(NodeBase*) * (count - (pos + 2) +1));
            count--;
            return true;
         }
      }
      return false;
   }
   
   void insert(Key k, NodeBase* child) {
      assert(count < maxEntries - 1);
      unsigned pos = lowerBound(k);
      memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
      memmove(children + pos + 1, children + pos, sizeof(NodeBase*) * (count - pos + 1));
      keys[pos] = k;
      children[pos] = child;
      std::swap(children[pos], children[pos + 1]);
      count++;
   }
};


template <class Key, class Value>
struct BTree {
   std::atomic<NodeBase*> root;
   BTree() { root = new BTreeLeaf<Key, Value>(); }
   void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
      auto inner = new BTreeInner<Key>();
      inner->count = 1;
      inner->keys[0] = k;
      inner->children[0] = leftChild;
      inner->children[1] = rightChild;
      root = inner;
   }

   void yield(int count) {
      // if (count>3)
      //    sched_yield();
      // else
      (void)count;
      _mm_pause();
   }

   void insert(Key k, Value v) {
      int restartCount = 0;
   restart:
      if (restartCount++) yield(restartCount);
      bool needRestart = false;

      // Current node
      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type == PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);
    
         // Split eagerly if full
         if (inner->isFull()) {
            // Lock
            if (parent) {
               parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
               if (needRestart) goto restart;
            }
            node->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
               if (parent) parent->writeUnlock();
               goto restart;
            }
            if (!parent && (node != root)) {  // there's a new parent
               node->writeUnlock();
               goto restart;
            }
            // Split
            Key sep;
            BTreeInner<Key>* newInner = inner->split(sep);
            if (parent)
               parent->insert(sep, newInner);
            else
               makeRoot(sep, inner, newInner);
            // Unlock and restart
            node->writeUnlock();
            if (parent) parent->writeUnlock();
            goto restart;
         }

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }
    
      auto leaf = static_cast<BTreeLeaf<Key, Value>*>(node);

      // Split leaf if full
      if (leaf->count == leaf->maxEntries) {
         // Lock
         if (parent) {
            parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }
         node->upgradeToWriteLockOrRestart(versionNode, needRestart);
         if (needRestart) {
            if (parent) parent->writeUnlock();
            goto restart;
         }
         if (!parent && (node != root)) {  // there's a new parent
            node->writeUnlock();
            goto restart;
         }
         // Split
         Key sep;
         BTreeLeaf<Key, Value>* newLeaf = leaf->split(sep);
         if (parent)
            parent->insert(sep, newLeaf);
         else
            makeRoot(sep, leaf, newLeaf);
         // Unlock and restart
         node->writeUnlock();
         if (parent) parent->writeUnlock();
         goto restart;
      } else {
         // only lock leaf node
         node->upgradeToWriteLockOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) {
               node->writeUnlock();
               goto restart;
            }
         }
         leaf->insert(k, v);
         node->writeUnlock();
         return;  // success
      }
   }

   bool lookup(Key k, Value& result) {
      int restartCount = 0;
   restart:
      if (restartCount++) yield(restartCount);
      bool needRestart = false;

      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type == PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }

      BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(node);
      unsigned pos = leaf->lowerBound(k);
      bool success = false;
      if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
         success = true;
         result = leaf->payloads[pos];
      }
      if (parent) {
         parent->readUnlockOrRestart(versionParent, needRestart);
         if (needRestart) goto restart;
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      return success;
   }

   bool remove(Key k) {
      int restartCount = 0;
   restart:
      if (restartCount++) yield(restartCount);
      bool needRestart = false;

      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type == PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }

      BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(node);
      // check for underflow
      // and try to merge
      if (parent && leaf->isUnderflow()) {
         // TODO lock coupling
         parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
         if (needRestart) goto restart;
      
         node->upgradeToWriteLockOrRestart(versionNode, needRestart);
         if (needRestart) {
            if (parent) parent->writeUnlock();
            goto restart;
         }
         if (!parent && (node != root)) {  // there's a new parent
            node->writeUnlock();
            goto restart;
         }
         auto inner = static_cast<BTreeInner<Key>*>(parent);
         auto pos = inner->lowerBound(k);

         if ((inner->count >= 2) && ((pos + 1) < inner->count)) {
            BTreeLeaf<Key, Value>* right = static_cast<BTreeLeaf<Key, Value>*>(inner->children[pos + 1]);
            // XXX todo  lock right node here
            
            // check if right fits into current node
            if (leaf->count + right->count >= BTreeLeaf<Key,Value>::maxEntries) {
               bool success = leaf->remove(k);
               node->writeUnlock();
               parent->writeUnlock();
               return success;
            }

            leaf->setFences(leaf->fenceKeys.getLower(), right->fenceKeys.getUpper());
            // copy right node to current node and remove from parent
            memcpy(leaf->keys + leaf->count, right->keys, sizeof(Key) * right->count);
            memcpy(leaf->payloads + leaf->count, right->payloads, sizeof(Value) * right->count);
            // adjust count
            leaf->count += right->count;
            inner->remove(pos);
            // currently leakes right node ptr due to optimistic latching        
         }
         bool success = leaf->remove(k);
         node->writeUnlock();
         parent->writeUnlock();
         return success;
      }
      // -------------------------------------------------------------------------------------
      // remove key 
      node->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      if (parent) {
         parent->readUnlockOrRestart(versionParent, needRestart);
         if (needRestart) {
            node->writeUnlock();
            goto restart;
         }
      }
      bool success = leaf->remove(k);
      node->writeUnlock();
      return success;
   }

   // -------------------------------------------------------------------------------------
   // Scan Code
   // -------------------------------------------------------------------------------------

   // TODO
   // - proper deletion
   // -------------------------------------------------------------------------------------
   enum class RC : uint8_t { FINISHED = 0, CONTINUE = 2 };
   struct op_result {
      RC return_code = RC::FINISHED;
      Key next_sep;  // used when scan needs to continue
   };
   // -------------------------------------------------------------------------------------
   struct DESC_SCAN {
      bool isFirstTraversal;
      // -------------------------------------------------------------------------------------
      DESC_SCAN(bool isFirstTraversal) : isFirstTraversal(isFirstTraversal){};
      // -------------------------------------------------------------------------------------
      // inner traversal
      auto next_node(Key k, BTreeInner<Key>& inner) { return inner.children[inner.lowerBound(k)]; }

      // -------------------------------------------------------------------------------------
      template <class Fn>
      op_result operator()(Key k, BTreeLeaf<Key, Value>& leaf, Fn&& func) {
         if (leaf.count == 0) {
            if (leaf.fenceKeys.isLowerInfinity())  // scan done
               return {RC::FINISHED, 0};
            return {RC::CONTINUE, leaf.fenceKeys.getLower().key}; 
         }
         // -------------------------------------------------------------------------------------
         auto lower_bound = leaf.lowerBound(k);  // if fence key is deleted
         if (lower_bound >= leaf.count) lower_bound = leaf.count - 1;
         auto it = typename BTreeLeaf<Key, Value>::desc_iterator(leaf, lower_bound);  // returns first key not less than k
         auto end = leaf.rend();
         bool enter_scan = true;

         if (leaf.keys[*it] > k) {  // lower bound returned one larger element
            if (*it == 0) {
               enter_scan = false;  // goes to next leaf;
            } else {
               ++it;  // adjust by one to left
            }
         }
         while (enter_scan) {
            if (!func(leaf.keys[*it], leaf.payloads[*it])) return {RC::FINISHED, 0};
            if (it == end) {  // end is 0 meaning it is including the last which is unusual
               break;
            }
            ++it;
         }
         if (leaf.fenceKeys.isLowerInfinity())  // scan doen
            return {RC::FINISHED, leaf.fenceKeys.getLower().key};
         return {RC::CONTINUE, leaf.fenceKeys.getLower().key};
      }
   };
   // -------------------------------------------------------------------------------------
   struct ASC_SCAN {
      bool isFirstTraversal;
      // -------------------------------------------------------------------------------------
      ASC_SCAN(bool isFirstTraversal) : isFirstTraversal(isFirstTraversal){};
      // -------------------------------------------------------------------------------------
      // inner traversal
      auto next_node(Key k, BTreeInner<Key>& inner) {
         if (isFirstTraversal)
            return inner.children[inner.lowerBound(k)];
         else
            return inner.children[inner.upperBound(k)]; 
      }
      // -------------------------------------------------------------------------------------
      template <class Fn>
      op_result operator()(Key k, BTreeLeaf<Key, Value>& leaf, Fn&& func) {
         if (leaf.count == 0) {
            if (leaf.fenceKeys.isUpperInfinity())  // scan done
               return {RC::FINISHED, 0};
            return {RC::CONTINUE, leaf.fenceKeys.getUpper().key};  // XXX must be replaced with fence key
         }
         // -------------------------------------------------------------------------------------
         auto it = typename BTreeLeaf<Key, Value>::asc_iterator(leaf, leaf.lowerBound(k));
         auto end = leaf.end();
         // -------------------------------------------------------------------------------------
         while (it!=end) {
            if (!func(leaf.keys[*it], leaf.payloads[*it])) return {RC::FINISHED, 0};
            ++it;
         }
         if (leaf.fenceKeys.isUpperInfinity())  // scan done
            return {RC::FINISHED, leaf.fenceKeys.getUpper().key};
         return {RC::CONTINUE, leaf.fenceKeys.getUpper().key};
      }
   };
   // -------------------------------------------------------------------------------------
   template <class SCAN_DIRECTION, class Fn>
   void scan(Key k, Fn&& func) {
      bool isFirstTraversal = true;
      op_result res;
      res.next_sep = k;
      do {
         res = scan_(res.next_sep, func, SCAN_DIRECTION(isFirstTraversal));
         isFirstTraversal =
             false;  // important for asc scan; after the first traversal it needs to use upperbound in inner nodes for fence keys
      } while (res.return_code == RC::CONTINUE);
   }
   // -------------------------------------------------------------------------------------
   template <class Fn, class SCAN_DIRECTION>
   op_result scan_(Key k, Fn&& func, SCAN_DIRECTION scan_functor) {
      int restartCount = 0;
   restart:
      if (restartCount++) yield(restartCount);
      bool needRestart = false;

      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;
      // -------------------------------------------------------------------------------------
      // inner traversal
      while (node->type == PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }
         parent = inner;
         versionParent = versionNode;
         node = scan_functor.next_node(k, *inner);  // upper or lower bound
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }
      // -------------------------------------------------------------------------------------
      BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(node);
      auto op_code = scan_functor(k, *leaf, func);
      // debug
      // std::cout << "Lower fence key " << leaf->fenceKeys.getLower() << "\n";
      // std::cout << "Upper fence key " << leaf->fenceKeys.getUpper() << "\n";
      // -------------------------------------------------------------------------------------
      if (parent) {
         parent->readUnlockOrRestart(versionParent, needRestart);
         if (needRestart) goto restart;
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      return op_code;
   }
};
}  // namespace twosided
