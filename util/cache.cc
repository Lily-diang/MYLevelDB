/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:05
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-05 16:10:38
 * @FilePath: \leveldb\util\cache.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// 定义每个哈希表的节点
// LevelDB中的cache以LRUHandle为单位插入cache中，一个LRUHandle称为一条Entry。
struct LRUHandle {
  void* value;  // 节点中保存的值
  void (*deleter)(const Slice&, void* value);  // 节点的销毁方法
  LRUHandle* next_hash; // 如果产生哈希冲突，则用一个单向链表串联冲突的节点
  LRUHandle* next; // LRU双向链表得的后一个节点
  LRUHandle* prev; // LRU双向链表中的前一个节点
  size_t charge;  // TODO(opt): Only allow uint32_t? 当前节点占用的容量，LevelDB中每个容量为1
  size_t key_length;
  bool in_cache;     // Whether entry is in the cache.如果不在，则可调用deldter进行销毁
  uint32_t refs;     // References, including cache reference, if present.
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons  该节点键的哈希值，该值缓存此处可避免每次都进行哈希运算，提高效率
  char key_data[1];  // Beginning of key 该节点键的占位符，键的实际长度保存在key_length中，有什么用吗？？？

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// 定义了一个哈希表，成员变量有哈希表桶个数、元素个数、桶的首址
// LevelDB实现了自己的哈希表，相较于内置的哈希表，在随机读取测试中自定义的哈希表速度可以快5%。
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        // 由于每个缓存条目都相当大，因此我们的目标是一个小
        // 平均链表长度 （<= 1）。
        Resize();
      }
    }
    return old;
  }
  /**
   * @brief 从哈希表中删除节点 ，直接从hash冲突链表中删除该节点，并返回该节点的指针
   * @param {Slice&} key 节点对应的key
   * @param {uint32_t} hash 哈希值，为了快速定位
   * @return {*}
   */
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 返回指向与键/哈希匹配的缓存条目的槽的指针。 如果
  // 没有这样的缓存条目，返回指向相应链表中尾随槽的指针。
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
// LevelDB中的整个缓存就是由若干个LRUCache组成的
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));  // 再哈希表中插入节点
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);  // 在哈希表中查找结点
  void Release(Cache::Handle* handle);    // 在哈希表中释放一个节点的引用
  void Erase(const Slice& key, uint32_t hash);    // 在哈希表中溢出一个节点
  void Prune();       // 将lru_双向链表中的节点全部移除
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);    // 将一个节点从双向链表中移除
  void LRU_Append(LRUHandle* list, LRUHandle* e);  // 将一个节点插入双向链表
  void Ref(LRUHandle* e);   //对一个节点引用计数加1
  void Unref(LRUHandle* e); // 对一个节点引用-1
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 移除一个节点

  // Initialized before use.
  // capacity_是cache的大小，对于block cache，单位为bytes，对于table cache，单位为个数。
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // lru.prev 指向最新节点，lru.next指向最旧节点，是双向链表结构,lru_中是参与淘汰的节点
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // 是双向链表结构，in_use_中是被访问的节点，不能被淘汰
  LRUHandle in_use_ GUARDED_BY(mutex_);
  // 实际使用的哈希表，LRU Cache对哈希表的操作都会通过底层的tabel_变量来操作，用于快速查找
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  // 先判断是否在lru_链表中，如果在则将其删除并移动到in_use_链表，然后引用计数加1
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  // 引用计数先-1
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// remove函数不用管在哪个链表，直接修改前后继指着即可
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge; 
    // 由于LRUCache已经插入了一个新节点e了，要删除旧节点，这里的table_.Insert(e)返回值代表旧节点（可能为空可能不为空）
    // 注意，此处hash表插入新的LRUHandle节点e就要用该语句！！！
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    // 不要缓存。（支持 capacity_==0 并关闭缓存。
    // next 由断言中的 key（） 读取，因此必须对其进行初始化
    e->next = nullptr;
  }
  // 当所占空间超过最大容量，从链表表头开始移除元素
  while (usage_ > capacity_ && lru_.next != &lru_) {  // 淘汰
    LRUHandle* old = lru_.next;  // 移除最旧的Entry
    assert(old->refs == 1);
    // 注意，这里分为两步份，先从hash表中移除，再从缓存（也即两个双向链表）中移除；；因为leveldb的cache主要由哈希表和两个链表组成
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
// 如果 e ！= nullptr，则完成从缓存中删除 *e;它已经
// 从哈希表中删除。 返回是否 e ！= nullptr。
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);  
    LRU_Remove(e);// 从list中删除
    e->in_cache = false; 
    usage_ -= e->charge; // 修改当前使用容量大小
    Unref(e);  // 引用计数-1
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
// LRU Cache共分为相同的16段，为啥？？？
static const int kNumShards = 1 << kNumShardBits;



// 以上都是对于单一LRUCache的实现，而LevelDB为了优化Cache锁，
// 提升访问效率，实现了sharded LRUCache，由多个LRUCache组成，默认为16个LRUCache。
class ShardedLRUCache : public Cache {
 private:
 // shared_数组共有16个LRUCache元素
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  // 查找时首先对进行查找的键进行HashSlice操作，并返回一个哈希值
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // 然后将HashSlice返回的哈希值传入Shard函数，该函数返回一个小于16的数，即为该次操作需要使用的段
  // 将hash值左移28位，取最高四位
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  // 以下所有函数都是通过调用每一个LRUCache的成员函数实现
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);   // 计算hash值
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);  // 查找该哈希值属于哪一个分片，到相应的分片中调用Insert函数
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace
//创建具有固定大小容量的新缓存。Cache的这种实现使用了LRU策略。
Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
