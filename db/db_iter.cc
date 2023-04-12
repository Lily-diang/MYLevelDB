// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.

// Leveldb数据库的MemTable和sstable文件的存储格式都是InternalKey(userkey, seq, type) => uservalue。
// DBIter把同一个userkey在DB中的多条记录合并为一条，综合考虑了userkey的序号、删除标记、和写覆盖等等因素。
// DBIter只会把userkey最新（seq最大的就是最新的，相同userkey的老记录（seq较小的）不会让上层看到）的一条记录展现给用户，
// 另外如果这条最新的记录是删除类型，则会跳过该记录，否则，遍历时会把已删除的key列举出来。
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;
  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);


  // 用于临时缓存user key到dst中
  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  // 清除saved_value_的内容
  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  // 选取在计划压缩之前可以读取的字节数。
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_; // 比较器
  Iterator* const iter_; //是一个MergingIterator
  SequenceNumber const sequence_; //DBIter只能访问到比sequence_小的kv对，这就方便了老版本（快照）数据库的遍历
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_; // ???
  size_t bytes_until_read_sampling_; // ???
};

// 解析key，从iter_中解析出key值，然后将该key值保存到ikey中
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    // iter_刚好在this->key()的所有entry之前，所以先跳转到this->key()  
    // 的entries范围之内，然后再做常规的skip
    if (!iter_->Valid()) { // 注意，leveldb 中所有迭代器失效位置都在最未尾的后面一个位置，当 iter 失效时设置为 First 位置，
      iter_->SeekToFirst();  // 这是因为上一次遍历的方向是 kReverse，即向 Prev 方向遍历直到 invalid，所以此处 SeekToFirst 是正确的
    } else {
      iter_->Next(); // 由于在此之前，调用了Prev()函数，saved_key_已经保存了当前entry的userkey，此时只需要将iter_向后移动一个位置
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }
  // 跳过 iter_key().user_key_ 更旧的版本和 deleteType 版本
  FindNextUserEntry(true, &saved_key_);
}

/**
 * @brief 循环跳过下一个delete的记录，直到遇到kValueType的记录。如果遇到了下一个有效的记录，则iter_就保存了其对应的迭代器，且valid_为真，否则valid_为假
 * @param {bool} skipping 
 * @param {string*} skip
 * @return 
 *
 *该函数耦合了两个功能: 
 *  1. 遍历过程中遇到了某个 user_key 的最新版本是 deletion, 则后面跳过此 user_key 的所有节点。此过程是递归的，即若存在连续这样的 user_key 则全部会被跳过。 -- (skipping 为 true|false 都有此功能)
 *  2. 跳过 user_key 等于函数参数指定的 skip 的旧版本 -- skipping 为 true 时有此功能
 * 
 *  总的来说，skipping 为 true 指示了是否需要向后跳过 skip 指示的 user_key 及已被 delete 的节点。 为 false 指示了只向后跳过最新版本为 delete 的节点直到遇到有效的节点。
 * 
 *  Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序所以同 user_key 较新的记录在前面被遍历出, 如果先遇到了一个 deleteType 的 InternalKey, 则后面同 user_key 的InternalKey 要被删除.
 * 
 *  传入的参数 skip 可能是 Internalkey, 但此时 skipping 是 false，不影响逻辑(见本文件中 Seek 函数中的调用).个人相信这是原作者的笔误.
 * 
 *  最后注意，如果我们设置 skipping 为 true 且指定 skip 是一个很大的 user_key，则由于两个功能会同时生效，若存在某个 user_key 的最新版本是 deletion 则 skip 会被覆盖
*/
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) { // 从iter_中解析出一个内部键放入ikey，且必须保证ikey的序列号小于或等于当前序列号
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          // 如果遇到iter userkey的删除操作，则说明后面该userkey都是无效的，因此需要跳过
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            // 如果 skipping=false 或遍历到的 user_key 大于 skip, 表示已经找到了 NextUserEntry ，直接返回
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

/* 
        与 Next 相对, 功能是找到仅仅比当前 user_key 小的有效的 user_key` 节点
        执行完 Prev 后迭代器的指针指示的位置是当前 saved_key 节点的上一个节点. 这一点不同于 Next.
        造成这个不同的关键原因是:
            Internalkey 排序规则是 Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
        那么相同 user_key 的更新的版本会排在前面(Prev 方向), 所以要找到当前 user_key 的前面一个有效 user_key`, 必须要
        遍历把 user_key` 的所有节点都遍历完, 才会得知这个 user_key` 是否没有被删除, 或是否有更新版本的值. 
*/
void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();// 如果方向为KForward的话，那么current_不一定是该user key的最新版本，所以要一直向前遍历
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      // 然后找到第一个与该user key不同的entry，但此处是上一个user key的最旧版本，因此要调用FindPrevUserEntry函数跳过无效entry
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}
/* 
    本函数耦合两个功能:
    1. 向前移动当前迭代器的位置, 使其指示到更小的一个 user_key 的节点.(此节点可能是 deletion)
    2. saved_key_ 需要指示出之前一个有效的 user_key`. (暗示着, 如果之前一个节点是 deletion, 则需要继续往前遍历).
        此过程是递归的, 如果往前移动的过程中, 遇到的 user_key 的最新版本总是 deletion 则始终需要往前移动.

    设当前迭代器指示着 user_key` 的某个节点, 往前遍历遇到它的最新版本, 但是类型是 deletion, 则遍历到的所有 user_key` 节点
    都不再是有效的, 所以需要继续往前遍历. 这也是函数里面这段判断代码的意义: 
        if ((value_type != kTypeDeletion) &&    //上一节点的类型不能是 deletion
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0)    //遇到了更小的 user_key
    举例说明:
    设 user_key: B < A, 且有如下操作流程: A.add    -->    A.add    -->    A.delete    -->    B.add --> B.delete, 则迭代器依次遍历的顺序是:(简单地以链表形式去表示)
    (提示: Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序):

    1(B.delete)    -->    2(B.add)    -->    3(A.delete)    -->    4(A.add)    -->    5(A.add)

    若当前迭代器位置指向 5, 则 FindPrevUserEntry 依次遍历的节点是 4, 3, 2, 1
    遍历到 2 时, 由于之前节点的类型是 deletion 则不应该 break, 继续遍历到 1, 再因为 !valid() 返回. 

    以下细节需要注意:
        1. 正常情况下函数结束时迭代器指向的位置的后一个节点不可能是 deletion, 当且仅当整个迭代器遍历到了尽头.
        2. 函数结束时迭代器所指位置可能是一个 deletion节点(这种情况下,形成这个局面的原因是做了空的 delete).举例如下：
            0(A.add)    -->    1(A.delete)    -->    2(B.add)
            当前迭代器指向 2 时, 往前遍历, 则到 1 节点时会 break. 此时迭代器会指在 1，且 saved_key_ 会保存着 2 节点的值.
            仔细观察上面的数据, 操作序列是: 增加B, 删除A, 增加A. 实际上，不管之前的数据(user_key) 有没有 A，“删除A”这个操作
            是无意义的.

*/
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
            // 我们遇到了前一个key的一个未被删除的entry，跳出循环  
            // 此时Key()将返回saved_key，saved key非空；
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        // 根据类型，如果是Deletion则清空saved key和saved value  
        // 否则，把iter_的user key和value赋给saved key和saved value
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }
  // 注意上面的 break 时，value_type!=kTypeDeletion，此时 value_type 并未被重新赋值
  // 所以此分支表示整个迭代器遍历完了，但是首个节点的 value_type 是 kTypeDeletion，所以置迭代器状态为 invalid
  if (value_type == kTypeDeletion) {// 表明遍历结束了，将direction设置为kForward  
    // End，并没有找到有效的Prev()
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

/**
 * @brief 查找target对应的位置，且要跳过删除了的entry 
 * @param {Slice&} target 要查找的目标键
 * @return {*}
 */
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  // 清空saved value和saved key，并根据target设置saved key
  ClearSavedValue();
  saved_key_.clear();
  // kValueTypeForSeek(1) > kDeleteType(0)，使用append是因为save_key_为空
  // 首先要把target封装成一个InterKey（InterKey中的序列号为当前序列号，ValueType为kValueTypeForSeek），然后在存入saved_key_
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
   // 可以定位到合法的iter，还需要跳过Delete的entry 
  if (iter_->Valid()) {// 因为不知道这个iter_是否被删除，如果被删除了需要跳过
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) { // 也要跳过被删除的entry
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
