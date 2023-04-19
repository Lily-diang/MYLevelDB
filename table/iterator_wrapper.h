/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:03
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-18 16:33:04
 * @FilePath: \leveldb\table\iterator_wrapper.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include <iostream>
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
// 一个内部包装类，其接口类似于迭代器，它
// 缓存底层迭代器的 valid（） 和 key（） 结果。
// 这有助于避免虚拟函数调用，并提供更好的
// 缓存位置。
// 无论是TwoLevelIterator还是MergingIterator，在使用时都反复需要获取其中iterator是否为valid或获取其value。比如在MergingIterator获取下一个key时，其需要比较所有iterator的key，但最终只会修改一个iterator的位置。
//
// 为了减少这一开销，LevelDB在TwoLevelIterator和MergingIterator中，通过IteratorWrapper对其组合的iterator进行了封装。IteratorWrapper会缓存iterator当前位置的valid状态和key，只有在iterator的位置改变时才会更新。这样，当访问TwoLevelIterator和MergingIterator时，不需要每次都访问到最下层的iterator，只需要访问缓存状态即可。
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
  }

  // Iterator interface methods
  bool Valid() const { return valid_; }
  Slice key() const {
    assert(Valid());
    return key_;
  }
  Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const {
    assert(iter_);
    return iter_->status();
  }
  int Next() {
    assert(iter_);
    iter_->Next();
    Update();
    return 0;
  }
  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  int SeekToFirst() { // !!!
    assert(iter_);
    set_run_index(iter_->SeekToFirst());
    Update();
    return get_run_index();
  }
  void SeekToLast() {  // !!!
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }

  inline void set_run_index (int index) { // !!!
    assert(index >= 0);
    run_index = index;
  }

  inline int get_run_index(){ // !!!
    return run_index;
  }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }


  Iterator* iter_;
  bool valid_;
  Slice key_;
  int run_index; // !!!
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
