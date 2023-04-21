/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:03
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-21 10:41:39
 * @FilePath: \leveldb\table\iterator.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"
#include "leveldb/RemixHelper.h"
#include "iostream"
#include "leveldb/Remix.h"
using namespace std;
namespace leveldb {

int Iterator::get_index_of_runs(){  // !!!
  return index_of_runs;
}

void Iterator::set_index_of_runs(int index){ // !!!
  assert(index >= 0);
  index_of_runs = index;
}

int Iterator::get_runs_num(){  // !!!
  return runs_num;
}
void Iterator::set_runs_num(int num){   // !!!
    runs_num = num;
}
const Comparator * Iterator::get_comparator(){ // ###
    return cmp_;
  }
void Iterator::set_comparator(const Comparator * cmp){ // ###
    cmp_ = cmp;
  }

Slice Iterator::KEY(){return Slice();}

void Iterator::Next(Remix* my_sorted_view,size_t &index_anchor_key, size_t &segment_index){};
  

Iterator::Iterator() {
  cleanup_head_.function = nullptr;
  cleanup_head_.next = nullptr;
  set_index_of_runs(0);
}

Iterator::~Iterator() {
  if (!cleanup_head_.IsEmpty()) {
    cleanup_head_.Run();
    for (CleanupNode* node = cleanup_head_.next; node != nullptr;) {
      node->Run();
      CleanupNode* next_node = node->next;
      delete node;
      node = next_node;
    }
  }
}

/**
 * @brief 这个函数是 Iterator 清理自身资源用的。
 * @param {CleanupFunction} func 函数
 * @param {void*} arg1 参数1
 * @param {void*} arg2 参数2
 * @return {*}
 */
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  CleanupNode* node;
  if (cleanup_head_.IsEmpty()) {
    node = &cleanup_head_;
  } else {
    node = new CleanupNode();
    node->next = cleanup_head_.next;
    cleanup_head_.next = node;
  }
  node->function = func;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

namespace {

class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) {}
  ~EmptyIterator() override = default;

  bool Valid() const override { return false; }
  void Seek(const Slice& target) override {}
  int SeekToFirst() override { return 0;}
  void SeekToLast() override {}
  int Next() override { assert(false); return 0;}
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  Status status() const override { return status_; }
  
 private:
  Status status_;
};

}  // anonymous namespace

Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK()); }

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

}  // namespace leveldb
