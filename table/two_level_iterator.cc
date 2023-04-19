// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include "leveldb/RemixHelper.h"

namespace leveldb {

namespace {

// 回调函数
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  //构造。对于SSTable来说：
  //1、index_iter是指向index block的迭代器；
  //2、block_function是Table::BlockReader,即读取一个block;
  //3、arg是指向一个SSTable;
  //4、options 读选项。
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;
  // 以下三个函数都是针对一级迭代器的函数
  // 这里就是seek到index block对应元素位置
  void Seek(const Slice& target) override;
  int SeekToFirst() override;
  void SeekToLast() override;
  // 以下函数都是针对二级迭代器的函数
  // DataBlock中的下一个Entry
  int Next() override;
  // DataBlock中的前一个Entry
  void Prev() override;
  //指向DataBlock的迭代器是否有效
  bool Valid() const override { return data_iter_.Valid(); }
  //DataBlock中的一个Entry的Key
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  //DataBlock中的一个Entry的Value
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  // DataBlock中的一个Entry的Value
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  //保存错误状态，如果最近一次状态是非ok状态，
  //则不保存
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  //跳过当前空的DataBlock,转到下一个DataBlock
  void SkipEmptyDataBlocksForward();
  //跳过当前空的DataBlock,转到前一个DataBlock
  void SkipEmptyDataBlocksBackward();
  //设置二级迭代器data_iter
  void SetDataIterator(Iterator* data_iter);
  //初始化DataBlock的二级迭代器
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;  //一级迭代器，对于SSTable来说就是指向index block
  IteratorWrapper data_iter_;  // May be nullptr，二级迭代器，对于SSTable来说就是指向DataBlock
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  //对于SSTable来说,保存index block中的offset+size。
  std::string data_block_handle_;
  int run_index; // !!!
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

//1、seek到target对应的一级迭代器位置;
//2、初始化二级迭代器;
//3、跳过当前空的DataBlock。
void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

int TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
  return 0;
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

int TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
  return 0;
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}
//针对二级迭代器。
//如果当前二级迭代器指向为空或者非法;
//那就向后跳到下一个非空的DataBlock。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

//初始化二级迭代器指向。
//对SSTable来说就是获取DataBlock的迭代器赋值给二级迭代器。
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    // index 指向的block的handle
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size()); // assign先将原字符串清空，然后赋予新的值作替换。
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
