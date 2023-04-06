/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:03
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-06 19:48:13
 * @FilePath: \leveldb\table\two_level_iterator.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

struct ReadOptions;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
// 返回一个新的两级迭代器。两级迭代器包含一个
// 索引迭代器，其值指向一系列块，其中
// 每个块本身就是一个键，值对的序列。返回的
// 两级迭代器产生所有键/值对的串联
// 在块的序列中。拥有“index_iter”和
// 不再需要时将删除它。
//
// 使用提供的函数将index_iter值转换为
// 对相应块的内容的迭代器。
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg, const ReadOptions& options,
                                const Slice& index_value),
    void* arg, const ReadOptions& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
