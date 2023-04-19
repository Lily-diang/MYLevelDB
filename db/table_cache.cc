/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:02
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-17 21:33:44
 * @FilePath: \leveldb\db\table_cache.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-04 22:59:17
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-17 17:39:52
 * @FilePath: \MYLevelDB\db\table_cache.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// 该结构保存一个RandomAccessFile实例和Table实例
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));  // 使用file_number构造键
  *handle = cache_->Lookup(key);  // 在缓存中查找key
  if (*handle == nullptr) {  // 若没找到，则需要从磁盘中打开一个SSTable文件
  // fname表示要打开的SSTable文件的文件名称
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;  // 随机读文件，用于读取SSTable文件
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) { // 这里为什么还要再调用一次，而且，old_fname与上文的fname不一样吗？
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // 打开SSTable文件并且生成了一个Table示例保存到table变量中
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      // 打开失败，进行错误处理
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 以文件序号作为键，TableAndFile实例作为值，插入缓存
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

/**
 * @brief 针对某一Level中的某一个SST，生成一个SSTable迭代器 
 * @return {*} 返回的实际上是Table类的迭代器
 */
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {   // 这一步有必要吗？？？
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  // 在table cache中查找file_name对应的table，然后将handle指向它
  Status s = FindTable(file_number, file_size, &handle);  // 这里的handle指的是LRUHandle，
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  // 查找SSTable就需要调用table类的NewIterator来构造迭代器
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) { 
    *tableptr = table;
  }
  return result;
}


Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
