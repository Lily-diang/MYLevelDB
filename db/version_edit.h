// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// 文件信息，维护了引用次数、允许的最大无效查询次数、文件序列号、文件大小（字节）、该文件中的最小键、该文件中的最大键
struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;           // 引用次数
  int allowed_seeks;  // 允许的最大无效查询次数，超过这个次数，该文件就要被compact
  uint64_t number;    // 文件序列号
  uint64_t file_size;    // 文件大小（字节）
  InternalKey smallest;  // 该文件中的最小键
  InternalKey largest;   // 该文件中的最大键
};

// 该结构用于存储生成新的version的中间结果，最终与Version N合并直接生成Version N+1
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  // 兼顾旧版本（已不再使用）
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  // 设置下一个文件序列号
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  // 设置写入序列号
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // 给一个层级增加文件，将参数中的信息复制到一个FileMetaData结构变量中，送入new_files_数组
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  // 给一个层级删除文件，将指定的文件序列号送入deleted_files_数组
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }
  // 编码
  void EncodeTo(std::string* dst) const;
  // 解码
  Status DecodeFrom(const Slice& src);
  
  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;  // 当前的key比较器名字comparator，db一旦创建，排序的逻辑就必须保持兼容，不可变更。此时就用comparator做凭证。
  uint64_t log_number_; //当前使用的log_number
  uint64_t prev_log_number_;
  uint64_t next_file_number_; // 其他文件编号
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;  // 每个level层的compact pointer
  DeletedFileSet deleted_files_;  // 要删除的SST
  std::vector<std::pair<int, FileMetaData>> new_files_; // 要添加的SST
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
