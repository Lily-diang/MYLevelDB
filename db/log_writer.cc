// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

/**
 * @brief 写入一条日志记录，首先会进行一个块检查，如果当前块的容量不够，他就会开启一个新块写入这个数据。
 * @param {Slice&} slice
 * @return {*}
 */
Status Writer::AddRecord(const Slice& slice) {
  // 指向要写入的文件content
  const char* ptr = slice.data();
  // 代表需要写入的记录内容的长度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  // begin为true代表该条记录是第一次被写入，即如果一个记录跨越多个块，只有写入第一个块时
  // begin为true，通过该值可以设定header中的type字段是否为kFirstType
  bool begin = true;
  do {
    //块容量检查，每个块的前七位都是 0x00 
    // kBlockSize表示块大小，block_offset_表示写入偏移量
    // leftover表示当前快还剩多少字节可用
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) { // 剩余字节小于头部，则换下一个块存储，tailer补零
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 如果leftover等于0，这说明正好满一个块，则将block_offset_置零，表示从下一个块开始存日志
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    // 计算块的剩余空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 当前快能写入的大小取决于剩余空间和记录剩余内容的最小值
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 该记录是否已经完整写入
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType; // 一条记录完整写到一个块
    } else if (begin) {
      type = kFirstType; // 跨块存储的第一部分
    } else if (end) {
      type = kLastType; // 最后一部分
    } else {
      type = kMiddleType;// 中间部分
    }
    // 按照格式将数据写入并刷新到磁盘文件，然后更新block_offset_字段长度
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  //  添加CRC等校验信息
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  // 然后更新block_offset_字段长度
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
