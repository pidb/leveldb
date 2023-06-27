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

// AddRecord 接收 WriterBatchInternal 的 contents (slice), 
// 将 key/value 编码后写入到日志.
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  // begine 用于记录 record 是否可以一次在一个 block (32kb) 中写入完成, 如果
  // 不能, 在下一次循环中 begin 将置为 false.
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
	// 当前的 block 不能写入一个完整的 log header (7bytes),
	// 则将当前 block 填充 \x00..., 然后再下一个 block 写入
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
	  // 填充后, 重置下一个 block 的偏移
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

	// avail 是当前 block 剩余可以写入的空间
	// 每个 block 固定的大小 - 当前 block 剩余的空间 - 固定的 block header 大小
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
	// left 是 batch 编码的内容的大小, fragment_length 若可以完整的写入 left, 则
	// 取 left, 否则取 avail block 实际剩余的空间
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
	// end 为 true 表示 left 可以完整的写入到当前 block, 
	// false 则相反.
    const bool end = (left == fragment_length);
	// 当前 block 一次性写完 type = kFullType, 否则
	// 1. 如果 begin = true, 说明 block 第一次写入
	// 2. 如果当前 block 写了一部分数据, 下一个 block 可以写完, type = kLastType
	// 3. 如果跨越多个 block 才能写完, 中间的 block  type = kMiddleType
    if (begin && end) {
      type = kFullType; // 一次完整的写入
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

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
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
