// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/common/rowblock_memory.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/overwrite.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ColumnBlockCell;
class SelectionVector;

// A block of data all belonging to a single column.
// This is simply a view into a buffer - it does not have any associated
// storage in and of itself. It does, however, maintain its type
// information, which can be used for extra type safety in debug mode.
class ColumnBlock {
 public:
  typedef ColumnBlockCell Cell;

  ColumnBlock(const TypeInfo* type,
              uint8_t* non_null_bitmap,
              void* data,
              size_t nrows,
              RowBlockMemory* memory)
      : type_(type),
        non_null_bitmap_(non_null_bitmap),
        data_(reinterpret_cast<uint8_t*>(data)),
        nrows_(nrows),
        memory_(memory) {
    DCHECK(data_) << "null data";
  }

  void SetCellIsNull(size_t idx, bool is_null) {
    DCHECK(is_nullable());
    BitmapChange(non_null_bitmap_, idx, !is_null);
  }

  void SetCellValue(size_t idx, const void* new_val) {
    strings::memcpy_inlined(mutable_cell_ptr(idx), new_val, type_->size());
  }

#ifndef NDEBUG
  void OverwriteWithPattern(size_t idx, StringPiece pattern) {
    char* col_data = reinterpret_cast<char*>(mutable_cell_ptr(idx));
    kudu::OverwriteWithPattern(col_data, type_->size(), pattern);
  }
#endif

  // Return a pointer to the given cell.
  const uint8_t* cell_ptr(size_t idx) const {
    DCHECK_LT(idx, nrows_);
    return data_ + type_->size() * idx;
  }

  // Returns a pointer to the given cell or NULL.
  const uint8_t* nullable_cell_ptr(size_t idx) const {
    return is_null(idx) ? nullptr : cell_ptr(idx);
  }

  Cell cell(size_t idx) const;

  // Return the bitmap indicating whether each cell is non-NULL.
  // A set bit indicates non-NULL.
  //
  // This returns nullptr for a non-nullable column.
  const uint8_t* non_null_bitmap() const {
    return non_null_bitmap_;
  }

  uint8_t* mutable_non_null_bitmap() {
    return non_null_bitmap_;
  }

  bool is_nullable() const {
    return non_null_bitmap_ != nullptr;
  }

  bool is_null(size_t idx) const {
    DCHECK(is_nullable());
    DCHECK_LT(idx, nrows_);
    return !BitmapTest(non_null_bitmap_, idx);
  }

  size_t stride() const { return type_->size(); }
  const uint8_t* data() const { return data_; }
  uint8_t* data() { return data_; }
  size_t nrows() const { return nrows_; }

  RowBlockMemory* memory() { return memory_; }
  Arena* arena() { return &memory_->arena; }

  const TypeInfo* type_info() const {
    return type_;
  }

  std::string ToString() const {
    std::string s;
    for (int i = 0; i < nrows(); i++) {
      if (i > 0) {
        s.append(" ");
      }
      if (is_nullable() && is_null(i)) {
        s.append("NULL");
      } else {
        type_->AppendDebugStringForValue(cell_ptr(i), &s);
      }
    }
    return s;
  }

  // Copies a range of cells between two ColumnBlocks.
  //
  // The extent of the range is designated by 'src_cell_off' and 'num_cells'. It
  // is copied to 'dst' at 'dst_cell_off'.
  //
  // Note: The inclusion of 'sel_vec' in this function is an admission that
  // ColumnBlocks are always used via RowBlocks, and a requirement for safe
  // handling of types with indirect data (i.e. deselected cells are not
  // relocated because doing so would be unsafe).
  //
  // TODO(adar): for columns with indirect data, existing arena allocations
  // belonging to cells in 'dst' that are overwritten will NOT be deallocated.
  Status CopyTo(const SelectionVector& sel_vec,
                ColumnBlock* dst,
                size_t src_cell_off,
                size_t dst_cell_off,
                size_t num_cells) const;

 protected:
  friend class ColumnBlockCell;
  friend class ColumnDataView;
  friend class RowBlockRow;

  // Return a pointer to the given cell.
  uint8_t* mutable_cell_ptr(size_t idx) {
    DCHECK_LT(idx, nrows_);
    return data_ + type_->size() * idx;
  }

  const TypeInfo* const type_;
  uint8_t* non_null_bitmap_;

  uint8_t* data_;
  const size_t nrows_;

  RowBlockMemory* memory_;
};

inline bool operator==(const ColumnBlock& a, const ColumnBlock& b) {
  // 1. Same number of rows.
  if (a.nrows() != b.nrows()) {
    return false;
  }

  // 2. Same nullability.
  if (a.is_nullable() != b.is_nullable()) {
    return false;
  }

  // 3. If nullable, same null bitmap contents.
  if (a.is_nullable() &&
      !BitmapEquals(a.non_null_bitmap(), b.non_null_bitmap(), a.nrows())) {
    return false;
  }

  // 4. Same data. We can't just compare the raw data because some entries may
  //    be pointers to the actual data elsewhere.
  for (int i = 0; i < a.nrows(); i++) {
    if (a.is_nullable() && a.is_null(i)) {
      continue;
    }
    if (a.type_info()->Compare(a.cell_ptr(i), b.cell_ptr(i)) != 0) {
      return false;
    }
  }

  return true;
}

inline bool operator!=(const ColumnBlock& a, const ColumnBlock& b) {
  return !(a == b);
}

// One of the cells in a ColumnBlock.
class ColumnBlockCell {
 public:
  ColumnBlockCell(const ColumnBlock& block, size_t row_idx)
      : block_(block), row_idx_(row_idx) {}

  const TypeInfo* typeinfo() const { return block_.type_info(); }
  size_t size() const { return block_.type_info()->size(); }
  const void* ptr() const {
    return is_nullable() ? block_.nullable_cell_ptr(row_idx_)
      : block_.cell_ptr(row_idx_);
  }
  void* mutable_ptr() { return block_.mutable_cell_ptr(row_idx_); }
  bool is_nullable() const { return block_.is_nullable(); }
  bool is_null() const { return block_.is_null(row_idx_); }
  void set_null(bool is_null) { block_.SetCellIsNull(row_idx_, is_null); }
 protected:
  ColumnBlock block_;
  const size_t row_idx_;
};

inline ColumnBlockCell ColumnBlock::cell(size_t idx) const {
  return ColumnBlockCell(*this, idx);
}

// Wrap the ColumnBlock to expose a directly raw block at the specified offset.
// Used by the reader and block encoders to read/write raw data.
class ColumnDataView {
 public:
  explicit ColumnDataView(ColumnBlock* column_block, size_t first_row_idx = 0)
      : column_block_(column_block),
        row_offset_(0) {
    Advance(first_row_idx);
  }

  void Advance(size_t skip) {
    // Check <= here, not <, since you can skip to
    // the very end of the data (leaving an empty block)
    DCHECK_LE(skip, column_block_->nrows());
    row_offset_ += skip;
  }

  size_t first_row_index() const {
    return row_offset_;
  }

  // Set 'nrows' bits of the non-null bitmap to 'value';
  // true if not null, false if null.
  void SetNonNullBits(size_t nrows, bool value) {
    BitmapChangeBits(column_block_->mutable_non_null_bitmap(), row_offset_, nrows, value);
  }

  uint8_t* data() {
    return column_block_->mutable_cell_ptr(row_offset_);
  }

  const uint8_t* data() const {
    return column_block_->cell_ptr(row_offset_);
  }

  RowBlockMemory* memory() { return column_block_->memory(); }

  Arena* arena() { return &memory()->arena; }

  size_t nrows() const {
    return column_block_->nrows() - row_offset_;
  }

  size_t stride() const {
    return column_block_->stride();
  }

  const TypeInfo* type_info() const {
    return column_block_->type_info();
  }

 private:
  ColumnBlock* column_block_;
  size_t row_offset_;
};

} // namespace kudu
