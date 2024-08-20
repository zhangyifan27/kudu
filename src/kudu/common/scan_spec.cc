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

#include "kudu/common/scan_spec.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/key_util.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::any_of;
using std::max;
using std::pair;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

void ScanSpec::AddPredicate(ColumnPredicate pred) {
  ColumnPredicate* predicate = FindOrNull(predicates_, pred.column().name());
  if (predicate != nullptr) {
    predicate->Merge(pred);
  } else {
    string column_name = pred.column().name();
    predicates_.emplace(std::move(column_name), std::move(pred));
  }
}

void ScanSpec::RemovePredicate(const string& column_name) {
  predicates_.erase(column_name);
}

void ScanSpec::RemovePredicates() {
  predicates_.clear();
}

bool ScanSpec::CanShortCircuit() const {
  if (has_limit() && *limit_ <= 0) {
    return true;
  }

  if (lower_bound_key_ && exclusive_upper_bound_key_ &&
      lower_bound_key_->encoded_key() >= exclusive_upper_bound_key_->encoded_key()) {
    return true;
  }

  return any_of(predicates_.begin(), predicates_.end(),
                [] (const pair<string, ColumnPredicate>& predicate) {
                  return predicate.second.predicate_type() == PredicateType::None ||
                      (predicate.second.predicate_type() == PredicateType::InList &&
                       predicate.second.raw_values().empty());
                });
}

bool ScanSpec::ContainsBloomFilterPredicate() const {
  return any_of(predicates_.begin(), predicates_.end(),
                [] (const pair<string, ColumnPredicate>& col_pred_pair) {
                  return col_pred_pair.second.predicate_type() == PredicateType::InBloomFilter;
                });
}

void ScanSpec::SetLowerBoundKey(const EncodedKey* key) {
  if (lower_bound_key_ == nullptr ||
      lower_bound_key_->encoded_key() < key->encoded_key()) {
    lower_bound_key_ = key;
  }
}

void ScanSpec::SetExclusiveUpperBoundKey(const EncodedKey* key) {
  if (exclusive_upper_bound_key_ == nullptr ||
      key->encoded_key() < exclusive_upper_bound_key_->encoded_key()) {
    exclusive_upper_bound_key_ = key;
  }
}

void ScanSpec::SetLowerBoundPartitionKey(const PartitionKey& partition_key) {
  if (lower_bound_partition_key_ < partition_key) {
    lower_bound_partition_key_ = partition_key;
  }
}

void ScanSpec::SetExclusiveUpperBoundPartitionKey(const PartitionKey& partition_key) {
  if (exclusive_upper_bound_partition_key_.empty() ||
      (!partition_key.empty() &&
       partition_key < exclusive_upper_bound_partition_key_)) {
    exclusive_upper_bound_partition_key_ = partition_key;
  }
}

string ScanSpec::ToString(const Schema& schema) const {
  vector<string> preds;

  if (lower_bound_key_ || exclusive_upper_bound_key_) {
    preds.push_back(EncodedKey::RangeToStringWithSchema(lower_bound_key_,
                                                        exclusive_upper_bound_key_,
                                                        schema));
  }

  // List predicates in stable order.
  for (size_t idx = 0; idx < schema.num_columns(); ++idx) {
    const ColumnPredicate* predicate = FindOrNull(predicates_, schema.column(idx).name());
    if (predicate != nullptr) {
      preds.push_back(predicate->ToString());
    }
  }

  const string limit = has_limit() ? Substitute(" LIMIT $0", *limit_) : "";
  return JoinStrings(preds, " AND ") + limit;
}

void ScanSpec::UnifyPrimaryKeyBoundsAndColumnPredicates(const Schema& schema,
                                                        Arena* arena,
                                                        bool remove_pushed_predicates) {
  LiftPrimaryKeyBounds(schema, arena);
  PushPredicatesIntoPrimaryKeyBounds(schema, arena, remove_pushed_predicates);
}

void ScanSpec::OptimizeScan(const Schema& schema, Arena* arena, bool remove_pushed_predicates) {
  UnifyPrimaryKeyBoundsAndColumnPredicates(schema, arena, remove_pushed_predicates);

  // KUDU-1652: Filter IS NOT NULL predicates for non-nullable columns.
  for (auto itr = predicates_.begin(); itr != predicates_.end(); ) {
    if (!itr->second.column().is_nullable() &&
        itr->second.predicate_type() == PredicateType::IsNotNull) {
      itr = predicates_.erase(itr);
    } else {
      itr = std::next(itr);
    }
  }
}

void ScanSpec::PruneInlistValuesIfPossible(const Schema& schema,
                                           const Partition& partition,
                                           const PartitionSchema& partition_schema) {
  for (auto& [column_name, predicate] : predicates_) {
    if (predicate.predicate_type() != PredicateType::InList) {
      continue;
    }

    int32_t idx;
    if (auto s = schema.FindColumn(column_name, &idx); !s.ok()) {
      LOG(DFATAL) << s.ToString();
      continue;
    }
    if (!schema.is_key_column(idx)) {
      continue;
    }

    auto* predicate_values = predicate.mutable_raw_values();
    predicate_values->erase(std::remove_if(
        predicate_values->begin(),
        predicate_values->end(),
        [idx, &schema, &partition, &partition_schema](const void* value) {
          // If the target partition cannot contain the row, there is no sense
          // of searching for the value: return 'true' if the value is to be
          // removed from the IN(...) predicate.
          KuduPartialRow row(&schema);
          if (auto s = row.Set(idx, reinterpret_cast<const uint8_t*>(value));
              !s.ok()) {
            LOG(DFATAL) << s.ToString();
            return false;
          }
          return !partition_schema.PartitionMayContainRow(partition, row);
        }),
        predicate_values->end());
  }
}

vector<ColumnSchema> ScanSpec::GetMissingColumns(const Schema& projection) {
  vector<ColumnSchema> missing_cols;
  unordered_set<string> missing_col_names;
  for (const auto& entry : predicates_) {
    const auto& predicate = entry.second;
    const auto& column_name = predicate.column().name();
    if (projection.find_column(column_name) == Schema::kColumnNotFound &&
        !ContainsKey(missing_col_names, column_name)) {
      missing_cols.push_back(predicate.column());
      InsertOrDie(&missing_col_names, column_name);
    }
  }
  return missing_cols;
}

void ScanSpec::PushPredicatesIntoPrimaryKeyBounds(const Schema& schema,
                                                  Arena* arena,
                                                  bool remove_pushed_predicates) {
  if (CanShortCircuit()) {
    return;
  }
  // Step 1: load key column predicate values into a pair of rows.
  uint8_t* lower_buf = static_cast<uint8_t*>(
      CHECK_NOTNULL(arena->AllocateBytes(schema.key_byte_size())));
  uint8_t* upper_buf = static_cast<uint8_t*>(
      CHECK_NOTNULL(arena->AllocateBytes(schema.key_byte_size())));
  ContiguousRow lower_key(&schema, lower_buf);
  ContiguousRow upper_key(&schema, upper_buf);

  int lower_bound_predicates_pushed =
      key_util::PushLowerBoundPrimaryKeyPredicates(predicates_, &lower_key);
  int upper_bound_predicates_pushed = key_util::PushUpperBoundPrimaryKeyPredicates(
      predicates_, &upper_key, arena);

  // Step 2: Erase pushed predicates
  // Predicates through the first range predicate may be erased.
  if (remove_pushed_predicates) {
    for (int32_t col_idx = 0;
         col_idx < max(lower_bound_predicates_pushed, upper_bound_predicates_pushed);
         col_idx++) {
      const string& column = schema.column(col_idx).name();
      PredicateType type = FindOrDie(predicates_, column).predicate_type();
      if (type == PredicateType::Equality) {
        RemovePredicate(column);
      } else if (type == PredicateType::Range) {
        RemovePredicate(column);
        break;
      } else if (type == PredicateType::InList) {
        // InList predicates should not be removed as the full constraints imposed by an InList
        // cannot be translated into only a single set of lower and upper bound primary keys
        break;
      } else if (type == PredicateType::InBloomFilter) {
        // InBloomFilter predicates should not be removed as the full constraints imposed by bloom
        // filters cannot be translated into only a single set of lower and upper bound primary keys
        break;
      } else {
        LOG(FATAL) << "Can not remove unknown predicate type";
      }
    }
  }

  // Step 3: set the new bounds
  if (lower_bound_predicates_pushed > 0) {
    EncodedKey* lower = EncodedKey::FromContiguousRow(ConstContiguousRow(lower_key), arena);
    SetLowerBoundKey(lower);
  }
  if (upper_bound_predicates_pushed > 0) {
    EncodedKey* upper = EncodedKey::FromContiguousRow(ConstContiguousRow(upper_key), arena);
    SetExclusiveUpperBoundKey(upper);
  }
}

void ScanSpec::LiftPrimaryKeyBounds(const Schema& schema, Arena* arena) {
  if (CanShortCircuit()) {
    return;
  }
  if (lower_bound_key_ == nullptr && exclusive_upper_bound_key_ == nullptr) { return; }
  int32_t num_key_columns = schema.num_key_columns();
  for (int32_t col_idx = 0; col_idx < num_key_columns; col_idx++) {
    const ColumnSchema& column = schema.column(col_idx);
    const void* lower = lower_bound_key_ == nullptr
      ? nullptr : lower_bound_key_->raw_keys()[col_idx];
    const void* upper = exclusive_upper_bound_key_ == nullptr
      ? nullptr : exclusive_upper_bound_key_->raw_keys()[col_idx];

    if (lower != nullptr && upper != nullptr && column.Compare(lower, upper) == 0) {
      // We are still in the equality prefix of the bounds
      AddPredicate(ColumnPredicate::Equality(column, lower));
    } else {

      // Determine if the upper bound column value is exclusive or inclusive.
      // The value is exclusive if all of the remaining column values are
      // equal to the minimum value, otherwise it's inclusive.
      //
      // examples, with key columns: (int8 a, int8 b):
      //
      // key >= (1, 2)
      //      < (3, min)
      // should result in predicate 1 <= a < 3
      //
      // key >= (1, 2)
      //      < (3, 5)
      // should result in predicate 1 <= a < 4
      bool is_exclusive = true;
      if (upper != nullptr) {
        uint8_t min[kLargestTypeSize];
        for (int32_t suffix_idx = col_idx + 1;
            is_exclusive && suffix_idx < num_key_columns;
            suffix_idx++) {
          const ColumnSchema& suffix_column = schema.column(suffix_idx);
          suffix_column.type_info()->CopyMinValue(min);
          const void* suffix_val = exclusive_upper_bound_key_->raw_keys()[suffix_idx];
          is_exclusive &= suffix_column.type_info()->Compare(suffix_val, min) == 0;
        }
      }

      if (is_exclusive) {
        ColumnPredicate predicate = ColumnPredicate::Range(column, lower, upper);
        if (predicate.predicate_type() == PredicateType::Equality &&
            col_idx + 1 < num_key_columns && lower_bound_key_ != nullptr) {
          // If this turns out to be an equality predicate, then we can add one
          // more lower bound predicate from the next component (if it exists).
          //
          // example, with key columns (int8 a, int8 b):
          //
          // key >= (2, 3)
          // key  < (3, min)
          //
          // should result in the predicates:
          //
          // a = 2
          // b >= 3
          AddPredicate(ColumnPredicate::Range(schema.column(col_idx + 1),
                                              lower_bound_key_->raw_keys()[col_idx + 1],
                                              nullptr));
        }
        AddPredicate(std::move(predicate));
      } else {
        auto pred = ColumnPredicate::InclusiveRange(column, lower, upper, arena);
        if (pred) AddPredicate(*pred);
      }
      break;
    }
  }
}

} // namespace kudu
