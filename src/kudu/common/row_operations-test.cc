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

#include "kudu/common/row_operations.h"

#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

DECLARE_int32(max_cell_size_bytes);

namespace kudu {

class RowOperationsTest : public KuduTest {
 public:
  RowOperationsTest()
    : arena_(1024) {
    SeedRandom();

    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", INT32));
    CHECK_OK(builder.AddColumn("int_val", INT32));
    CHECK_OK(builder.AddNullableColumn("string_val", STRING));
    schema_ = builder.Build();
    schema_without_ids_ = builder.BuildWithoutIds();
  }
 protected:
  void CheckDecodeDoesntCrash(const Schema& client_schema,
                              const Schema& server_schema,
                              const RowOperationsPB& pb);
  void DoFuzzTest(const Schema& server_schema,
                  const KuduPartialRow& row,
                  int n_random_changes);

  Schema schema_;
  Schema schema_without_ids_;
  Arena arena_;
};

// Perform some random mutation to a random byte in the provided string.
static void DoRandomMutation(string* s) {
  int target_idx = random() % s->size();
  char* target_byte = &(*s)[target_idx];
  switch (random() % 3) {
    case 0:
      // increment a random byte by 1
      (*target_byte)++;
      break;
    case 1:
      // decrement a random byte by 1
      (*target_byte)--;
      break;
    case 2:
      // replace byte with random value
      (*target_byte) = random();
      break;
  }
}

void RowOperationsTest::CheckDecodeDoesntCrash(const Schema& client_schema,
                                               const Schema& server_schema,
                                               const RowOperationsPB& pb) {
  arena_.Reset();
  RowOperationsPBDecoder decoder(&pb, &client_schema, &server_schema, &arena_);
  // Decoding the operations, regardless of the mode, should not result in a
  // crash.
  vector<DecodedRowOperation> ops;
  Status s = decoder.DecodeOperations<DecoderMode::WRITE_OPS>(&ops);
  if (!s.ok()) {
    s = decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops);
  }
  if (s.ok() && !ops.empty()) {
    // If we got an OK result, then we should be able to stringify without
    // crashing. This ensures that any indirect data (eg strings) gets
    // set correctly.
    ignore_result(ops[0].ToString(server_schema));
  }
  // Bad Status is OK -- we expect corruptions here.
}

void RowOperationsTest::DoFuzzTest(const Schema& server_schema,
                                   const KuduPartialRow& row,
                                   int n_random_changes) {
  for (int operation = 0; operation <= 11; operation++) {
    RowOperationsPB pb;
    RowOperationsPBEncoder enc(&pb);

    switch (operation) {
      case 0:
        enc.Add(RowOperationsPB::INSERT, row);
        break;
      case 1:
        enc.Add(RowOperationsPB::UPSERT, row);
        break;
      case 2:
        enc.Add(RowOperationsPB::UPDATE, row);
        break;
      case 3:
        enc.Add(RowOperationsPB::DELETE, row);
        break;
      case 4:
        enc.Add(RowOperationsPB::SPLIT_ROW, row);
        break;
      case 5:
        enc.Add(RowOperationsPB::RANGE_LOWER_BOUND, row);
        break;
      case 6:
        enc.Add(RowOperationsPB::RANGE_UPPER_BOUND, row);
        break;
      case 7:
        enc.Add(RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND, row);
        break;
      case 8:
        enc.Add(RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND, row);
        break;
      case 9:
        enc.Add(RowOperationsPB::INSERT_IGNORE, row);
        break;
      case 10:
        enc.Add(RowOperationsPB::UPDATE_IGNORE, row);
        break;
      case 11:
        enc.Add(RowOperationsPB::DELETE_IGNORE, row);
        break;
    }

    const Schema* client_schema = row.schema();

    // Check that the un-mutated row doesn't crash.
    CheckDecodeDoesntCrash(*client_schema, server_schema, pb);

    RowOperationsPB mutated;

    // Check all possible truncations of the protobuf 'rows' field.
    for (int i = 0; i < pb.rows().size(); i++) {
      mutated.CopyFrom(pb);
      mutated.mutable_rows()->resize(i);
      CheckDecodeDoesntCrash(*client_schema, server_schema, mutated);
    }

    // Check bit flips of every bit in the first three bytes, which are
    // particularly interesting, since they contain the null/isset
    // bitmaps.
    for (int bit = 0; bit < 8 * 3; bit++) {
      int byte_idx = bit / 8;
      int bit_idx = bit % 8;
      int mask = 1 << bit_idx;

      (*mutated.mutable_rows())[byte_idx] ^= mask;
      CheckDecodeDoesntCrash(*client_schema, server_schema, mutated);
      (*mutated.mutable_rows())[byte_idx] ^= mask;
    }

    // Check random byte changes in the 'rows' field.
    for (int i = 0; i < n_random_changes; i++) {
      mutated.CopyFrom(pb);
      DoRandomMutation(mutated.mutable_rows());
      CheckDecodeDoesntCrash(*client_schema, server_schema, mutated);
    }
  }
}

// Test that, even if the protobuf is corrupt in some way, we do not
// crash. These protobufs are provided by clients, so we want to make sure
// a malicious client can't crash the server.
TEST_F(RowOperationsTest, FuzzTest) {
  const int n_iters = AllowSlowTests() ? 10000 : 1000;

  KuduPartialRow row(&schema_without_ids_);
  EXPECT_OK(row.SetInt32("int_val", 54321));
  EXPECT_OK(row.SetStringCopy("string_val", "hello world"));
  DoFuzzTest(schema_, row, n_iters);
  EXPECT_OK(row.SetNull("string_val"));
  DoFuzzTest(schema_, row, n_iters);
}

// Add the given column, but with some probability change the type
// and nullability.
void AddFuzzedColumn(SchemaBuilder* builder,
                     const string& name,
                     DataType default_type) {
  DataType rand_types[] = {INT32, INT64, DOUBLE, STRING};
  DataType t = default_type;
  if (random() % 3 == 0) {
    t = rand_types[random() % arraysize(rand_types)];
  }
  bool nullable = random() & 1;
  CHECK_OK(builder->AddColumn(name, t, nullable, NULL, NULL));
}

// Generate a randomized schema, where some columns might be missing,
// and types/nullability are randomized. We weight towards not making
// too many changes so that it's likely we generate compatible client
// and server schemas.
Schema GenRandomSchema(bool with_ids) {
  SchemaBuilder builder;
  if (random() % 5 != 0) {
    AddFuzzedColumn(&builder, "c1", INT32);
  }
  if (random() % 5 != 0) {
    AddFuzzedColumn(&builder, "c2", INT32);
  }
  if (random() % 5 != 0 || !builder.is_valid()) {
    AddFuzzedColumn(&builder, "c3", STRING);
  }

  return with_ids ? builder.Build() : builder.BuildWithoutIds();
}

namespace {

struct FailingCase {
  Schema* client_schema;
  Schema* server_schema;
  KuduPartialRow* row;
};
FailingCase g_failing_case;

// ASAN callback which will dump the case which caused a failure.
void DumpFailingCase() {
  LOG(INFO) << "Failed on the following case:";
  LOG(INFO) << "Client schema:\n" << g_failing_case.client_schema->ToString();
  LOG(INFO) << "Server schema:\n" << g_failing_case.server_schema->ToString();
  LOG(INFO) << "Row: " << g_failing_case.row->ToString();
}

void GlogFailure() {
  DumpFailingCase();
  abort();
}

} // anonymous namespace

// Fuzz test which generates random pairs of client/server schemas, with
// random mutations like adding an extra column, removing a column, changing
// types, and changing nullability.
TEST_F(RowOperationsTest, SchemaFuzz) {
#ifdef ADDRESS_SANITIZER
  // Prevent timeouts in ASAN build.
  const int kSlowTestIters = 1000;
#else
  const int kSlowTestIters = 10000;
#endif
  const int n_iters = AllowSlowTests() ? kSlowTestIters : 10;
  for (int i = 0; i < n_iters; i++) {
    // Generate a random client and server schema pair.
    Schema client_schema = GenRandomSchema(false);
    Schema server_schema = GenRandomSchema(true);
    KuduPartialRow row(&client_schema);

    // On a crash or ASAN failure, dump the case information to the log so we
    // can write a more specific repro.
    g_failing_case.client_schema = &client_schema;
    g_failing_case.server_schema = &server_schema;
    g_failing_case.row = &row;
    ASAN_SET_DEATH_CALLBACK(&DumpFailingCase);
    google::InstallFailureFunction(reinterpret_cast<google::logging_fail_func_t>(GlogFailure));

    for (int i = 0; i < client_schema.num_columns(); i++) {
      if (client_schema.column(i).is_nullable() &&
          random() % 3 == 0) {
        CHECK_OK(row.SetNull(i));
        continue;
      }
      switch (client_schema.column(i).type_info()->type()) {
        case INT32:
          CHECK_OK(row.SetInt32(i, 12345));
          break;
        case INT64:
          CHECK_OK(row.SetInt64(i, 12345678));
          break;
        case DOUBLE:
          CHECK_OK(row.SetDouble(i, 1234.5678));
          break;
        case STRING:
          CHECK_OK(row.SetStringCopy(i, "hello"));
          break;
        default:
          LOG(FATAL);
      }
    }

    DoFuzzTest(server_schema, row, 100);
    ASAN_SET_DEATH_CALLBACK(NULL);
    google::InstallFailureFunction(reinterpret_cast<google::logging_fail_func_t>(abort));
  }
}

// One case from SchemaFuzz which failed previously.
TEST_F(RowOperationsTest, TestFuzz1) {
  SchemaBuilder client_schema_builder;
  client_schema_builder.AddColumn("c1", INT32, false, nullptr, nullptr);
  client_schema_builder.AddColumn("c2", STRING, false, nullptr, nullptr);
  Schema client_schema = client_schema_builder.BuildWithoutIds();
  SchemaBuilder server_schema_builder;
  server_schema_builder.AddColumn("c1", INT32, false, nullptr, nullptr);
  server_schema_builder.AddColumn("c2", STRING, false, nullptr, nullptr);
  Schema server_schema = server_schema_builder.Build();
  KuduPartialRow row(&client_schema);
  CHECK_OK(row.SetInt32(0, 12345));
  CHECK_OK(row.SetStringCopy(1, "hello"));
  DoFuzzTest(server_schema, row, 100);
}

// Another case from SchemaFuzz which failed previously.
TEST_F(RowOperationsTest, TestFuzz2) {
  SchemaBuilder client_schema_builder;
  client_schema_builder.AddColumn("c1", STRING, true, nullptr, nullptr);
  client_schema_builder.AddColumn("c2", STRING, false, nullptr, nullptr);
  Schema client_schema = client_schema_builder.BuildWithoutIds();
  SchemaBuilder server_schema_builder;
  server_schema_builder.AddColumn("c1", STRING, true, nullptr, nullptr);
  server_schema_builder.AddColumn("c2", STRING, false, nullptr, nullptr);
  Schema server_schema = server_schema_builder.Build();
  KuduPartialRow row(&client_schema);
  CHECK_OK(row.SetNull(0));
  CHECK_OK(row.SetStringCopy(1, "hello"));
  DoFuzzTest(server_schema, row, 100);
}

namespace {

// Project client_row into server_schema, and stringify the result.
// If an error occurs, the result string is "error: <stringified Status>"
string TestProjection(RowOperationsPB::Type type,
                      const KuduPartialRow& client_row,
                      const Schema& server_schema) {
  RowOperationsPB pb;
  RowOperationsPBEncoder enc(&pb);
  enc.Add(type, client_row);

  // Decode it
  Arena arena(1024);
  vector<DecodedRowOperation> ops;
  RowOperationsPBDecoder dec(&pb, client_row.schema(), &server_schema, &arena);
  Status s = dec.DecodeOperations<DecoderMode::WRITE_OPS>(&ops);

  if (!s.ok()) {
    return "error: " + s.ToString();
  }
  CHECK_EQ(1, ops.size());
  return ops[0].ToString(server_schema);
}

} // anonymous namespace

// Test decoding partial rows from a client who has a schema which matches
// the table schema.
TEST_F(RowOperationsTest, ProjectionTestWholeSchemaSpecified) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       1);
  // Test a row missing 'key', which is key column.
  {
    KuduPartialRow client_row(&client_schema);
    EXPECT_EQ("row error: Invalid argument: No value provided for required column: "
              "key INT32 NOT NULL",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Force to set null on key column.
  {
    int col_idx;
    ASSERT_OK(client_schema.FindColumn("key", &col_idx));
    KuduPartialRow client_row(&client_schema);
    ContiguousRow row(&client_schema, client_row.row_data_);
    row.set_null(col_idx, true);
    BitmapSet(client_row.isset_bitmap_, col_idx);
    EXPECT_EQ("row error: Invalid argument: NULL values not allowed for non-nullable column: "
              "key INT32 NOT NULL",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Test a row missing 'int_val', which is required.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    EXPECT_EQ("row error: Invalid argument: No value provided for required column: "
              "int_val INT32 NOT NULL",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Test a row missing 'string_val', which is nullable
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetInt32("int_val", 54321));
    // The NULL should get filled in.
    EXPECT_EQ("INSERT (int32 key=12345, int32 int_val=54321, string string_val=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));
  }

  // Test a row with all of the fields specified, both with the nullable field
  // specified to be NULL and non-NULL.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetInt32("int_val", 54321));
    CHECK_OK(client_row.SetStringCopy("string_val", "hello world"));
    EXPECT_EQ(R"(INSERT (int32 key=12345, int32 int_val=54321, string string_val="hello world"))",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));

    // The first result should have the field specified.
    // The second result should have the field NULL, since it was explicitly set.
    CHECK_OK(client_row.SetNull("string_val"));
    EXPECT_EQ("INSERT (int32 key=12345, int32 int_val=54321, string string_val=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, schema_));

  }
}

TEST_F(RowOperationsTest, ProjectionTestWithDefaults) {
  int32_t nullable_default = 123;
  int32_t non_null_default = 456;
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", INT32));
  CHECK_OK(b.AddColumn("nullable_with_default", INT32, true,
                       &nullable_default, &nullable_default));
  CHECK_OK(b.AddColumn("non_null_with_default", INT32, false,
                       &non_null_default, &non_null_default));
  Schema server_schema = b.Build();

  // Clients may not have the defaults specified.
  // TODO: evaluate whether this should be true - how "dumb" should clients be?
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("nullable_with_default", INT32, true),
                         ColumnSchema("non_null_with_default", INT32, false) },
                       1);

  // Specify just the key. The other two columns have defaults, so they'll get filled in.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    EXPECT_EQ("INSERT (int32 key=12345, int32 nullable_with_default=123,"
              " int32 non_null_with_default=456)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify the key and override both defaults
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetInt32("nullable_with_default", 12345));
    CHECK_OK(client_row.SetInt32("non_null_with_default", 54321));
    EXPECT_EQ("INSERT (int32 key=12345, int32 nullable_with_default=12345,"
              " int32 non_null_with_default=54321)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify the key and override both defaults, overriding the nullable
  // one to NULL.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetNull("nullable_with_default"));
    CHECK_OK(client_row.SetInt32("non_null_with_default", 54321));
    EXPECT_EQ("INSERT (int32 key=12345, int32 nullable_with_default=NULL,"
              " int32 non_null_with_default=54321)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify the key and override both defaults, overriding the non-nullable
  // one to NULL.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetInt32("nullable_with_default", 12345));
    Status s = client_row.SetNull("non_null_with_default");
    CHECK(s.IsInvalidArgument());
    EXPECT_EQ("INSERT (int32 key=12345, int32 nullable_with_default=12345,"
              " int32 non_null_with_default=456)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Test cases where the client only has a subset of the fields
// of the table, but where the missing columns have defaults
// or are NULLable.
TEST_F(RowOperationsTest, ProjectionTestWithClientHavingValidSubset) {
  int32_t nullable_default = 123;
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", INT32));
  CHECK_OK(b.AddColumn("int_val", INT32));
  CHECK_OK(b.AddColumn("new_int_with_default", INT32, false,
                       &nullable_default, &nullable_default));
  CHECK_OK(b.AddNullableColumn("new_nullable_int", INT32));
  Schema server_schema = b.Build();

  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32) },
                       1);

  // Specify just the key. This is an error because we're missing int_val.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    EXPECT_EQ("row error: Invalid argument: No value provided for required column:"
              " int_val INT32 NOT NULL",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }

  // Specify both of the columns that the client is aware of.
  // Defaults should be filled for the other two.
  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    CHECK_OK(client_row.SetInt32("int_val", 12345));
    EXPECT_EQ("INSERT (int32 key=12345, int32 int_val=12345,"
              " int32 new_int_with_default=123, int32 new_nullable_int=NULL)",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Test cases where the client is missing a column which is non-null
// and has no default. This is an incompatible client.
TEST_F(RowOperationsTest, ProjectionTestWithClientHavingInvalidSubset) {
  SchemaBuilder b;
  CHECK_OK(b.AddKeyColumn("key", INT32));
  CHECK_OK(b.AddColumn("int_val", INT32));
  Schema server_schema = b.Build();

  CHECK_OK(b.RemoveColumn("int_val"));
  Schema client_schema = b.BuildWithoutIds();

  {
    KuduPartialRow client_row(&client_schema);
    CHECK_OK(client_row.SetInt32("key", 12345));
    EXPECT_EQ("error: Invalid argument: Client missing required column:"
              " int_val INT32 NOT NULL",
              TestProjection(RowOperationsPB::INSERT, client_row, server_schema));
  }
}

// Simple Update case where the client and server schemas match.
TEST_F(RowOperationsTest, TestProjectUpdates) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       1);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  // Check without specifying any columns
  KuduPartialRow client_row(&client_schema);
  EXPECT_EQ("row error: Invalid argument: No value provided for key column: key INT32 NOT NULL",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Specify the key and no columns to update
  ASSERT_OK(client_row.SetInt32("key", 12345));
  EXPECT_EQ("row error: Invalid argument: No fields updated, key is: (int32 key=12345)",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Specify the key and update one column.
  ASSERT_OK(client_row.SetInt32("int_val", 12345));
  EXPECT_EQ("MUTATE (int32 key=12345) SET int_val=12345",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Specify the key and update both columns
  ASSERT_OK(client_row.SetStringNoCopy("string_val", "foo"));
  EXPECT_EQ(R"(MUTATE (int32 key=12345) SET int_val=12345, string_val="foo")",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Update the nullable column to null.
  ASSERT_OK(client_row.SetNull("string_val"));
  EXPECT_EQ("MUTATE (int32 key=12345) SET int_val=12345, string_val=NULL",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));

  // Force to set null on key column.
  {
    KuduPartialRow client_row(&client_schema);
    int col_idx;
    ASSERT_OK(client_schema.FindColumn("key", &col_idx));
    ContiguousRow row(&client_schema, client_row.row_data_);
    row.set_null(col_idx, true);
    BitmapSet(client_row.isset_bitmap_, col_idx);
    EXPECT_EQ("row error: Invalid argument: NULL values not allowed for key column: "
              "key INT32 NOT NULL",
              TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
  }

  // Force to set null on non-nullable column.
  {
    KuduPartialRow client_row(&client_schema);
    ASSERT_OK(client_row.SetInt32("key", 12345));
    int col_idx;
    ASSERT_OK(client_schema.FindColumn("int_val", &col_idx));
    ContiguousRow row(&client_schema, client_row.row_data_);
    row.set_null(col_idx, true);
    BitmapSet(client_row.isset_bitmap_, col_idx);
    EXPECT_EQ("row error: Invalid argument: NULL value not allowed for non-nullable column: "
              "int_val INT32 NOT NULL",
              TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
  }
}

// Client schema has the columns in a different order. Makes
// sure the name-based projection is functioning.
TEST_F(RowOperationsTest, TestProjectUpdatesReorderedColumns) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("string_val", STRING, true),
                         ColumnSchema("int_val", INT32) },
                       1);
  Schema server_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_OK(client_row.SetInt32("key", 12345));
  ASSERT_OK(client_row.SetInt32("int_val", 54321));
  EXPECT_EQ("MUTATE (int32 key=12345) SET int_val=54321",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

// Client schema is missing one of the columns in the server schema.
// This is OK on an update.
TEST_F(RowOperationsTest, DISABLED_TestProjectUpdatesSubsetOfColumns) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       1);
  Schema server_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_OK(client_row.SetInt32("key", 12345));
  ASSERT_OK(client_row.SetStringNoCopy("string_val", "foo"));
  EXPECT_EQ("MUTATE (int32 key=12345) SET string_val=foo",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

TEST_F(RowOperationsTest, TestClientMismatchedType) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT8) },
                       1);
  Schema server_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("int_val", INT32) },
                       1);
  server_schema = SchemaBuilder(server_schema).Build();

  KuduPartialRow client_row(&client_schema);
  ASSERT_OK(client_row.SetInt32("key", 12345));
  ASSERT_OK(client_row.SetInt8("int_val", 1));
  EXPECT_EQ("error: Invalid argument: The column 'int_val' must have type "
            "INT32 NOT NULL found INT8 NOT NULL",
            TestProjection(RowOperationsPB::UPDATE, client_row, server_schema));
}

TEST_F(RowOperationsTest, TestProjectDeletes) {
  Schema client_schema({ ColumnSchema("key", INT32),
                         ColumnSchema("key_2", INT32),
                         ColumnSchema("int_val", INT32),
                         ColumnSchema("string_val", STRING, true) },
                       2);
  Schema server_schema = SchemaBuilder(client_schema).Build();

  KuduPartialRow client_row(&client_schema);
  // No columns set
  EXPECT_EQ("row error: Invalid argument: No value provided for key column: key INT32 NOT NULL",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Only half the key set
  ASSERT_OK(client_row.SetInt32("key", 12345));
  EXPECT_EQ("row error: Invalid argument: No value provided for key column: key_2 INT32 NOT NULL",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Whole key set (correct)
  ASSERT_OK(client_row.SetInt32("key_2", 54321));
  EXPECT_EQ("MUTATE (int32 key=12345, int32 key_2=54321) DELETE",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Extra column set
  ASSERT_OK(client_row.SetNull("string_val"));
  EXPECT_EQ("row error: Invalid argument: DELETE should not have a value for column: "
            "string_val STRING NULLABLE",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Extra column set (incorrect)
  ASSERT_OK(client_row.SetStringNoCopy("string_val", "hello"));
  EXPECT_EQ("row error: Invalid argument: DELETE should not have a value for column: "
            "string_val STRING NULLABLE",
            TestProjection(RowOperationsPB::DELETE, client_row, server_schema));

  // Force to set null on key column.
  {
    KuduPartialRow client_row(&client_schema);
    int col_idx;
    ASSERT_OK(client_schema.FindColumn("key", &col_idx));
    ContiguousRow row(&client_schema, client_row.row_data_);
    row.set_null(col_idx, true);
    BitmapSet(client_row.isset_bitmap_, col_idx);
    EXPECT_EQ("row error: Invalid argument: NULL values not allowed for key column: "
              "key INT32 NOT NULL",
              TestProjection(RowOperationsPB::DELETE, client_row, server_schema));
  }

  // Force to set null on non-key column.
  {
    KuduPartialRow client_row(&client_schema);
    ASSERT_OK(client_row.SetInt32("key", 12345));
    ASSERT_OK(client_row.SetInt32("key_2", 12345));
    int col_idx;
    ASSERT_OK(client_schema.FindColumn("int_val", &col_idx));
    ContiguousRow row(&client_schema, client_row.row_data_);
    row.set_null(col_idx, true);
    BitmapSet(client_row.isset_bitmap_, col_idx);
    EXPECT_EQ("row error: Invalid argument: DELETE should not have a value for column: "
              "int_val INT32 NOT NULL",
              TestProjection(RowOperationsPB::DELETE, client_row, server_schema));
  }
}

namespace {
// If 'use_tablet_schema_as_client_schema' is true, the tablet schema with its
// column IDs is passed in as the client_schema for the RowOperationsPBDecoder.
void TestSplitKeys(bool use_tablet_schema_as_client_schema) {
  Schema client_schema = Schema({ ColumnSchema("int8", INT8),
                                  ColumnSchema("int16", INT16),
                                  ColumnSchema("int32", INT32),
                                  ColumnSchema("int64", INT64),
                                  ColumnSchema("string", STRING),
                                  ColumnSchema("binary", BINARY),
                                  ColumnSchema("timestamp", UNIXTIME_MICROS),
                                  ColumnSchema("missing", STRING) },
                                8);

  // Use values at the upper end of the range.
  int8_t int8_expected = 0xFE;
  int16_t int16_expected = 0xFFFE;
  int32_t int32_expected = 0xFFFFFE;
  int64_t int64_expected = 0xFFFFFFFE;

  KuduPartialRow row(&client_schema);
  ASSERT_OK(row.SetInt8("int8", int8_expected));
  ASSERT_OK(row.SetInt16("int16", int16_expected));
  ASSERT_OK(row.SetInt32("int32", int32_expected));
  ASSERT_OK(row.SetInt64("int64", int64_expected));
  ASSERT_OK(row.SetStringNoCopy("string", "string-value"));
  ASSERT_OK(row.SetBinaryNoCopy("binary", "binary-value"));
  ASSERT_OK(row.SetUnixTimeMicros("timestamp", 9));

  RowOperationsPB pb;
  RowOperationsPBEncoder(&pb).Add(RowOperationsPB::SPLIT_ROW, row);

  Schema schema = client_schema.CopyWithColumnIds();
  RowOperationsPBDecoder decoder(
      &pb, use_tablet_schema_as_client_schema ? &schema : &client_schema, &schema, nullptr);
  vector<DecodedRowOperation> ops;
  ASSERT_OK(decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops));
  ASSERT_EQ(1, ops.size());

  const shared_ptr<KuduPartialRow>& row2 = ops[0].split_row;

  int8_t int8_val;
  ASSERT_OK(row2->GetInt8("int8", &int8_val));
  CHECK_EQ(int8_expected, int8_val);

  int16_t int16_val;
  ASSERT_OK(row2->GetInt16("int16", &int16_val));
  CHECK_EQ(int16_expected, int16_val);

  int32_t int32_val;
  ASSERT_OK(row2->GetInt32("int32", &int32_val));
  CHECK_EQ(int32_expected, int32_val);

  int64_t int64_val;
  ASSERT_OK(row2->GetInt64("int64", &int64_val));
  CHECK_EQ(int64_expected, int64_val);

  Slice string_val;
  ASSERT_OK(row2->GetString("string", &string_val));
  CHECK_EQ("string-value", string_val);

  Slice binary_val;
  ASSERT_OK(row2->GetBinary("binary", &binary_val));
  CHECK_EQ(Slice("binary-value"), binary_val);

  CHECK(!row2->IsColumnSet("missing"));
}

void CheckExceedCellLimit(const Schema& client_schema,
                          const vector<string>& col_values,
                          RowOperationsPB::Type op_type,
                          const Status& expect_status,
                          const string& expect_msg) {
  ASSERT_EQ(client_schema.num_columns(), col_values.size());

  // Fill the row.
  KuduPartialRow row(&client_schema);
  for (size_t i = 0; i < client_schema.num_columns(); ++i) {
    if ((op_type == RowOperationsPB::DELETE || op_type == RowOperationsPB::DELETE_IGNORE) &&
         i >= client_schema.num_key_columns()) {
      // DELETE should not have a value for non-key column.
      break;
    }
    const ColumnSchema& column_schema = client_schema.column(i);
    switch (column_schema.type_info()->type()) {
      case STRING:
        ASSERT_OK(row.SetStringNoCopy(static_cast<int>(i), col_values[i]));
        break;
      case BINARY:
        ASSERT_OK(row.SetBinaryNoCopy(static_cast<int>(i), col_values[i]));
        break;
      default:
        ASSERT_TRUE(false) << "Unsupported type: " << column_schema.ToString();
    }
  }

  RowOperationsPB pb;
  RowOperationsPBEncoder(&pb).Add(op_type, row);

  Arena arena(1024*1024);
  Schema schema = client_schema.CopyWithColumnIds();
  RowOperationsPBDecoder decoder(&pb, &client_schema, &schema, &arena);
  vector<DecodedRowOperation> ops;
  Status s;
  switch (op_type) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::UPDATE:
    case RowOperationsPB::DELETE:
    case RowOperationsPB::UPSERT:
    case RowOperationsPB::INSERT_IGNORE:
    case RowOperationsPB::UPDATE_IGNORE:
    case RowOperationsPB::DELETE_IGNORE:
      s = decoder.DecodeOperations<WRITE_OPS>(&ops);
      break;
    case RowOperationsPB::SPLIT_ROW:
    case RowOperationsPB::RANGE_LOWER_BOUND:
    case RowOperationsPB::RANGE_UPPER_BOUND:
    case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND:
    case RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND:
      s = decoder.DecodeOperations<SPLIT_ROWS>(&ops);
      break;
    default:
      ASSERT_TRUE(false) << "Unsupported op_type " << op_type;
  }
  ASSERT_OK(s);
  for (const auto& op : ops) {
    ASSERT_EQ(op.result.CodeAsString(),
        expect_status.CodeAsString()) << op.result.message().ToString();
    ASSERT_STR_CONTAINS(op.result.ToString(), expect_msg);
  }
}

void CheckInsertUpsertExceedCellLimit(const Schema& client_schema,
                                      const vector<string>& col_values,
                                      const Status& expect_status,
                                      const string& expect_msg) {
  for (auto op_type : { RowOperationsPB::INSERT,
                        RowOperationsPB::INSERT_IGNORE,
                        RowOperationsPB::UPSERT }) {
    NO_FATALS(CheckExceedCellLimit(client_schema, col_values, op_type, expect_status, expect_msg));
  }
}

void CheckUpdateDeleteExceedCellLimit(const Schema& client_schema,
                                      const vector<string>& col_values,
                                      const Status& expect_status,
                                      const string& expect_msg) {
  for (auto op_type : { RowOperationsPB::UPDATE,
                        RowOperationsPB::UPDATE_IGNORE,
                        RowOperationsPB::DELETE,
                        RowOperationsPB::DELETE_IGNORE }) {
    NO_FATALS(CheckExceedCellLimit(client_schema, col_values, op_type, expect_status, expect_msg));
  }
}

void CheckWriteExceedCellLimit(const Schema& client_schema,
                               const vector<string>& col_values,
                               const Status& expect_status,
                               const string& expect_msg) {
  NO_FATALS(CheckInsertUpsertExceedCellLimit(client_schema, col_values, expect_status, expect_msg));
  NO_FATALS(CheckUpdateDeleteExceedCellLimit(client_schema, col_values, expect_status, expect_msg));
}

void CheckSplitExceedCellLimit(const Schema& client_schema,
                               const vector<string>& col_values,
                               const Status& expect_status,
                               const string& expect_msg) {
  for (auto op_type : { RowOperationsPB::SPLIT_ROW,
                        RowOperationsPB::RANGE_LOWER_BOUND,
                        RowOperationsPB::RANGE_UPPER_BOUND,
                        RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND,
                        RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND }) {
    NO_FATALS(CheckExceedCellLimit(client_schema, col_values, op_type, expect_status, expect_msg));
  }
}
} // anonymous namespace

// Decodes a split row using RowOperationsPBDecoder with DecoderMode::SPLIT_ROWS under
// two conditions, one where the client schema doesn't have column IDs as expected and
// another where both the client schema and the tablet schema are the same object.
TEST_F(RowOperationsTest, SplitKeysRoundTrip) {
  TestSplitKeys(false);
  TestSplitKeys(true);
}

TEST_F(RowOperationsTest, ExceedCellLimit) {
  Schema client_schema = Schema({ ColumnSchema("key_string", STRING),
                                  ColumnSchema("key_binary", BINARY),
                                  ColumnSchema("col_string", STRING),
                                  ColumnSchema("col_binary", BINARY) },
                                2);

  const string long_string(static_cast<size_t>(FLAGS_max_cell_size_bytes), 'a');
  const string too_long_string(static_cast<size_t>(FLAGS_max_cell_size_bytes + 1), 'a');
  const vector<string> base_col_values(4, long_string);

  // All column cell sizes are not exceed.
  NO_FATALS(CheckWriteExceedCellLimit(client_schema, base_col_values, Status::OK(), ""));

  // Some column cell size exceed for INSERT and UPSERT operation.
  for (size_t i = 0; i < client_schema.num_columns(); ++i) {
    auto col_values(base_col_values);
    col_values[i] = too_long_string;
    NO_FATALS(CheckInsertUpsertExceedCellLimit(client_schema,
                                               col_values,
                                               Status::InvalidArgument(""),
                                               Substitute("value too large for column '$0'"
                                                          " ($1 bytes, maximum is $2 bytes)",
                                                          client_schema.column(i).name(),
                                                          FLAGS_max_cell_size_bytes + 1,
                                                          FLAGS_max_cell_size_bytes)));
  }

  // Key column cell size exceed for UPDATE and DELETE operation, it's OK.
  for (size_t i = 0; i < client_schema.num_key_columns(); ++i) {
    auto col_values(base_col_values);
    col_values[i] = too_long_string;
    NO_FATALS(CheckUpdateDeleteExceedCellLimit(client_schema, col_values, Status::OK(), ""));
  }

  // Non-key column cell size exceed for UPDATE operation.
  for (size_t i = client_schema.num_key_columns(); i < client_schema.num_columns(); ++i) {
    auto col_values(base_col_values);
    col_values[i] = too_long_string;
    NO_FATALS(CheckExceedCellLimit(client_schema,
                                   col_values,
                                   RowOperationsPB::UPDATE,
                                   Status::InvalidArgument(""),
                                   Substitute("value too large for column '$0'"
                                              " ($1 bytes, maximum is $2 bytes)",
                                              client_schema.column(i).name(),
                                              FLAGS_max_cell_size_bytes + 1,
                                              FLAGS_max_cell_size_bytes)));
  }

  // Some column cell size exceed for SPLIT_ROW type operation, it's OK.
  for (size_t i = 0; i < client_schema.num_columns(); ++i) {
    auto col_values(base_col_values);
    col_values[i] = too_long_string;
    NO_FATALS(CheckSplitExceedCellLimit(client_schema, col_values, Status::OK(), ""));
  }
}

} // namespace kudu
