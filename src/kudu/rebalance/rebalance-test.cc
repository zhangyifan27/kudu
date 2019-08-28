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

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/rebalance_algo.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/util/test_macros.h"

using kudu::cluster_summary::ReplicaSummary;
using kudu::cluster_summary::ServerHealthSummary;
using kudu::cluster_summary::TableSummary;
using kudu::cluster_summary::TabletSummary;

using std::inserter;
using std::multimap;
using std::ostream;
using std::pair;
using std::sort;
using std::string;
using std::transform;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rebalance {

namespace {

struct ReplicaSummaryInput {
  std::string ts_uuid;
  bool is_voter;
};

struct ServerHealthSummaryInput {
  std::string uuid;
};

struct TabletSummaryInput {
  std::string id;
  std::string table_id;
  std::vector<ReplicaSummaryInput> replicas;
};

struct TableSummaryInput {
  std::string id;
  int replication_factor;
};

// The input to build KsckResults data. Contains relevant sub-fields of the
// KsckResults to use in the test.
struct KsckResultsInput {
  vector<ServerHealthSummaryInput> tserver_summaries;
  vector<TabletSummaryInput> tablet_summaries;
  vector<TableSummaryInput> table_summaries;
};

// The configuration for the test.
struct KsckResultsTestConfig {
  // The input for the test: ksck results.
  KsckResultsInput input;

  // The reference result of transformation of the 'input' field.
  ClusterBalanceInfo ref_balance_info;
};

ClusterRawInfo GenerateRawClusterInfo(const KsckResultsInput& input) {
  ClusterRawInfo raw_info;
  {
    vector<ServerHealthSummary>& summaries = raw_info.tserver_summaries;
    for (const auto& summary_input : input.tserver_summaries) {
      ServerHealthSummary summary;
      summary.uuid = summary_input.uuid;
      summaries.emplace_back(std::move(summary));
    }
  }
  {
    vector<TabletSummary>& summaries = raw_info.tablet_summaries;
    for (const auto& summary_input : input.tablet_summaries) {
      TabletSummary summary;
      summary.id = summary_input.id;
      summary.table_id = summary_input.table_id;
      auto& replicas = summary.replicas;
      for (const auto& replica_input : summary_input.replicas) {
        ReplicaSummary replica;
        replica.ts_uuid = replica_input.ts_uuid;
        replica.is_voter = replica_input.is_voter;
        replicas.emplace_back(std::move(replica));
      }
      summaries.emplace_back(std::move(summary));
    }
  }
  {
    vector<TableSummary>& summaries = raw_info.table_summaries;
    for (const auto& summary_input : input.table_summaries) {
      TableSummary summary;
      summary.id = summary_input.id;
      summary.replication_factor = summary_input.replication_factor;
      summaries.emplace_back(std::move(summary));
    }
  }
  return raw_info;
}

// The order of the key-value pairs whose keys compare equivalent is the order
// of insertion and does not change. Since the insertion order is not
// important for the comparison with the reference results, this comparison
// operator normalizes both the 'lhs' and 'rhs', so the comparison operator
// compares only the contents of the 'servers_by_replica_count', not the order
// of the elements.
bool HasSameContents(const ServersByCountMap& lhs,
                     const ServersByCountMap& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  auto it_lhs = lhs.begin();
  auto it_rhs = rhs.begin();
  while (it_lhs != lhs.end() && it_rhs != rhs.end()) {
    auto key_lhs = it_lhs->first;
    auto key_rhs = it_rhs->first;
    if (key_lhs != key_rhs) {
      return false;
    }

    auto eq_range_lhs = lhs.equal_range(key_lhs);
    auto eq_range_rhs = rhs.equal_range(key_rhs);

    auto getValues = [](pair<ServersByCountMap::const_iterator,
                             ServersByCountMap::const_iterator> range) {
      vector<string> values;
      transform(range.first, range.second,
                inserter(values, values.begin()),
                [](const ServersByCountMap::value_type& elem) {
                  return elem.second;
                });
      sort(values.begin(), values.end());
      return values;
    };

    vector<string> lhs_values = getValues(eq_range_lhs);
    vector<string> rhs_values = getValues(eq_range_rhs);
    if (lhs_values != rhs_values) {
      return false;
    }

    // Advance the iterators to continue with next key.
    it_lhs = eq_range_lhs.second;
    it_rhs = eq_range_rhs.second;
  }

  return true;
}

} // anonymous namespace

bool operator==(const TableBalanceInfo& lhs, const TableBalanceInfo& rhs) {
  return
    lhs.table_id == rhs.table_id &&
    HasSameContents(lhs.servers_by_replica_count,
                    rhs.servers_by_replica_count);
}

bool HasSameContents(const multimap<int32_t, TableBalanceInfo>& lhs,
                     const multimap<int32_t, TableBalanceInfo>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  auto it_lhs = lhs.begin();
  auto it_rhs = rhs.begin();
  while (it_lhs != lhs.end() && it_rhs != rhs.end()) {
    auto key_lhs = it_lhs->first;
    auto key_rhs = it_rhs->first;
    if (key_lhs != key_rhs) {
      return false;
    }

    auto eq_range_lhs = lhs.equal_range(key_lhs);
    auto eq_range_rhs = rhs.equal_range(key_rhs);

    auto getValues = [](pair<multimap<int32_t, TableBalanceInfo>::const_iterator,
                             multimap<int32_t, TableBalanceInfo>::const_iterator> range) {
      vector<TableBalanceInfo> values;
      transform(range.first, range.second,
                inserter(values, values.begin()),
                [](const multimap<int32_t, TableBalanceInfo>::value_type& elem) {
                  return elem.second;
                });
      sort(values.begin(), values.end(),
           [](const TableBalanceInfo& lhs, const TableBalanceInfo& rhs) {
             return lhs.table_id < rhs.table_id;
           });
      return values;
    };

    vector<TableBalanceInfo> lhs_values = getValues(eq_range_lhs);
    vector<TableBalanceInfo> rhs_values = getValues(eq_range_rhs);
    if (lhs_values != rhs_values) {
      return false;
    }

    // Advance the iterators to continue with next key.
    it_lhs = eq_range_lhs.second;
    it_rhs = eq_range_rhs.second;
  }

  return true;
}

bool operator==(const ClusterBalanceInfo& lhs, const ClusterBalanceInfo& rhs) {
  return
      HasSameContents(lhs.table_info_by_skew, rhs.table_info_by_skew) &&
      HasSameContents(lhs.servers_by_total_replica_count,
                      rhs.servers_by_total_replica_count);
}

ostream& operator<<(ostream& s, const ClusterBalanceInfo& info) {
  s << "[";
  for (const auto& elem : info.servers_by_total_replica_count) {
    s << " " << elem.first << ":" << elem.second;
  }
  s << " ]; [";
  for (const auto& elem : info.table_info_by_skew) {
    s << " " << elem.first << ":{ " << elem.second.table_id
      << " [";
    for (const auto& e : elem.second.servers_by_replica_count) {
      s << " " << e.first << ":" << e.second;
    }
    s << " ] }";
  }
  s << " ]";
  return s;
}

class KsckResultsToClusterBalanceInfoTest : public ::testing::Test {
 protected:
  void RunTest(const Rebalancer::Config& rebalancer_cfg,
               const vector<KsckResultsTestConfig>& test_configs) {
    for (auto idx = 0; idx < test_configs.size(); ++idx) {
      SCOPED_TRACE(Substitute("test config index: $0", idx));
      const auto& cfg = test_configs[idx];
      auto raw_info = GenerateRawClusterInfo(cfg.input);

      Rebalancer rebalancer(rebalancer_cfg);
      ClusterInfo ci;
      ASSERT_OK(rebalancer.BuildClusterInfo(
          raw_info, Rebalancer::MovesInProgress(), &ci));

      ASSERT_EQ(cfg.ref_balance_info, ci.balance);
    }
  }
};

// Test converting KsckResults result into ClusterBalanceInfo when movement
// of RF=1 replicas is allowed.
TEST_F(KsckResultsToClusterBalanceInfoTest, MoveRf1Replicas) {
  const Rebalancer::Config rebalancer_config = {
    {},     // blacklist_tservers
    {},     // ignored_tservers
    {},     // master_addresses
    {},     // table_filters
    5,      // max_moves_per_server
    30,     // max_staleness_interval_sec
    0,      // max_run_time_sec
    true,   // move_rf1_replicas
  };

  const vector<KsckResultsTestConfig> test_configs = {
    // Empty
    {
      {},
      {}
    },
    // One tserver, one table, one tablet, RF=1.
    {
      {
        { { "ts_0" }, },
        { { "tablet_0", "table_a", { { "ts_0", true }, }, }, },
        { { "table_a", 1 }, },
      },
      {
        { { 0, { "table_a", { { 1, "ts_0" }, } } }, },
        { { 1, "ts_0" }, },
      }
    },
    // Balanced configuration: three tservers, one table, one tablet, RF=3.
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a0", "table_a", { { "ts_0", true }, }, },
          { "tablet_a0", "table_a", { { "ts_1", true }, }, },
          { "tablet_a0", "table_a", { { "ts_2", true }, }, },
        },
        { { "table_a", 3 } },
      },
      {
        {
          { 0, { "table_a", {
                { 1, "ts_2" },
                { 1, "ts_1" },
                { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
        },
      }
    },
    // Simple unbalanced configuration.
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a_0", "table_a", { { "ts_0", true }, }, },
          { "tablet_b_0", "table_b", { { "ts_0", true }, }, },
          { "tablet_c_0", "table_c", { { "ts_0", true }, }, },
        },
        { { { "table_a", 1 }, { "table_b", 1 }, { "table_c", 1 }, } },
      },
      {
        {
          { 1, { "table_c", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
          { 1, { "table_b", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
          { 1, { "table_a", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 0, "ts_2" }, { 0, "ts_1" }, { 3, "ts_0" },
        },
      }
    },
    // table_a: 1 tablet with RF=3
    // table_b: 3 tablets with RF=1
    // table_c: 2 tablets with RF=1
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a_0", "table_a", { { "ts_0", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_1", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_2", true }, }, },
          { "tablet_b_0", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_1", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_2", "table_b", { { "ts_0", true }, }, },
          { "tablet_c_0", "table_c", { { "ts_1", true }, }, },
          { "tablet_c_1", "table_c", { { "ts_1", true }, }, },
        },
        { { { "table_a", 3 }, { "table_b", 1 }, { "table_c", 1 }, } },
      },
      {
        {
          { 2, { "table_c", {
                { 0, "ts_0" }, { 0, "ts_2" }, { 2, "ts_1" },
              }
            }
          },
          { 3, { "table_b", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 3, "ts_0" },
              }
            }
          },
          { 0, { "table_a", {
                { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 1, "ts_2" }, { 3, "ts_1" }, { 4, "ts_0" },
        },
      }
    },
  };

  NO_FATALS(RunTest(rebalancer_config, test_configs));
}

// Test converting KsckResults result into ClusterBalanceInfo when movement of
// RF=1 replicas is disabled.
TEST_F(KsckResultsToClusterBalanceInfoTest, DoNotMoveRf1Replicas) {
  const Rebalancer::Config rebalancer_config = {
    {},     // blacklist_tservers
    {},     // ignored_tservers
    {},     // master_addresses
    {},     // table_filters
    5,      // max_moves_per_server
    30,     // max_staleness_interval_sec
    0,      // max_run_time_sec
    false,  // move_rf1_replicas
  };

  const vector<KsckResultsTestConfig> test_configs = {
    // Empty
    {
      {},
      {}
    },
    // One tserver, one table, one tablet, RF=1.
    {
      {
        { { "ts_0" }, },
        { { "tablet_0", "table_a", { { "ts_0", true }, }, }, },
        { { "table_a", 1 }, },
      },
      {
        {},
        { { 0, "ts_0" }, }
      }
    },
    // Two tservers, two tables, RF=1.
    {
      {
        { { "ts_0" }, { "ts_1" }, },
        {
          { "tablet_a0", "table_a", { { "ts_0", true }, }, },
          { "tablet_b0", "table_b", { { "ts_0", true }, }, },
          { "tablet_b1", "table_b", { { "ts_1", true }, }, },
        },
        { { "table_a", 1 }, { "table_b", 1 } },
      },
      {
        {},
        { { 0, "ts_1" }, { 0, "ts_0" }, }
      }
    },
    // table_a: 1 tablet with RF=3
    // table_b: 3 tablets with RF=1
    // table_c: 2 tablets with RF=1
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a_0", "table_a", { { "ts_0", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_1", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_2", true }, }, },
          { "tablet_b_0", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_1", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_2", "table_b", { { "ts_0", true }, }, },
          { "tablet_c_0", "table_c", { { "ts_1", true }, }, },
          { "tablet_c_1", "table_c", { { "ts_1", true }, }, },
        },
        { { { "table_a", 3 }, { "table_b", 1 }, { "table_c", 1 }, } },
      },
      {
        {
          {
            0, {
              "table_a", {
                { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
        },
      }
    },
  };

  NO_FATALS(RunTest(rebalancer_config, test_configs));
}

} // namespace rebalance
} // namespace kudu
