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
#include "kudu/master/auto_leader_rebalancer.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"// IWYU pragma: keep
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h" // IWYU pragma: keep
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {
class AutoRebalancerTask;
}  // namespace master
}  // namespace kudu

using kudu::client::KuduTable;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::MiniTabletServer;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::rpc::RpcController;
using std::string;
using std::unique_ptr;
using kudu::client::sp::shared_ptr;
using std::vector;

DECLARE_bool(auto_leader_rebalancing_enabled);
DECLARE_bool(auto_rebalancing_enabled);
DECLARE_bool(leader_rebalancing_ignore_soft_deleted_tables);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_uint32(auto_leader_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_max_moves_per_server);
DECLARE_uint32(auto_rebalancing_wait_for_replica_moves_seconds);
DECLARE_uint32(leader_rebalancing_max_moves_per_round);

namespace kudu {
namespace master {

enum class BalanceThreadType { REPLICA_REBALANCE, LEADER_REBALANCE };

class LeaderRebalancerTest : public KuduTest {
 public:
  Status CreateAndStartCluster() {
    // Disable replica rebalancing, we'll do it manually
    FLAGS_auto_rebalancing_enabled = true;
    FLAGS_auto_rebalancing_interval_seconds = 1;                // Shorten for testing.
    FLAGS_auto_rebalancing_wait_for_replica_moves_seconds = 0;  // Shorten for testing.
    // Disable leader rebalancing, we'll do it manually.
    FLAGS_auto_leader_rebalancing_enabled = false;
    cluster_.reset(new InternalMiniCluster(env_, cluster_opts_));
    return cluster_->Start();
  }

  void CreateWorkloadTable(int num_tablets, int num_replicas) {
    workload_.reset(new TestWorkload(cluster_.get()));
    workload_->set_num_tablets(num_tablets);
    workload_->set_num_replicas(num_replicas);
    workload_->Setup();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  std::string table_name() { return workload_->table_name(); }

  Status RunLeaderRebalanceForTable(
      const scoped_refptr<TableInfo>& table_info,
      const vector<string>& tserver_uuids,
      std::unordered_map<string, int>* global_leader_count) {
    master::Master* master = cluster_->mini_master()->master();
    return master->catalog_manager()->auto_leader_rebalancer()
        ->RunLeaderRebalanceForTable(table_info, tserver_uuids, {}, global_leader_count);
  }

  Status CheckLeaderBalance() {
    // Leader master
    master::Master* master = cluster_->mini_master()->master();
    master::CatalogManager* catalog_manager = master->catalog_manager();
    scoped_refptr<master::TableInfo> table_info;
    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager);
      catalog_manager->GetTableInfoByName(table_name(), &table_info);
    }

    TSDescriptorVector descriptors;
    master->ts_manager()->GetAllDescriptors(&descriptors);

    std::vector<std::string> tserver_uuids;
    for (const auto& e : descriptors) {
      if (e->PresumedDead()) {
        continue;
      }
      tserver_uuids.emplace_back(e->permanent_uuid());
    }

    return catalog_manager->auto_leader_rebalancer()->RunLeaderRebalanceForTable(
        table_info, tserver_uuids, {}, nullptr, AutoLeaderRebalancerTask::ExecuteMode::TEST);
  }

  // Get the leader numbers of each tablet server.
  Status GetLeaderDistribution(std::map<string, int32_t>* leader_map,
                             const string& table_name) {
    leader_map->clear();
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(workload_->client()->OpenTable(table_name, &table));

    scoped_refptr<TableInfo> table_info;
    master::Master* master = cluster_->mini_master()->master();
    master::CatalogManager* catalog_manager = master->catalog_manager();
    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager);
      RETURN_NOT_OK(catalog_manager->GetTableInfo(table->id(), &table_info));
    }
    std::vector<string> leader_list;
    for (const auto& tablet : table_info->tablet_map()) {
      client::KuduTablet* ptr;
      RETURN_NOT_OK(workload_->client()->GetTablet(tablet.second->id(), &ptr));
      unique_ptr<client::KuduTablet> tablet_ptr(ptr);
      for (const auto* replica : tablet_ptr->replicas()) {
        if (replica->is_leader()) {
          leader_list.push_back(replica->ts().uuid());
        }
      }
    }
    TSDescriptorVector descriptors;
    master->ts_manager()->GetAllDescriptors(&descriptors);
    for (const auto& e : descriptors) {
      if (e->PresumedDead()) {
        continue;
      }
      leader_map->emplace(e->permanent_uuid(), count(
          leader_list.begin(), leader_list.end(), e->permanent_uuid()));
    }

    return Status::OK();
  }

  // Make the leader distribution as the vector passed in.
  Status MakeLeaderDistribution(std::vector<int32_t> leader_distribution,
                                const string table_name) {
    master::Master* master = cluster_->mini_master()->master();
    TSDescriptorVector descriptors;
    master->ts_manager()->GetAllDescriptors(&descriptors);
    if (descriptors.size() != leader_distribution.size()) {
      return Status::IllegalState("The size of leader distribution vector should "
                                  "be the number of tablet servers.");
    }

    scoped_refptr<TableInfo> table;
    master::CatalogManager* catalog_manager = master->catalog_manager();
    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager);
      catalog_manager->GetTableInfoByName(table_name, &table);
    }

    if (std::accumulate(leader_distribution.begin(), leader_distribution.end(), 0) !=
        table->num_tablets()) {
      return Status::IllegalState("The sum of leader distribution should "
                                  "be the tablet number of the table.");
    }

    int32_t index = 0;
    int32_t tmp_distribution = 0;
    // Skip any leading tablet servers that should hold no leaders.
    while (index < static_cast<int32_t>(leader_distribution.size()) &&
           leader_distribution.at(index) == 0) {
      index++;
    }
    MiniTabletServer* tserver = cluster_->mini_tablet_server(index);
    for (const auto& tablet : table->tablet_map()) {
      if (tmp_distribution >= leader_distribution.at(index)) {
        // Advance to the next tablet server that should hold at least one
        // leader, skipping any that should hold none. This lets the
        // distribution vector contain interior zeros (e.g. {1, 0, 1}).
        do {
          index++;
        } while (index < static_cast<int32_t>(leader_distribution.size()) &&
                 leader_distribution.at(index) == 0);
        tmp_distribution = 0;
        tserver = cluster_->mini_tablet_server(index);
      }
      unique_ptr<client::KuduTablet> tablet_copy;
      {
        client::KuduTablet* ptr;
        RETURN_NOT_OK(workload_->client()->GetTablet(tablet.second->id(), &ptr));
        tablet_copy.reset(ptr);
      }
      for (const auto* replica: tablet_copy->replicas()) {
        if (replica->is_leader()) {
          if (replica->ts().uuid() == tserver->uuid()) {
            break;
          }
          LeaderStepDownRequestPB req;
          req.set_dest_uuid(replica->ts().uuid());
          req.set_tablet_id(tablet.second->id());
          req.set_new_leader_uuid(tserver->uuid());
          req.set_mode(consensus::GRACEFUL);
          LeaderStepDownResponsePB resp;
          RpcController rpc;
          RETURN_NOT_OK(cluster_->tserver_consensus_proxy(cluster_
                        ->tablet_server_index_by_uuid(replica->ts().uuid()))
                        ->LeaderStepDown(req, &resp, &rpc));
          break;
        }
      }
      tmp_distribution++;
    }
    return Status::OK();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  InternalMiniClusterOptions cluster_opts_;
  unique_ptr<TestWorkload> workload_;
};

// Verify if the leader rebalancing is able to balance the leaders in various
// workloads.
// We need to make sure that the function RunLeaderRebalanceForTable is
// correct. After that we could use it to check leader balance by passing
// TEST mode.
TEST_F(LeaderRebalancerTest, FunctionalTestForDivided) {
  const int kNumTServers = 3;
  const int kNumTablets = 9;
  cluster_opts_.num_tablet_servers = kNumTServers;

  ASSERT_OK(CreateAndStartCluster());
  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Simulate the leader distribution.
  std::vector<int32_t> leader_distribution = {4, 4, 1};
  ASSERT_OK(MakeLeaderDistribution(leader_distribution, table_name()));

  SleepFor(MonoDelta::FromMilliseconds(3000));
  std::map<string, int32_t> leader_map;
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }

  // Try to do rebalance 10 times.
  master::Master* master = cluster_->mini_master()->master();
  int32_t retries = 10;
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  // Check the leader numbers of each tablet server. It should always be floor(avg)
  // or ceil(avg), where the parameter avg is (tablet num) / (tablet server num).
  double expected_leader_num = static_cast<double>(kNumTablets) / 3;
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }
  for (const auto& leader: leader_map) {
    ASSERT_GE(leader.second, std::floor(expected_leader_num));
    ASSERT_LE(leader.second, std::ceil(expected_leader_num));
  }

  // Try different leader distribution.
  std::vector<int32_t> leader_distribution2 = {0, 8, 1};
  ASSERT_OK(MakeLeaderDistribution(leader_distribution2, table_name()));

  SleepFor(MonoDelta::FromMilliseconds(3000));
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }

  for (int i = 0; i < retries; i++) {
    SCOPED_TRACE(strings::Substitute("try $0", i));
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }
  for (const auto& leader: leader_map) {
    ASSERT_GE(leader.second, std::floor(expected_leader_num));
    ASSERT_LE(leader.second, std::ceil(expected_leader_num));
  }
}

TEST_F(LeaderRebalancerTest, FunctionalTestForNotDivided) {
  const int kNumTServers = 3;
  const int kNumTablets = 10;
  cluster_opts_.num_tablet_servers = kNumTServers;

  ASSERT_OK(CreateAndStartCluster());
  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Simulate the leader distribution.
  std::vector<int32_t> leader_distribution = {5, 4, 1};
  ASSERT_OK(MakeLeaderDistribution(leader_distribution, table_name()));

  SleepFor(MonoDelta::FromMilliseconds(3000));
  std::map<string, int32_t> leader_map;
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }

  // Try to do rebalance 10 times.
  master::Master* master = cluster_->mini_master()->master();
  int32_t retries = 10;
  master::AutoLeaderRebalancerTask* leader_rebalancer =
    master->catalog_manager()->auto_leader_rebalancer();
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  // Check the leader numbers of each tablet server. It should always be floor(avg)
  // or ceil(avg), where the parameter avg is (tablet num) / (tablet server num).
  double expected_leader_num = static_cast<double>(kNumTablets) / 3;
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }
  for (const auto& leader: leader_map) {
    ASSERT_GE(leader.second, std::floor(expected_leader_num));
    ASSERT_LE(leader.second, std::ceil(expected_leader_num));
  }

  // Try different leader distribution.
  std::vector<int32_t> leader_distribution2 = {8, 1, 1};
  ASSERT_OK(MakeLeaderDistribution(leader_distribution2, table_name()));

  SleepFor(MonoDelta::FromMilliseconds(3000));
  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }

  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  ASSERT_OK(GetLeaderDistribution(&leader_map, table_name()));
  LOG(INFO) << "The leader distribution is " << '\n';
  for (const auto& leader : leader_map) {
    std::cout << leader.first << "  " << leader.second << '\n';
  }
  for (const auto& leader: leader_map) {
    ASSERT_GE(leader.second, std::floor(expected_leader_num));
    ASSERT_LE(leader.second, std::ceil(expected_leader_num));
  }
}

// Create a cluster, and create a table,
// whether tablets is balanced, which decided by creating table process.
// Bring up another tserver, the table is not balanced and leaders is also
// not balanced. We verify that moves are scheduled,
// since the cluster is no longer balanced.
TEST_F(LeaderRebalancerTest, AddTserver) {
  const int kNumTServers = 3;
  const int kNumTablets = 59;

  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Add a tablet server and verify that the master schedules some moves, and
  // the tablet servers copy bytes as appropriate.
  ASSERT_OK(cluster_->AddTabletServer());
  int tserver_size = cluster_->num_tablet_servers();
  LOG(INFO) << "add a tserver: " << cluster_->mini_tablet_server(tserver_size - 1)->uuid();

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer = master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  // To wait replica_rebalancer execute some runs and reach balanced.
  SleepFor(MonoDelta::FromSeconds(20 * FLAGS_auto_rebalancing_interval_seconds));
  constexpr const int32_t retries = 40;
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  ASSERT_OK(CheckLeaderBalance());
}

TEST_F(LeaderRebalancerTest, RestartTserver) {
  const int kNumTServers = 4;
  const int kNumTablets = 59;
  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer = master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  // To wait replica_rebalancer execute some runs and reach balanced.
  SleepFor(MonoDelta::FromSeconds(10 * FLAGS_auto_rebalancing_interval_seconds));
  constexpr const int32_t retries = 20;
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }
  ASSERT_OK(CheckLeaderBalance());
}

TEST_F(LeaderRebalancerTest, TestMaintenanceMode) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr const int kNumTServers = 3;
  constexpr const int kNumTablets = 10;
  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(leader_rebalancer, nullptr);

  constexpr const int kCurrentTserverIndex = 0;
  tserver::MiniTabletServer* mini_tserver = cluster_->mini_tablet_server(kCurrentTserverIndex);
  // Sets the tserver state for a tserver to 'MAINTENANCE_MODE'.
  ASSERT_OK(
      master->ts_manager()->SetTServerState(mini_tserver->uuid(),
                                            TServerStatePB::MAINTENANCE_MODE,
                                            ChangeTServerStateRequestPB::ALLOW_MISSING_TSERVER,
                                            master->catalog_manager()->sys_catalog()));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE,
            master->ts_manager()->GetTServerState(mini_tserver->uuid()));
  // Restart the tserver to force transferring all leaders on it to make leaders not balanced.
  mini_tserver->Shutdown();
  SleepFor(MonoDelta::FromMilliseconds(5 * FLAGS_heartbeat_interval_ms));
  ASSERT_OK(mini_tserver->Start());

  // Try to run some 'leader rebalance' iterations. If mini_tserver is not in MAINTENANCE_MODE,
  // it's enough to reach leader balanced, more tries is not necessary and less tries
  // may not reach leader rebalanced.
  constexpr const int32_t retries = std::max(kNumTablets / 2, 3);
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }
  // This cluster cannot reach the state of rebalanced leadership distribution since 1 of the 3
  // tservers is in maintenance mode.
  Status status = CheckLeaderBalance();
  ASSERT_TRUE(status.IsIllegalState()) << status.ToString();

  {
    // The tserver in maintenance mode should have no leaders.
    std::shared_ptr<tserver::TabletServerServiceProxy> proxy =
        cluster_->tserver_proxy(kCurrentTserverIndex);
    tserver::ListTabletsRequestPB req;
    ListTabletsResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
    ASSERT_OK(proxy->ListTablets(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_FALSE(resp.status_and_schema().empty());
    for (const auto& replica : resp.status_and_schema()) {
      ASSERT_NE(consensus::RaftPeerPB::LEADER, replica.role());
    }
  }
}

// Check that the global tie breaker in RunLeaderRebalanceForTable prefers a
// tserver with fewer leaders overall when two candidates are equally good for
// the table being balanced.
//
// Setup: two tables, each with 2 tablets and RF=3 on a 3 tserver cluster, so
// every tserver holds a replica of every tablet.
//   - table2 puts both of its leaders on ts0 ({2,0,0}). ts0 is over the target
//     for this table (ceil(2/3)=1) and has to give up one leader. The two
//     places it could go, ts1 and ts2, each hold 0 of their 2 replicas as
//     leaders, so they are equally good for table2.
//   - table1 is already balanced, but we put its second leader on whichever of
//     those two has the smaller uuid, so that tserver ends up with more leaders
//     overall.
//
// We rebalance table1 first to fill in the global counts, then rebalance
// table2. Since the two destinations are tied for table2, the choice comes down
// to the overall counts: the tserver with the larger uuid (0 leaders so far)
// should win over the one with the smaller uuid (1 leader).
//
// This is deterministic either way. With the tie breaker the leader lands on
// the larger uuid tserver. Without it the tie falls through to the uuid
// comparison, which always picks the smaller uuid tserver (the loaded one), and
// the check below fails. So the test actually guards against the tie breaker
// being removed, rather than passing or failing depending on the order the
// replicas happen to come back in.
TEST_F(LeaderRebalancerTest, MultiTableLeaderBalance) {
  const int kNumTServers = 3;
  const int kNumTablets = 2;

  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());

  const string kTable1Name = "multi_table_leader_balance_table1";
  workload_.reset(new TestWorkload(cluster_.get()));
  workload_->set_table_name(kTable1Name);
  workload_->set_num_tablets(kNumTablets);
  workload_->set_num_replicas(3);
  workload_->Setup();

  const string kTable2Name = "multi_table_leader_balance_table2";
  workload_.reset(new TestWorkload(cluster_.get()));
  workload_->set_table_name(kTable2Name);
  workload_->set_num_tablets(kNumTablets);
  workload_->set_num_replicas(3);
  workload_->Setup();

  // ts0 holds both of table2's leaders and is the source of the move. ts1 and
  // ts2 are the two tied destinations. We want the one with the smaller global
  // count to be the tserver with the larger uuid.
  const string ts0_uuid = cluster_->mini_tablet_server(0)->uuid();
  const string ts1_uuid = cluster_->mini_tablet_server(1)->uuid();
  const string ts2_uuid = cluster_->mini_tablet_server(2)->uuid();
  const string& lo_uuid = std::min(ts1_uuid, ts2_uuid);
  const string& hi_uuid = std::max(ts1_uuid, ts2_uuid);

  // table1 is balanced ({1,1,0} in some order), but we put its second leader on
  // the smaller uuid tserver, so once table1 is counted the global totals are
  // lo_uuid=1 and hi_uuid=0.
  std::vector<int32_t> table1_dist(kNumTServers, 0);
  table1_dist[0] = 1;
  if (lo_uuid == ts1_uuid) {
    table1_dist[1] = 1;
  } else {
    table1_dist[2] = 1;
  }
  ASSERT_OK(MakeLeaderDistribution(table1_dist, kTable1Name));
  // table2 keeps all its leaders on ts0, which forces exactly one move.
  ASSERT_OK(MakeLeaderDistribution({kNumTablets, 0, 0}, kTable2Name));

  using LeaderMap = std::map<string, int32_t>;

  // Wait for the leadership moves from MakeLeaderDistribution to settle.
  ASSERT_EVENTUALLY([&] {
    LeaderMap dist1;
    LeaderMap dist2;
    ASSERT_OK(GetLeaderDistribution(&dist1, kTable1Name));
    ASSERT_OK(GetLeaderDistribution(&dist2, kTable2Name));
    ASSERT_EQ(1, dist1.at(ts0_uuid));
    ASSERT_EQ(1, dist1.at(lo_uuid));
    ASSERT_EQ(0, dist1.at(hi_uuid));
    ASSERT_EQ(kNumTablets, dist2.at(ts0_uuid));
  });

  master::Master* master = cluster_->mini_master()->master();

  TSDescriptorVector descriptors;
  master->ts_manager()->GetAllDescriptors(&descriptors);
  vector<string> tserver_uuids;
  for (const auto& e : descriptors) {
    if (!e->PresumedDead()) {
      tserver_uuids.emplace_back(e->permanent_uuid());
    }
  }

  // Start the global map at 0 for every tserver, just like RunLeaderRebalancer.
  std::unordered_map<string, int> global_leader_count;
  for (const auto& uuid : tserver_uuids) {
    global_leader_count[uuid] = 0;
  }

  scoped_refptr<TableInfo> table1_info;
  scoped_refptr<TableInfo> table2_info;
  {
    CatalogManager::ScopedLeaderSharedLock leaderlock(master->catalog_manager());
    master->catalog_manager()->GetTableInfoByName(kTable1Name, &table1_info);
    master->catalog_manager()->GetTableInfoByName(kTable2Name, &table2_info);
  }

  // Rebalance table1 first. Nothing moves (it is already balanced), but its
  // leader counts get added to the global map (ts0+1, lo_uuid+1, hi_uuid+0),
  // which sets up the imbalance the tie breaker needs.
  ASSERT_OK(RunLeaderRebalanceForTable(table1_info, tserver_uuids, &global_leader_count));
  ASSERT_EQ(1, global_leader_count.at(lo_uuid));
  ASSERT_EQ(0, global_leader_count.at(hi_uuid));

  // Now rebalance table2. ts0 has 2 leaders but the target is ceil(2/3)=1, so it
  // sheds one. lo_uuid and hi_uuid are tied for table2 (0 of 2 each), but their
  // global counts differ (1 vs 0), so the leader should go to hi_uuid.
  ASSERT_OK(RunLeaderRebalanceForTable(table2_info, tserver_uuids, &global_leader_count));

  // Wait for table2's move to settle, then check that hi_uuid (not lo_uuid) got
  // the leader. Without the tie breaker the tie falls back to the smaller uuid
  // (lo_uuid, the one we loaded earlier), leaving the cluster skewed overall.
  AssertEventually([&] {
    LeaderMap dist2;
    ASSERT_OK(GetLeaderDistribution(&dist2, kTable2Name));
    ASSERT_EQ(1, dist2.at(ts0_uuid));
    ASSERT_EQ(1, dist2.at(hi_uuid));
    ASSERT_EQ(0, dist2.at(lo_uuid));
  }, MonoDelta::FromSeconds(120));
  NO_PENDING_FATALS();
}

class FilterSoftDeletedTableTest :
    public LeaderRebalancerTest,
    public ::testing::WithParamInterface<bool> {
};

INSTANTIATE_TEST_SUITE_P(, FilterSoftDeletedTableTest, ::testing::Bool());
TEST_P(FilterSoftDeletedTableTest, TestFilterSofteDeletedTable) {
  FLAGS_leader_rebalancing_ignore_soft_deleted_tables = GetParam();

  constexpr const int kNumTServers = 3;
  constexpr const int kNumTablets = 9;
  constexpr const int kNumReplicas = 3;
  constexpr const char* const soft_deleted_table = "soft_deleted_table";

  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ kNumReplicas);

  // Simulate the leader distribution.
  std::vector<int32_t> leader_distribution = {4, 4, 1};
  ASSERT_OK(MakeLeaderDistribution(leader_distribution, table_name()));
  SleepFor(MonoDelta::FromMilliseconds(3000));

  string first_table = table_name();

  // Create a new table.
  workload_.reset(new TestWorkload(cluster_.get()));
  workload_->set_table_name(soft_deleted_table);
  workload_->set_num_tablets(kNumTablets);
  workload_->set_num_replicas(kNumReplicas);
  workload_->Setup();

  // Simulate the leader distribution.
  ASSERT_OK(MakeLeaderDistribution(leader_distribution, soft_deleted_table));
  SleepFor(MonoDelta::FromMilliseconds(3000));

  // Delete the table 'soft_deleted_table'.
  ASSERT_OK(workload_->client()->SoftDeleteTable(soft_deleted_table, 3600));

  // Try to run leader rebalance for 10 times.
  int32_t retries = 10;
  master::Master* master = cluster_->mini_master()->master();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  std::map<string, int32_t> leader_map;
  // The first table is leader rebalanced.
  ASSERT_OK(GetLeaderDistribution(&leader_map, first_table));
  for (const auto& leader: leader_map) {
    ASSERT_EQ(leader.second, 3);
  }

  ASSERT_OK(GetLeaderDistribution(&leader_map, soft_deleted_table));
  // The soft deleted table is not leader rebalanced.
  if (FLAGS_leader_rebalancing_ignore_soft_deleted_tables) {
    for (const auto& leader: leader_map) {
      ASSERT_NE(leader.second, 3);
    }
  } else {
    for (const auto& leader: leader_map) {
      ASSERT_EQ(leader.second, 3);
    }
  }
}

}  // namespace master
}  // namespace kudu
