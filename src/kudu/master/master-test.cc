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

#include "kudu/master/master.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/generated/version_defines.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_options.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/table_metrics.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/monitored_task.h"
#include "kudu/server/rpc_server.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/atomic.h"
#include "kudu/util/cache.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/version_info.h"

using boost::none;
using boost::optional;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::itest::GetClusterId;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::accumulate;
using std::map;
using std::multiset;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(enable_deleted_tables_and_tablets_cleanup);
DECLARE_bool(enable_per_range_hash_schemas);
DECLARE_bool(master_client_location_assignment_enabled);
DECLARE_bool(master_support_authz_tokens);
DECLARE_bool(mock_table_metrics_for_testing);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_double(sys_catalog_fail_during_write);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(default_num_replicas);
DECLARE_int32(deleted_table_and_tablet_reserved_secs);
DECLARE_int32(diagnostics_log_stack_traces_interval_ms);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(flush_upper_bound_ms);
DECLARE_int32(master_inject_latency_on_tablet_lookups_ms);
DECLARE_int32(max_table_comment_length);
DECLARE_int32(rpc_service_queue_length);
DECLARE_int64(live_row_count_for_testing);
DECLARE_int64(on_disk_size_for_testing);
DECLARE_string(location_mapping_cmd);
DECLARE_string(log_filename);

METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableSchema);

namespace kudu {
namespace master {

class MasterTest : public KuduTest {
 protected:
  void SetUp() override {
    KuduTest::SetUp();

    // In this test, we create tables to test catalog manager behavior,
    // but we have no tablet servers. Typically this would be disallowed.
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;

    // Start master
    mini_master_.reset(new MiniMaster(GetTestPath("Master"), HostPort("127.0.0.1", 0)));
    ASSERT_OK(mini_master_->Start());
    master_ = mini_master_->master();
    ASSERT_OK(master_->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
        MonoDelta::FromSeconds(90)));

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                        mini_master_->bound_rpc_addr().host()));
  }

  void TearDown() override {
    mini_master_->Shutdown();
    KuduTest::TearDown();
  }

  struct HashDimension {
    vector<string> columns;
    int32_t num_buckets;
    uint32_t seed;
  };
  typedef vector<HashDimension> HashSchema;

  void DoListTables(const ListTablesRequestPB& req, ListTablesResponsePB* resp);
  void DoListAllTables(ListTablesResponsePB* resp);

  Status CreateTable(const string& name,
                     const Schema& schema,
                     const optional<TableTypePB>& type = none,
                     const optional<string>& owner = none,
                     const optional<string>& comment = none);

  Status CreateTable(const string& name,
                     const Schema& schema,
                     const vector<KuduPartialRow>& split_rows,
                     const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds = {},
                     const vector<HashSchema>& range_hash_schemas = {});

  Status CreateTable(const string& name,
                     const Schema& schema,
                     const optional<TableTypePB>& type,
                     const optional<string>& owner,
                     const optional<string>& comment,
                     const vector<KuduPartialRow>& split_rows,
                     const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds,
                     const vector<HashSchema>& range_hash_schemas);

  shared_ptr<Messenger> client_messenger_;
  unique_ptr<MiniMaster> mini_master_;
  Master* master_;
  unique_ptr<MasterServiceProxy> proxy_;
};

Status MasterTest::CreateTable(const string& name,
                               const Schema& schema,
                               const optional<TableTypePB>& type,
                               const optional<string>& owner,
                               const optional<string>& comment) {
  KuduPartialRow split1(&schema);
  RETURN_NOT_OK(split1.SetInt32("key", 10));

  KuduPartialRow split2(&schema);
  RETURN_NOT_OK(split2.SetInt32("key", 20));

  return CreateTable(
      name, schema, type, owner, comment, { split1, split2 }, {}, {});
}

Status MasterTest::CreateTable(
    const string& name,
    const Schema& schema,
    const vector<KuduPartialRow>& split_rows,
    const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds,
    const vector<HashSchema>& range_hash_schemas) {
  return CreateTable(
        name, schema, none, none, none, split_rows, bounds, range_hash_schemas);
}

Status MasterTest::CreateTable(
    const string& name,
    const Schema& schema,
    const optional<TableTypePB>& type,
    const optional<string>& owner,
    const optional<string>& comment,
    const vector<KuduPartialRow>& split_rows,
    const vector<pair<KuduPartialRow, KuduPartialRow>>& bounds,
    const vector<HashSchema>& range_hash_schemas) {

  if (!range_hash_schemas.empty() && range_hash_schemas.size() != bounds.size()) {
    return Status::InvalidArgument(
        "'bounds' and 'range_hash_schemas' must be of the same size");
  }

  CreateTableRequestPB req;
  req.set_name(name);
  if (type) {
    req.set_table_type(*type);
  }
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  RowOperationsPBEncoder splits_encoder(req.mutable_split_rows_range_bounds());
  for (const KuduPartialRow& row : split_rows) {
    splits_encoder.Add(RowOperationsPB::SPLIT_ROW, row);
  }
  for (const pair<KuduPartialRow, KuduPartialRow>& bound : bounds) {
    splits_encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    splits_encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
  }

  auto* ps_pb = req.mutable_partition_schema();
  for (size_t i = 0; i < range_hash_schemas.size(); ++i) {
    auto* range = ps_pb->add_custom_hash_schema_ranges();
    RowOperationsPBEncoder encoder(range->mutable_range_bounds());
    const auto& bound = bounds[i];
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, bound.first);
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, bound.second);
    const auto& range_hash_schema = range_hash_schemas[i];
    for (const auto& hash_dimension : range_hash_schema) {
      auto* hash_dimension_pb = range->add_hash_schema();
      for (const string& col_name : hash_dimension.columns) {
        hash_dimension_pb->add_columns()->set_name(col_name);
      }
      hash_dimension_pb->set_num_buckets(hash_dimension.num_buckets);
      hash_dimension_pb->set_seed(hash_dimension.seed);
    }
  }

  if (owner) {
    req.set_owner(*owner);
  }
  if (comment) {
    req.set_comment(*comment);
  }
  RpcController controller;
  if (!bounds.empty()) {
    controller.RequireServerFeature(MasterFeatures::RANGE_PARTITION_BOUNDS);
  }

  CreateTableResponsePB resp;
  RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &controller));
  if (resp.has_error()) {
    RETURN_NOT_OK(StatusFromPB(resp.error().status()));
  }
  return Status::OK();
}

void MasterTest::DoListTables(const ListTablesRequestPB& req, ListTablesResponsePB* resp) {
  RpcController controller;
  ASSERT_OK(proxy_->ListTables(req, resp, &controller));
  SCOPED_TRACE(SecureDebugString(*resp));
  ASSERT_FALSE(resp->has_error());
}

void MasterTest::DoListAllTables(ListTablesResponsePB* resp) {
  ListTablesRequestPB req;
  DoListTables(req, resp);
}

static void MakeHostPortPB(const string& host, uint32_t port, HostPortPB* pb) {
  pb->set_host(host);
  pb->set_port(port);
}

TEST_F(MasterTest, TestPingServer) {
  // Ping the server.
  PingRequestPB req;
  PingResponsePB resp;
  RpcController controller;
  ASSERT_OK(proxy_->Ping(req, &resp, &controller));
}

// Test that shutting down a MiniMaster without starting it does not
// SEGV.
TEST_F(MasterTest, TestShutdownWithoutStart) {
  MiniMaster m("/xxxx", HostPort("127.0.0.1", 0));
  m.Shutdown();
}

// Test that ensures that, when specified, restarting a master from within the
// same process can correctly instantiate proper block cache metrics.
TEST_F(MasterTest, TestResetBlockCacheMetricsInSameProcess) {
  mini_master_->Shutdown();
  // Make sure we flush quickly so we start using the block cache ASAP.
  FLAGS_flush_threshold_mb = 1;
  FLAGS_flush_threshold_secs = 1;
  FLAGS_flush_upper_bound_ms = 0;

  // If implemented incorrectly, since the BlockCache is a singleton, we could
  // end up with incorrect block cache metrics upon resetting the master
  // because we register metrics twice. Ensure that's not the case when we
  // supply the option to reset metrics.
  mini_master_->mutable_options()->set_block_cache_metrics_policy(
      Cache::ExistingMetricsPolicy::kReset);
  ASSERT_OK(mini_master_->Restart());

  // Keep on creating tables until we flush, at which point we'll use the block
  // cache and increment metrics.
  // NOTE: we could examine the master's metric_entity() directly, but that
  // could itself interfere with the reported metrics, e.g. by calling
  // FindOrCreateCount(). Going through the web UI is more organic anyway.
  const Schema kTableSchema({ ColumnSchema("key", INT32), ColumnSchema("v", STRING) }, 1);
  constexpr const char* kTablePrefix = "testtb";
  EasyCurl c;
  int i = 0;
  ASSERT_EVENTUALLY([&] {
      ASSERT_OK(CreateTable(Substitute("$0-$1", kTablePrefix, i++), kTableSchema));
      faststring buf;
      ASSERT_OK(c.FetchURL(
          Substitute("http://$0/metrics?ids=kudu.master&metrics=block_cache_inserts",
                     mini_master_->bound_http_addr().ToString()),
          &buf));
      string raw = buf.ToString();
      // If instrumented correctly, the new master should eventually display
      // non-zero metrics for the block cache.
      ASSERT_STR_MATCHES(raw, ".*\"value\": [1-9].*");
  });
}

TEST_F(MasterTest, TestStartupWebPage) {
  EasyCurl c;
  faststring buf;
  string addr = mini_master_->bound_http_addr().ToString();
  mini_master_->Shutdown();
  std::atomic<bool> run_status_reader = false;
  thread read_startup_page([&] {
    EasyCurl thread_c;
    faststring thread_buf;
    while (!run_status_reader) {
      SleepFor(MonoDelta::FromMilliseconds(10));
      if (!(thread_c.FetchURL(strings::Substitute("http://$0/startup", addr), &thread_buf)).ok()) {
        continue;
      }
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"init_status\":(100|0)( |,)");
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"read_filesystem_status\":(100|0)( |,)");
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"read_instance_metadatafiles_status\""
                                                    ":(100|0)( |,)");
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"read_data_directories_status\":"
                                                    "([0-9]|[1-9][0-9]|100)( |,)");
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"initialize_master_catalog_status\":"
                                                    "([0-9]|[1-9][0-9]|100)( |,)");
      ASSERT_STR_MATCHES(thread_buf.ToString(), "\"start_rpc_server_status\":(100|0)( |,)");
    }
  });
  SCOPED_CLEANUP({
    run_status_reader = true;
    read_startup_page.join();
  });

  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->WaitForCatalogManagerInit());
  run_status_reader = true;

  // After all the steps have been completed, ensure every startup step has 100 percent status
  ASSERT_OK(c.FetchURL(strings::Substitute("http://$0/startup", addr), &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "\"init_status\":100");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"read_filesystem_status\":100");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"read_instance_metadatafiles_status\":100");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"read_data_directories_status\":100");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"initialize_master_catalog_status\":100");
  ASSERT_STR_CONTAINS(buf.ToString(), "\"start_rpc_server_status\":100");
}

TEST_F(MasterTest, TestRegisterAndHeartbeat) {
  const char* const kTsUUID = "my-ts-uuid";

  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid(kTsUUID);
  common.mutable_ts_instance()->set_instance_seqno(1);

  // Try a heartbeat. The server hasn't heard of us, so should ask us
  // to re-register.
  {
    RpcController rpc;
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    req.mutable_common()->CopyFrom(common);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_TRUE(resp.needs_reregister());
    ASSERT_TRUE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  vector<shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(0, descs.size()) << "Should not have registered anything";

  shared_ptr<TSDescriptor> ts_desc;
  ASSERT_FALSE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));

  // Register the fake TS, without sending any tablet report.
  ServerRegistrationPB fake_reg;
  MakeHostPortPB("localhost", 1000, fake_reg.add_rpc_addresses());
  MakeHostPortPB("localhost", 2000, fake_reg.add_http_addresses());
  fake_reg.set_software_version(VersionInfo::GetVersionInfo());
  fake_reg.set_start_time(10000);

  // Information on replica management scheme.
  ReplicaManagementInfoPB rmi;
  rmi.set_replacement_scheme(FLAGS_raft_prepare_replacement_before_eviction
      ? ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : ReplicaManagementInfoPB::EVICT_FIRST);

  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should have registered the TS";
  ServerRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  ASSERT_EQ(SecureDebugString(fake_reg), SecureDebugString(reg))
      << "Master got different registration";

  ASSERT_TRUE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

  // If the tablet server somehow lost the response to its registration RPC, it would
  // attempt to register again. In that case, we shouldn't reject it -- we should
  // just respond the same.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // If we send the registration RPC while the master isn't the leader, it
  // shouldn't ask for a full tablet report.
  {
    CatalogManager::ScopedLeaderDisablerForTests o(master_->catalog_manager());
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_FALSE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // Send a full tablet report, but with the master as a follower. The
  // report will be ignored.
  {
    CatalogManager::ScopedLeaderDisablerForTests o(master_->catalog_manager());
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_FALSE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  // Now send a full report with the master as leader. The master will process
  // it; this is reflected in the response.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_TRUE(resp.has_tablet_report());
  }

  // Having sent a full report, an incremental report will also be processed.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(true);
    tr->set_sequence_number(0);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_TRUE(resp.has_tablet_report());
  }

  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should still only have one TS registered";

  ASSERT_TRUE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

  // Ensure that the ListTabletServers shows the faked server.
  {
    ListTabletServersRequestPB req;
    ListTabletServersResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->ListTabletServers(req, &resp, &rpc));

    LOG(INFO) << SecureDebugString(resp);
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(1, resp.servers_size());
    ASSERT_EQ("my-ts-uuid", resp.servers(0).instance_id().permanent_uuid());
    ASSERT_EQ(1, resp.servers(0).instance_id().instance_seqno());
  }

  // Ensure that /dump-entities endpoint also shows the faked server.
  {
    EasyCurl c;
    faststring buf;
    string addr = mini_master_->bound_http_addr().ToString();
    ASSERT_OK(c.FetchURL(Substitute("http://$0/dump-entities", addr), &buf));
    rapidjson::Document doc;
    doc.Parse<0>(buf.ToString().c_str());
    const rapidjson::Value& tablet_servers = doc["tablet_servers"];
    ASSERT_EQ(tablet_servers.Size(), 1);
    const rapidjson::Value& tablet_server = tablet_servers[rapidjson::SizeType(0)];
    ASSERT_STREQ("localhost:1000",
        tablet_server["rpc_addrs"][rapidjson::SizeType(0)].GetString());
    ASSERT_STREQ("http://localhost:2000",
        tablet_server["http_addrs"][rapidjson::SizeType(0)].GetString());
    ASSERT_STREQ("my-ts-uuid", tablet_server["uuid"].GetString());
    ASSERT_TRUE(tablet_server["millis_since_heartbeat"].GetInt64() >= 0);
    ASSERT_EQ(true, tablet_server["live"].GetBool());
    ASSERT_STREQ(VersionInfo::GetVersionInfo().c_str(),
        tablet_server["version"].GetString());
    string start_time;
    StringAppendStrftime(&start_time, "%Y-%m-%d %H:%M:%S %Z", static_cast<time_t>(10000), true);
    ASSERT_STREQ(start_time.c_str(), tablet_server["start_time"].GetString());
  }

  // Ensure that trying to re-register with a different version is OK.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    // This string should never match the actual VersionInfo string, although
    // the numeric portion will match.
    req.mutable_registration()->set_software_version(Substitute("kudu $0 (rev SOME_NON_GIT_HASH)",
                                                                KUDU_VERSION_STRING));

    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
  }

  // Ensure that trying to re-register with a different start_time is OK.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    // 10 minutes later.
    req.mutable_registration()->set_start_time(10600);

    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
  }

  // Ensure that trying to re-register with a different port fails.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    req.mutable_registration()->mutable_rpc_addresses(0)->set_port(1001);
    Status s = proxy_->TSHeartbeat(req, &resp, &rpc);
    ASSERT_TRUE(s.IsRemoteError());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Tablet server my-ts-uuid is attempting to re-register "
                        "with a different host/port.");
  }

  // Ensure an attempt to register fails if the tablet server uses the replica
  // management scheme which is different from the scheme that
  // the catalog manager uses.
  {
    ServerRegistrationPB fake_reg;
    MakeHostPortPB("localhost", 3000, fake_reg.add_rpc_addresses());
    MakeHostPortPB("localhost", 4000, fake_reg.add_http_addresses());

    // Set replica management scheme to something different that master uses
    // (here, it's just inverted).
    ReplicaManagementInfoPB rmi;
    rmi.set_replacement_scheme(FLAGS_raft_prepare_replacement_before_eviction
        ? ReplicaManagementInfoPB::EVICT_FIRST
        : ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION);

    TSHeartbeatRequestPB req;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);

    TSHeartbeatResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    const Status s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsConfigurationError()) << s.ToString();
    const string msg = Substitute(
        "replica replacement scheme (.*) of the tablet server $0 "
        "at .*:[0-9]+ differs from the catalog manager's (.*); "
        "they must be run with the same scheme", kTsUUID);
    ASSERT_STR_MATCHES(s.ToString(), msg);
  }

  // Ensure that the TS doesn't register if location mapping fails.
  {
    // Set a command that always fails.
    FLAGS_location_mapping_cmd = "false";

    // Restarting the master to take into account the new setting for the
    // --location_mapping_cmd flag.
    mini_master_->Shutdown();
    ASSERT_OK(mini_master_->Restart());

    // Set a new UUID so registration is for the first time.
    auto new_common = common;
    new_common.mutable_ts_instance()->set_permanent_uuid("lmc-fail-ts");

    TSHeartbeatRequestPB hb_req;
    TSHeartbeatResponsePB hb_resp;
    RpcController rpc;
    hb_req.mutable_common()->CopyFrom(new_common);
    hb_req.mutable_registration()->CopyFrom(fake_reg);
    hb_req.mutable_replica_management_info()->CopyFrom(rmi);

    // Registration should fail.
    Status s = proxy_->TSHeartbeat(hb_req, &hb_resp, &rpc);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to run location mapping command");

    // Make sure the tablet server isn't returned to clients.
    ListTabletServersRequestPB list_ts_req;
    ListTabletServersResponsePB list_ts_resp;
    rpc.Reset();
    ASSERT_OK(proxy_->ListTabletServers(list_ts_req, &list_ts_resp, &rpc));

    LOG(INFO) << SecureDebugString(list_ts_resp);
    ASSERT_FALSE(list_ts_resp.has_error());
    ASSERT_EQ(0, list_ts_resp.servers_size());
  }
}

TEST_F(MasterTest, TestCatalog) {
  const char *kTableName = "testtb";
  const char *kOtherTableName = "tbtest";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  ListTablesResponsePB tables;
  NO_FATALS(DoListAllTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());

  // Delete the table
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // List tables, should show no table
  NO_FATALS(DoListAllTables(&tables));
  ASSERT_EQ(0, tables.tables_size());

  // Re-create the table
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  // Restart the master, verify the table still shows up.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->
      WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));

  NO_FATALS(DoListAllTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());

  // Test listing tables with a filter.
  ASSERT_OK(CreateTable(kOtherTableName, kTableSchema));

  {
    ListTablesRequestPB req;
    req.set_name_filter("test");
    DoListTables(req, &tables);
    ASSERT_EQ(2, tables.tables_size());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("tb");
    DoListTables(req, &tables);
    ASSERT_EQ(2, tables.tables_size());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter(kTableName);
    DoListTables(req, &tables);
    ASSERT_EQ(1, tables.tables_size());
    ASSERT_EQ(kTableName, tables.tables(0).name());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("btes");
    DoListTables(req, &tables);
    ASSERT_EQ(1, tables.tables_size());
    ASSERT_EQ(kOtherTableName, tables.tables(0).name());
  }

  {
    ListTablesRequestPB req;
    req.set_name_filter("randomname");
    DoListTables(req, &tables);
    ASSERT_EQ(0, tables.tables_size());
  }
}

TEST_F(MasterTest, ListTablesWithTableFilter) {
  static const char* const kUserTableName = "user_table";
  static const char* const kSystemTableName = "system_table";
  const Schema kSchema({ColumnSchema("key", INT32), ColumnSchema("v1", INT8)}, 1);

  // Make sure this test scenario stays valid
  ASSERT_EQ(TableTypePB_MAX, TableTypePB::TXN_STATUS_TABLE);

  // Given there is not any tablet server in the cluster, there should be no
  // tables out there yet, so a request with an empty (default) filter should
  // return empty list of tables.
  {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(0, resp.tables_size());
  }

  // The same reasoning as above, but supply filter with all known table types
  // in the 'table_type' field.
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::DEFAULT_TABLE);
    req.add_type_filter(TableTypePB::TXN_STATUS_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(0, resp.tables_size());
  }

  ASSERT_OK(CreateTable(kUserTableName, kSchema));

  // An empty 'filter_type' field is equivalent to setting the 'filter_type'
  // to [TableTypePB::DEFAULT_TABLE].
  {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(1, resp.tables_size());
    ASSERT_EQ(kUserTableName, resp.tables(0).name());
  }

  // Specify the table type filter explicitly.
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::DEFAULT_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(1, resp.tables_size());
    ASSERT_EQ(kUserTableName, resp.tables(0).name());
  }

  // No system tables are present at this point.
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::TXN_STATUS_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(0, resp.tables_size());
  }

  ASSERT_OK(CreateTable(
      kSystemTableName, kSchema, TableTypePB::TXN_STATUS_TABLE));

  // Only user tables should be listed because the explicitly specified
  // 'type_filter' is set to [TableTypePB::DEFAULT_TABLE].
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::DEFAULT_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(1, resp.tables_size());
    ASSERT_EQ(kUserTableName, resp.tables(0).name());
  }

  // Only the txn status system table should be listed because the explicitly
  // specified 'type_filter' is set to [TableTypePB::TXN_STATUS_TABLE].
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::TXN_STATUS_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(1, resp.tables_size());
    ASSERT_EQ(kSystemTableName, resp.tables(0).name());
  }

  // Both tables should be output when the filter is set to
  // [TableTypePB::DEFAULT_TABLE, TableTypePB::TXN_STATUS_TABLE].
  {
    ListTablesRequestPB req;
    req.add_type_filter(TableTypePB::DEFAULT_TABLE);
    req.add_type_filter(TableTypePB::TXN_STATUS_TABLE);
    ListTablesResponsePB resp;
    NO_FATALS(DoListTables(req, &resp));
    ASSERT_EQ(2, resp.tables_size());
    unordered_set<string> table_names;
    table_names.emplace(resp.tables(0).name());
    table_names.emplace(resp.tables(1).name());
    ASSERT_EQ(2, table_names.size());
    ASSERT_TRUE(ContainsKey(table_names, kUserTableName));
    ASSERT_TRUE(ContainsKey(table_names, kSystemTableName));
  }
}

TEST_F(MasterTest, TestCreateTableCheckRangeInvariants) {
  constexpr const char* const kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32), ColumnSchema("val", INT32) }, 1);

  // No duplicate split rows.
  {
    KuduPartialRow split1(&kTableSchema);
    ASSERT_OK(split1.SetInt32("key", 1));
    KuduPartialRow split2(&kTableSchema);
    ASSERT_OK(split2.SetInt32("key", 2));
    Status s = CreateTable(kTableName, kTableSchema, { split1, split1, split2 });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "duplicate split row");
  }

  // No empty split rows.
  {
    KuduPartialRow split1(&kTableSchema);
    ASSERT_OK(split1.SetInt32("key", 1));
    KuduPartialRow split2(&kTableSchema);
    Status s = CreateTable(kTableName, kTableSchema, { split1, split2 });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "split rows must contain a value for at "
                        "least one range partition column");
  }

  // No split rows and range specific hashing concurrently.
  {
    google::FlagSaver flag_saver;
    FLAGS_enable_per_range_hash_schemas = true; // enable for testing.
    KuduPartialRow split1(&kTableSchema);
    ASSERT_OK(split1.SetInt32("key", 1));
    KuduPartialRow a_lower(&kTableSchema);
    KuduPartialRow a_upper(&kTableSchema);
    ASSERT_OK(a_lower.SetInt32("key", 0));
    ASSERT_OK(a_upper.SetInt32("key", 100));
    vector<HashSchema> range_hash_schemas = {{}};
    Status s = CreateTable(kTableName,
                           kTableSchema,
                           { split1 },
                           { { a_lower, a_upper } },
                           range_hash_schemas);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Both 'split_rows' and 'range_hash_schemas' cannot be "
                        "populated at the same time.");
  }

  // No non-range columns.
  {
    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 1));
    ASSERT_OK(split.SetInt32("val", 1));
    Status s = CreateTable(kTableName, kTableSchema, { split });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "split rows may only contain values for "
                        "range partition columns: val");
  }

  { // Overlapping bounds.
    KuduPartialRow a_lower(&kTableSchema);
    KuduPartialRow a_upper(&kTableSchema);
    KuduPartialRow b_lower(&kTableSchema);
    KuduPartialRow b_upper(&kTableSchema);
    ASSERT_OK(a_lower.SetInt32("key", 0));
    ASSERT_OK(a_upper.SetInt32("key", 100));
    ASSERT_OK(b_lower.SetInt32("key", 50));
    ASSERT_OK(b_upper.SetInt32("key", 150));
    Status s = CreateTable(kTableName,
                           kTableSchema,
                           {},
                           { { a_lower, a_upper }, { b_lower, b_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "overlapping range partition");
  }
  { // Split row out of bounds (above).
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 150));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 200));

    Status s = CreateTable(
        kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "split out of bounds");
  }
  { // Split row out of bounds (below).
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 150));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", -120));

    Status s = CreateTable(
        kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "split out of bounds");
  }
  { // Lower bound greater than upper bound.
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 150));
    ASSERT_OK(bound_upper.SetInt32("key", 0));

    Status s = CreateTable(
        kTableName, kTableSchema, vector<KuduPartialRow>{},
        { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        "range partition lower bound must be less than the upper bound");
  }
  { // Lower bound equals upper bound.
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 0));

    Status s = CreateTable(
        kTableName, kTableSchema,
        vector<KuduPartialRow>{}, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        "range partition lower bound must be less than the upper bound");
  }
  { // Split equals lower bound
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 10));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 0));

    Status s = CreateTable(
        kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "split matches lower or upper bound");
  }
  { // Split equals upper bound
    KuduPartialRow bound_lower(&kTableSchema);
    KuduPartialRow bound_upper(&kTableSchema);
    ASSERT_OK(bound_lower.SetInt32("key", 0));
    ASSERT_OK(bound_upper.SetInt32("key", 10));

    KuduPartialRow split(&kTableSchema);
    ASSERT_OK(split.SetInt32("key", 10));

    Status s = CreateTable(
        kTableName, kTableSchema, { split }, { { bound_lower, bound_upper } });
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "split matches lower or upper bound");
  }
}

TEST_F(MasterTest, TestCreateTableInvalidKeyType) {
  const char *kTableName = "testtb";

  const DataType types[] = { BOOL, FLOAT, DOUBLE };
  for (DataType type : types) {
    const Schema kTableSchema({ ColumnSchema("key", type) }, 1);
    Status s = CreateTable(kTableName, kTableSchema, vector<KuduPartialRow>());
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
        "key column may not have type of BOOL, FLOAT, or DOUBLE");
  }
}

TEST_F(MasterTest, TestCreateTableOwnerNameTooLong) {
  constexpr const char* const kTableName = "testb";
  const string kOwnerName =
      "abcdefghijklmnopqrstuvwxyz01234567899"
      "abcdefghijklmnopqrstuvwxyz01234567899"
      "abcdefghijklmnopqrstuvwxyz01234567899"
      "abcdefghijklmnopqrstuvwxyz01234567899";

  const Schema kTableSchema({ ColumnSchema("key", INT32), ColumnSchema("val", INT32) }, 1);
  Status s = CreateTable(kTableName, kTableSchema, none, kOwnerName);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "invalid owner name");
}

TEST_F(MasterTest, TestCreateTableCommentTooLong) {
  constexpr const char* const kTableName = "testb";
  const string kComment = string(FLAGS_max_table_comment_length + 1, 'x');
  const Schema kTableSchema({ ColumnSchema("key", INT32), ColumnSchema("val", INT32) }, 1);
  Status s = CreateTable(kTableName, kTableSchema, none, none, kComment);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "invalid table comment");
}

// Regression test for KUDU-253/KUDU-592: crash if the schema passed to CreateTable
// is invalid.
TEST_F(MasterTest, TestCreateTableInvalidSchema) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name("table");
  for (int i = 0; i < 2; i++) {
    ColumnSchemaPB* col = req.mutable_schema()->add_columns();
    col->set_name("col");
    col->set_type(INT32);
    col->set_is_key(true);
  }

  ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ("code: INVALID_ARGUMENT message: \"Duplicate column name: col\"",
            SecureShortDebugString(resp.error().status()));
}

TEST_F(MasterTest, TestVirtualColumns) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name("table");
  ColumnSchemaPB* col = req.mutable_schema()->add_columns();
  col->set_name("foo");
  col->set_type(INT8);
  col->set_is_key(true);
  col = req.mutable_schema()->add_columns();
  col->set_name("bar");
  col->set_type(IS_DELETED);
  bool read_default = true;
  col->set_read_default_value(&read_default, sizeof(read_default));

  ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(Substitute(
      "code: INVALID_ARGUMENT message: \"may not create virtual column of type "
      "\\'$0\\' (column \\'bar\\')\"", GetTypeInfo(IS_DELETED)->name()),
            SecureShortDebugString(resp.error().status()));
}

// Test that, if the client specifies mismatched read and write defaults,
// we return an error.
TEST_F(MasterTest, TestCreateTableMismatchedDefaults) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name("table");

  ColumnSchemaPB* col = req.mutable_schema()->add_columns();
  col->set_name("key");
  col->set_type(INT32);
  col->set_is_key(true);

  col = req.mutable_schema()->add_columns();
  col->set_name("col");
  col->set_type(BINARY);
  col->set_is_nullable(true);
  req.mutable_schema()->mutable_columns(1)->set_read_default_value("hello");
  req.mutable_schema()->mutable_columns(1)->set_write_default_value("bye");

  ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ("code: INVALID_ARGUMENT message: \"column \\'col\\' has "
            "mismatched read/write defaults\"",
            SecureShortDebugString(resp.error().status()));
}

// Non-PK columns cannot be used for per-range custom hash bucket schemas.
TEST_F(MasterTest, NonPrimaryKeyColumnsForPerRangeCustomHashSchema) {
  constexpr const char* const kTableName = "nicetry";
  const Schema kTableSchema(
      { ColumnSchema("key", INT32), ColumnSchema("int32_val", INT32) }, 1);

  // Explicitly enable support for per-range custom hash bucket schemas.
  FLAGS_enable_per_range_hash_schemas = true;

  // For simplicity, a single tablet replica is enough.
  FLAGS_default_num_replicas = 1;

  KuduPartialRow lower(&kTableSchema);
  KuduPartialRow upper(&kTableSchema);
  ASSERT_OK(lower.SetInt32("key", 0));
  ASSERT_OK(upper.SetInt32("key", 100));
  vector<HashSchema> range_hash_schemas{{{{"int32_val"}, 2, 0}}};
  const auto s = CreateTable(
      kTableName, kTableSchema, {}, { { lower, upper } }, range_hash_schemas);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "must specify only primary key columns for "
                      "hash bucket partition components");
}

// Regression test for KUDU-253/KUDU-592: crash if the GetTableLocations RPC call is
// invalid.
TEST_F(MasterTest, TestInvalidGetTableLocations) {
  const string kTableName = "test";
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, schema));
  {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    // Set the "start" key greater than the "end" key.
    req.set_partition_key_start("zzzz");
    req.set_partition_key_end("aaaa");
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ("code: INVALID_ARGUMENT message: "
              "\"start partition key is greater than the end partition key\"",
              SecureShortDebugString(resp.error().status()));
  }
}

#ifndef __APPLE__
// Test that, if the master's RPC service queue overflows, thread stack traces
// are dumped to the diagnostics log.
//
// This test is not relevant on OS X where stack trace dumping is not supported.
TEST_F(MasterTest, TestDumpStacksOnRpcQueueOverflow) {
  mini_master_->mutable_options()->rpc_opts.num_service_threads = 1;
  mini_master_->mutable_options()->rpc_opts.service_queue_length = 1;
  // Disable periodic stack tracing, since we want to be asserting only for the
  // overflow-triggered stack traces. If we don't disable it, it's possible that
  // the overflow will happen during the middle of an unrelated scheduled trace,
  // in which case it will be ignored.
  ANNOTATE_BENIGN_RACE(&FLAGS_diagnostics_log_stack_traces_interval_ms,
      "modified at runtime for test");
  FLAGS_diagnostics_log_stack_traces_interval_ms = 0;
  FLAGS_master_inject_latency_on_tablet_lookups_ms = 1000;
  // Use a new log directory so that the tserver and master don't share the
  // same one. This allows us to isolate the diagnostics log from the master.
  FLAGS_log_dir = JoinPathSegments(GetTestDataDirectory(), "master-logs");
  FLAGS_log_filename = "kudu-master";
  Status s = env_->CreateDir(FLAGS_log_dir);
  ASSERT_TRUE(s.ok() || s.IsAlreadyPresent()) << s.ToString();
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->
      WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));

  // Send a bunch of RPCs which will cause the service queue to overflow. We can send a
  // bogus request since the latency will be injected even if the table doesn't exist.
  GetTableLocationsRequestPB req;
  req.mutable_table()->set_table_name("abc");
  const int kNumRpcs = 20;
  GetTableLocationsResponsePB resps[kNumRpcs];
  RpcController rpcs[kNumRpcs];
  CountDownLatch latch(kNumRpcs);
  for (int i = 0; i < kNumRpcs; i++) {
    proxy_->GetTableLocationsAsync(req, &resps[i], &rpcs[i],
                                   [&latch]() { latch.CountDown(); });
  }
  latch.Wait();

  // Ensure that the metrics log contains a single trace dump for the overflowed service
  // queue. Even though we may have overflowed the queue many times, we throttle the dumping
  // so there should only be one.
  vector<string> log_paths;
  ASSERT_OK(env_->Glob(FLAGS_log_dir + "/*diagnostics*", &log_paths));
  ASSERT_EQ(1, log_paths.size());

  // The dump may not arrive immediately since symbolization may take a few seconds,
  // especially in TSAN builds, etc.
  ASSERT_EVENTUALLY([&]() {
    faststring log_contents;
    ASSERT_OK(ReadFileToString(env_, log_paths[0], &log_contents));
    vector<string> lines = strings::Split(log_contents.ToString(), "\n");
    int dump_count = std::count_if(lines.begin(), lines.end(), [](const string& line) {
        return MatchPattern(line, "*service queue overflowed for kudu.master.MasterService*");
      });
    ASSERT_EQ(dump_count, 1) << log_contents.ToString();
  });
}
#endif // #ifndef __APPLE__

// Tests that if the master is shutdown while a table visitor is active, the
// shutdown waits for the visitor to finish, avoiding racing and crashing.
TEST_F(MasterTest, TestShutdownDuringTableVisit) {
  ASSERT_OK(master_->catalog_manager()->ElectedAsLeaderCb());

  // Master will now shut down, potentially racing with
  // CatalogManager::PrepareForLeadershipTask().
}

// Tests that the catalog manager handles spurious calls to ElectedAsLeaderCb()
// (i.e. those without a term change) correctly by ignoring them. If they
// aren't ignored, a concurrent GetTableLocations() call may trigger a
// use-after-free.
TEST_F(MasterTest, TestGetTableLocationsDuringRepeatedTableVisit) {
  const char* kTableName = "test";
  Schema schema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, schema));

  AtomicBool done(false);

  // Hammers the master with GetTableLocations() calls.
  thread t([&]() {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);

    while (!done.Load()) {
      RpcController controller;
      CHECK_OK(proxy_->GetTableLocations(req, &resp, &controller));
    }
  });

  // Call ElectedAsLeaderCb() repeatedly. If these spurious calls aren't
  // ignored, the concurrent GetTableLocations() calls may crash the master.
  for (int i = 0; i < 100; i++) {
    master_->catalog_manager()->ElectedAsLeaderCb();
  }
  done.Store(true);
  t.join();
}

// The catalog manager had a bug wherein GetTableSchema() interleaved with
// CreateTable() could expose intermediate uncommitted state to clients. This
// test ensures that bug does not regress.
TEST_F(MasterTest, TestGetTableSchemaIsAtomicWithCreateTable) {
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  CountDownLatch started(1);
  AtomicBool done(false);

  // Kick off a thread that hammers the master with GetTableSchema() calls,
  // checking that the results make sense.
  thread t([&]() {
    GetTableSchemaRequestPB req;
    GetTableSchemaResponsePB resp;
    req.mutable_table()->set_table_name(kTableName);

    started.CountDown();
    while (!done.Load()) {
      RpcController controller;

      CHECK_OK(proxy_->GetTableSchema(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are two possible outcomes:
      //
      // 1. GetTableSchema() happened before CreateTable(): we expect to see a
      //    TABLE_NOT_FOUND error.
      // 2. GetTableSchema() happened after CreateTable(): we expect to see the
      //    full table schema.
      //
      // Any other outcome is an error.
      if (resp.has_error()) {
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code());
      } else {
        Schema receivedSchema;
        CHECK_OK(SchemaFromPB(resp.schema(), &receivedSchema));
        CHECK(kTableSchema.Equals(receivedSchema)) <<
            strings::Substitute("$0 not equal to $1",
                                kTableSchema.ToString(), receivedSchema.ToString());
        CHECK_EQ(kTableName, resp.table_name());
      }
    }
  });

  // Only create the table after the thread has started.
  started.Wait();
  EXPECT_OK(CreateTable(kTableName, kTableSchema));

  done.Store(true);
  t.join();
}

class ConcurrentGetTableSchemaTest :
    public MasterTest,
    public ::testing::WithParamInterface<bool> {
 protected:
  ConcurrentGetTableSchemaTest()
      : supports_authz_(GetParam()) {
  }

  void SetUp() override {
    MasterTest::SetUp();
    FLAGS_master_support_authz_tokens = supports_authz_;
  }

  const bool supports_authz_;
  static const MonoDelta kRunInterval;
  static const Schema kTableSchema;
  static const string kTableNamePattern;
};

const MonoDelta ConcurrentGetTableSchemaTest::kRunInterval =
    MonoDelta::FromSeconds(5);
const Schema ConcurrentGetTableSchemaTest::kTableSchema = {
    {
      ColumnSchema("key", INT32),
      ColumnSchema("v1", UINT64),
      ColumnSchema("v2", STRING)
    }, 1 };
const string ConcurrentGetTableSchemaTest::kTableNamePattern = "test_table_$0"; // NOLINT

// Send multiple GetTableSchema() RPC requests for different tables.
TEST_P(ConcurrentGetTableSchemaTest, Rpc) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // kNumTables corresponds to the number of threads sending GetTableSchema
  // RPCs. If setting a bit higher than the RPC queue size limit, there is
  // a chance of overflowing the RPC queue.
  const int kNumTables = FLAGS_rpc_service_queue_length;

  for (auto idx = 0; idx < kNumTables; ++idx) {
    EXPECT_OK(CreateTable(Substitute(kTableNamePattern, idx), kTableSchema));
  }

  AtomicBool done(false);

  // Using multiple proxies to emulate multiple clients working concurrently,
  // one proxy per a worker thread below. If using single proxy for all the
  // threads instead, GetTableSchema() calls issued by the threads below
  // would be serialized by that single proxy, not providing the required level
  // of concurrency.
  vector<unique_ptr<MasterServiceProxy>> proxies;
  proxies.reserve(kNumTables);
  for (auto i = 0; i < kNumTables; ++i) {
    // No more than one reactor is needed since every worker thread below sends
    // out a single GetTableSchema() request at a time; no other
    // incoming/outgoing requests are handled by a worked thread.
    MessengerBuilder bld("Client");
    bld.set_num_reactors(1);
    shared_ptr<Messenger> msg;
    ASSERT_OK(bld.Build(&msg));
    const auto& addr = mini_master_->bound_rpc_addr();
    proxies.emplace_back(new MasterServiceProxy(std::move(msg), addr, addr.host()));
  }

  // Start many threads that hammer the master with GetTableSchema() calls
  // for various tables.
  vector<thread> caller_threads;
  caller_threads.reserve(kNumTables);
  vector<uint64_t> call_counters(kNumTables, 0);
  vector<uint64_t> error_counters(kNumTables, 0);
  for (auto idx = 0; idx < kNumTables; ++idx) {
    caller_threads.emplace_back([&, idx]() {
      auto table_name = Substitute(kTableNamePattern, idx);
      GetTableSchemaRequestPB req;
      req.mutable_table()->set_table_name(table_name);
      GetTableSchemaResponsePB resp;
      while (!done.Load()) {
        RpcController controller;
        resp.Clear();
        CHECK_OK(proxies[idx]->GetTableSchema(req, &resp, &controller));
        ++call_counters[idx];
        if (resp.has_error()) {
          LOG(WARNING) << "GetTableSchema failed: " << SecureDebugString(resp);
          ++error_counters[idx];
          break;
        }
      }
    });
  }
  SCOPED_CLEANUP({
    for (auto& t : caller_threads) {
      t.join();
    }
  });

  SleepFor(kRunInterval);
  done.Store(true);

  const auto errors = accumulate(error_counters.begin(), error_counters.end(), 0UL);
  if (errors != 0) {
    FAIL() << Substitute("detected $0 errors", errors);
  }

  const auto& ent = master_->metric_entity();
  auto hist = METRIC_handler_latency_kudu_master_MasterService_GetTableSchema
      .Instantiate(ent);
  hist->histogram()->DumpHumanReadable(&LOG(INFO));

  const double total = accumulate(call_counters.begin(), call_counters.end(), 0UL);
  LOG(INFO) << Substitute(
      "GetTableSchema RPC: $0 req/sec (authz $1)",
      total / kRunInterval.ToSeconds(), supports_authz_ ? "enabled" : "disabled");
}

// Validate the hostname and the UUID of the server from the metrics webpage
TEST_F(MasterTest, ServerAttributes) {
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(Substitute("http://$0/metrics?ids=kudu.master",
                                mini_master_->bound_http_addr().ToString()),
                                &buf));
  string raw = buf.ToString();
  string server_hostname;
  ASSERT_STR_CONTAINS(raw, "\"uuid\": \"" + mini_master_->permanent_uuid() + "\"");
  ASSERT_OK(GetFQDN(&server_hostname));
  ASSERT_STR_CONTAINS(raw, "\"hostname\": \"" + server_hostname + "\"");
}

// Run multiple threads calling GetTableSchema() directly to system catalog.
TEST_P(ConcurrentGetTableSchemaTest, DirectMethodCall) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr int kNumTables = 200;
  static const string kUserName = "test-user";

  for (auto idx = 0; idx < kNumTables; ++idx) {
    EXPECT_OK(CreateTable(Substitute(kTableNamePattern, idx), kTableSchema));
  }

  AtomicBool done(false);
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  const auto* token_signer = supports_authz_
      ? mini_master_->master()->token_signer() : nullptr;
  optional<const string&> username;
  if (supports_authz_) {
    username = kUserName;
  }

  // Start many threads that hammer the master with GetTableSchema() calls
  // for various tables.
  vector<thread> caller_threads;
  caller_threads.reserve(kNumTables);
  vector<uint64_t> call_counters(kNumTables, 0);
  vector<uint64_t> error_counters(kNumTables, 0);
  for (auto idx = 0; idx < kNumTables; ++idx) {
    caller_threads.emplace_back([&, idx]() {
      GetTableSchemaRequestPB req;
      req.mutable_table()->set_table_name(Substitute(kTableNamePattern, idx));
      while (!done.Load()) {
        RpcController controller;
        GetTableSchemaResponsePB resp;
        {
          CatalogManager::ScopedLeaderSharedLock l(cm);
          CHECK_OK(cm->GetTableSchema(&req, &resp, username, token_signer));
        }
        ++call_counters[idx];
        if (resp.has_error()) {
          LOG(WARNING) << "GetTableSchema failed: " << SecureDebugString(resp);
          ++error_counters[idx];
          break;
        }
      }
    });
  }

  SleepFor(kRunInterval);
  done.Store(true);
  for (auto& t : caller_threads) {
    t.join();
  }

  const auto errors = accumulate(error_counters.begin(), error_counters.end(), 0UL);
  if (errors != 0) {
    FAIL() << Substitute("detected $0 errors", errors);
  }

  const double total = accumulate(call_counters.begin(), call_counters.end(), 0UL);
  LOG(INFO) << Substitute(
      "GetTableSchema function: $0 req/sec (authz $1)",
      total / kRunInterval.ToSeconds(), supports_authz_ ? "enabled" : "disabled");
}

INSTANTIATE_TEST_SUITE_P(SupportsAuthzTokens,
                         ConcurrentGetTableSchemaTest, ::testing::Bool());

// Verifies that on-disk master metadata is self-consistent and matches a set
// of expected contents.
//
// Sample usage:
//
//   MasterMetadataVerifier v(live, dead);
//   sys_catalog->VisitTables(&v);
//   sys_catalog->VisitTablets(&v);
//   ASSERT_OK(v.Verify());
//
class MasterMetadataVerifier : public TableVisitor,
                               public TabletVisitor {
 public:
  MasterMetadataVerifier(unordered_set<string>  live_table_names,
                         multiset<string>  dead_table_names)
    : live_table_names_(std::move(live_table_names)),
      dead_table_names_(std::move(dead_table_names)) {
  }

  Status VisitTable(const std::string& table_id,
                    const SysTablesEntryPB& metadata) override {
     InsertOrDie(&visited_tables_by_id_, table_id,
                 { table_id, metadata.name(), metadata.state() });
     return Status::OK();
   }

  Status VisitTablet(const std::string& table_id,
                     const std::string& tablet_id,
                     const SysTabletsEntryPB& metadata) override {
    InsertOrDie(&visited_tablets_by_id_, tablet_id,
                { tablet_id, table_id, metadata.state() });
    return Status::OK();
  }

  Status Verify() {
    RETURN_NOT_OK(VerifyTables());
    RETURN_NOT_OK(VerifyTablets());
    return Status::OK();
  }

 private:
  Status VerifyTables() {
    unordered_set<string> live_visited_table_names;
    multiset<string> dead_visited_table_names;

    for (const auto& entry : visited_tables_by_id_) {
      const Table& table = entry.second;
      switch (table.state) {
        case SysTablesEntryPB::RUNNING:
        case SysTablesEntryPB::ALTERING:
          InsertOrDie(&live_visited_table_names, table.name);
          break;
        case SysTablesEntryPB::REMOVED:
          // InsertOrDie() doesn't work on multisets, where the returned
          // element is not an std::pair.
          dead_visited_table_names.insert(table.name);
          break;
        default:
          return Status::Corruption(
              Substitute("Table $0 has unexpected state $1",
                         table.id,
                         SysTablesEntryPB::State_Name(table.state)));
      }
    }

    if (live_visited_table_names != live_table_names_) {
      return Status::Corruption("Live table name set mismatch");
    }

    if (dead_visited_table_names != dead_table_names_) {
      return Status::Corruption("Dead table name set mismatch");
    }
    return Status::OK();
  }

  Status VerifyTablets() {
    // Each table should be referenced by exactly this number of tablets.
    const int kNumExpectedReferences = 3;

    // Build table ID --> table map for use in verification below.
    unordered_map<string, const Table*> tables_by_id;
    for (const auto& entry : visited_tables_by_id_) {
      InsertOrDie(&tables_by_id, entry.second.id, &entry.second);
    }

    map<string, int> referenced_tables;
    for (const auto& entry : visited_tablets_by_id_) {
      const Tablet& tablet = entry.second;
      switch (tablet.state) {
        case SysTabletsEntryPB::PREPARING:
        case SysTabletsEntryPB::CREATING:
        case SysTabletsEntryPB::DELETED:
        {
          const Table* table = FindPtrOrNull(tables_by_id, tablet.table_id);
          if (!table) {
            return Status::Corruption(Substitute(
                "Tablet $0 belongs to non-existent table $1",
                tablet.id, tablet.table_id));
          }
          string table_state_str = SysTablesEntryPB_State_Name(table->state);
          string tablet_state_str = SysTabletsEntryPB_State_Name(tablet.state);

          // PREPARING or CREATING tablets must be members of RUNNING or
          // ALTERING tables.
          //
          // DELETED tablets must be members of REMOVED tables.
          if (((tablet.state == SysTabletsEntryPB::PREPARING ||
                tablet.state == SysTabletsEntryPB::CREATING) &&
               (table->state != SysTablesEntryPB::RUNNING &&
                table->state != SysTablesEntryPB::ALTERING)) ||
              (tablet.state == SysTabletsEntryPB::DELETED &&
               table->state != SysTablesEntryPB::REMOVED)) {
            return Status::Corruption(
                Substitute("Unexpected states: table $0=$1, tablet $2=$3",
                           table->id, table_state_str,
                           tablet.id, tablet_state_str));
          }

          referenced_tables[tablet.table_id]++;
          break;
        }
        default:
          return Status::Corruption(
              Substitute("Tablet $0 has unexpected state $1",
                         tablet.id,
                         SysTabletsEntryPB::State_Name(tablet.state)));
      }
    }

    for (const auto& entry : referenced_tables) {
      if (entry.second != kNumExpectedReferences) {
        return Status::Corruption(
            Substitute("Table $0 has bad reference count ($1 instead of $2)",
                       entry.first, entry.second, kNumExpectedReferences));
      }
    }
    return Status::OK();
  }

  // Names of tables that are thought to be created and never deleted.
  const unordered_set<string> live_table_names_;

  // Names of tables that are thought to be deleted. A table with a given name
  // could be deleted more than once.
  const multiset<string> dead_table_names_;

  // Table ID to table map populated during VisitTables().
  struct Table {
    string id;
    string name;
    SysTablesEntryPB::State state;
  };
  unordered_map<string, Table> visited_tables_by_id_;

  // Tablet ID to tablet map populated during VisitTablets().
  struct Tablet {
    string id;
    string table_id;
    SysTabletsEntryPB::State state;
  };
  unordered_map<string, Tablet> visited_tablets_by_id_;
};

TEST_F(MasterTest, TestMasterMetadataConsistentDespiteFailures) {
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  // When generating random table names, we use a uniform distribution so
  // as to generate the occasional name collision; the test should cope.
  const int kUniformBound = 25;

  // Ensure some portion of the attempted operations fail.
  FLAGS_sys_catalog_fail_during_write = 0.25;
  int num_injected_failures = 0;

  // Tracks all "live" tables (i.e. created and not yet deleted).
  vector<string> table_names;

  // Tracks all deleted tables. A given name may have been deleted more
  // than once.
  multiset<string> deleted_table_names;
  Random r(SeedRandom());

  // Spend some time hammering the master with create/alter/delete operations.
  MonoDelta time_to_run = MonoDelta::FromSeconds(AllowSlowTests() ? 10 : 1);
  MonoTime deadline = MonoTime::Now() + time_to_run;
  while (MonoTime::Now() < deadline) {
    int next_action = r.Uniform(3);
    switch (next_action) {
      case 0:
      {
        // Create a new table with a random name and three tablets.
        //
        // No name collision checking, so this table may already exist.
        CreateTableRequestPB req;
        CreateTableResponsePB resp;
        RpcController controller;

        req.set_name(Substitute("table-$0", r.Uniform(kUniformBound)));
        ASSERT_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
        RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());
        KuduPartialRow row(&kTableSchema);
        ASSERT_OK(row.SetInt32("key", 10));
        encoder.Add(RowOperationsPB::SPLIT_ROW, row);
        ASSERT_OK(row.SetInt32("key", 20));
        encoder.Add(RowOperationsPB::SPLIT_ROW, row);

        ASSERT_OK(proxy_->CreateTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsAlreadyPresent() || s.IsRuntimeError()) << s.ToString();
          if (s.IsRuntimeError()) {
            ASSERT_STR_CONTAINS(s.ToString(),
                                SysCatalogTable::kInjectedFailureStatusMsg);
            num_injected_failures++;
          }
        } else {
          table_names.push_back(req.name());
        }
        break;
      }
      case 1:
      {
        // Rename a random table to some random name.
        //
        // No name collision checking, so the new table name may already exist.
        int num_tables = table_names.size();
        if (num_tables == 0) {
          break;
        }
        int table_idx = r.Uniform(num_tables);
        AlterTableRequestPB req;
        AlterTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(table_names[table_idx]);
        req.set_new_table_name(Substitute("table-$0", r.Uniform(kUniformBound)));
        ASSERT_OK(proxy_->AlterTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsAlreadyPresent() || s.IsRuntimeError()) << s.ToString();
          if (s.IsRuntimeError()) {
            ASSERT_STR_CONTAINS(s.ToString(),
                                SysCatalogTable::kInjectedFailureStatusMsg);
            num_injected_failures++;
          }
        } else {
          table_names[table_idx] = req.new_table_name();
        }
        break;
      }
      case 2:
      {
        // Delete a random table.
        int num_tables = table_names.size();
        if (num_tables == 0) {
          break;
        }
        int table_idx = r.Uniform(num_tables);
        DeleteTableRequestPB req;
        DeleteTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(table_names[table_idx]);
        ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
          ASSERT_STR_CONTAINS(s.ToString(),
                              SysCatalogTable::kInjectedFailureStatusMsg);
          num_injected_failures++;
        } else {
          deleted_table_names.insert(table_names[table_idx]);
          table_names[table_idx] = table_names.back();
          table_names.pop_back();
        }
        break;
      }
      default:
        LOG(FATAL) << "Cannot reach here!";
    }
  }

  // Injected failures are random, but given the number of operations we did,
  // we should expect to have seen at least one.
  ASSERT_GE(num_injected_failures, 1);

  // Restart the catalog manager to ensure that it can survive reloading the
  // metadata we wrote to disk. Make sure failure injection is disabled as
  // restarting may issue several catalog writes.
  mini_master_->Shutdown();
  FLAGS_sys_catalog_fail_during_write = 0.0;
  ASSERT_OK(mini_master_->Restart());

  // Reload the metadata again, this time verifying its consistency.
  unordered_set<string> live_table_names(table_names.begin(),
                                         table_names.end());
  MasterMetadataVerifier verifier(live_table_names, deleted_table_names);
  SysCatalogTable* sys_catalog =
      mini_master_->master()->catalog_manager()->sys_catalog();
  ASSERT_OK(sys_catalog->VisitTables(&verifier));
  ASSERT_OK(sys_catalog->VisitTablets(&verifier));
  ASSERT_OK(verifier.Verify());
}

TEST_F(MasterTest, TestConcurrentCreateOfSameTable) {
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);

  // Kick off a bunch of threads all trying to create the same table.
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&]() {
      CreateTableRequestPB req;
      CreateTableResponsePB resp;
      RpcController controller;

      req.set_name(kTableName);
      CHECK_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
      CHECK_OK(proxy_->CreateTable(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are three expected outcomes:
      //
      // 1. This thread won the CreateTable() race: no error.
      // 2. This thread lost the CreateTable() race, but the table is still
      //    in the process of being created: TABLE_ALREADY_PRESENT error with
      //    ServiceUnavailable status.
      // 3. This thread arrived after the CreateTable() race was already over:
      //    TABLE_ALREADY_PRESENT error with AlreadyPresent status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        if (resp.error().code() == MasterErrorPB::TABLE_ALREADY_PRESENT) {
          CHECK(s.IsServiceUnavailable() || s.IsAlreadyPresent()) << failure_msg;
        } else {
          FAIL() << failure_msg;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(MasterTest, TestConcurrentRenameOfSameTable) {
  const char* kOldName = "testtb";
  const char* kNewName = "testtb-new";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);
  ASSERT_OK(CreateTable(kOldName, kTableSchema));

  // Kick off a bunch of threads all trying to rename the same table.
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&]() {
      AlterTableRequestPB req;
      AlterTableResponsePB resp;
      RpcController controller;

      req.mutable_table()->set_table_name(kOldName);
      req.set_new_table_name(kNewName);
      CHECK_OK(proxy_->AlterTable(req, &resp, &controller));
      SCOPED_TRACE(SecureDebugString(resp));

      // There are two expected outcomes:
      //
      // 1. This thread won the AlterTable() race: no error.
      // 2. This thread lost the AlterTable() race: TABLE_NOT_FOUND error
      //    with NotFound status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code()) << failure_msg;
        CHECK(s.IsNotFound()) << failure_msg;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(MasterTest, TestConcurrentCreateAndRenameOfSameTable) {
  const char* kOldName = "testtb";
  const char* kNewName = "testtb-new";
  const Schema kTableSchema({ ColumnSchema("key", INT32),
                              ColumnSchema("v1", UINT64),
                              ColumnSchema("v2", STRING) },
                            1);
  ASSERT_OK(CreateTable(kOldName, kTableSchema));

  AtomicBool create_success(false);
  AtomicBool rename_success(false);
  vector<thread> threads;
  for (int i = 0; i < 10; i++) {
    if (i % 2) {
      threads.emplace_back([&]() {
        CreateTableRequestPB req;
        CreateTableResponsePB resp;
        RpcController controller;

        req.set_name(kNewName);
        RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());

        KuduPartialRow split1(&kTableSchema);
        CHECK_OK(split1.SetInt32("key", 10));
        encoder.Add(RowOperationsPB::SPLIT_ROW, split1);

        KuduPartialRow split2(&kTableSchema);
        CHECK_OK(split2.SetInt32("key", 20));
        encoder.Add(RowOperationsPB::SPLIT_ROW, split2);

        CHECK_OK(SchemaToPB(kTableSchema, req.mutable_schema()));
        CHECK_OK(proxy_->CreateTable(req, &resp, &controller));
        SCOPED_TRACE(SecureDebugString(resp));

        // There are three expected outcomes:
        //
        // 1. This thread finished well before the others: no error.
        // 2. This thread raced with CreateTable() or AlterTable():
        //    TABLE_ALREADY_PRESENT error with ServiceUnavailable status.
        // 3. This thread finished well after the others:
        //    TABLE_ALREADY_PRESENT error with AlreadyPresent status.
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          string failure_msg = Substitute("Unexpected response: $0", SecureDebugString(resp));
          if (resp.error().code() == MasterErrorPB::TABLE_ALREADY_PRESENT) {
              CHECK(s.IsServiceUnavailable() || s.IsAlreadyPresent()) << failure_msg;
          } else {
            FAIL() << failure_msg;
          }
        } else {
          // Creating the table should only succeed once.
          CHECK(!create_success.Exchange(true));
        }
      });
    } else {
      threads.emplace_back([&]() {
        AlterTableRequestPB req;
        AlterTableResponsePB resp;
        RpcController controller;

        req.mutable_table()->set_table_name(kOldName);
        req.set_new_table_name(kNewName);
        CHECK_OK(proxy_->AlterTable(req, &resp, &controller));
        SCOPED_TRACE(SecureDebugString(resp));

        // There are four expected outcomes:
        //
        // 1. This thread finished well before the others: no error.
        // 2. This thread raced with CreateTable() or AlterTable():
        //    TABLE_ALREADY_PRESENT error with ServiceUnavailable status.
        // 3. This thread completed well after a CreateTable():
        //    TABLE_ALREADY_PRESENT error with AlreadyPresent status.
        // 4. This thread completed well after an AlterTable():
        //    TABLE_NOT_FOUND error with NotFound status.
        if (resp.has_error()) {
          Status s = StatusFromPB(resp.error().status());
          string failure_msg = Substitute("Unexpected response: $0",
                                          SecureDebugString(resp));
          switch (resp.error().code()) {
            case MasterErrorPB::TABLE_ALREADY_PRESENT:
              CHECK(s.IsServiceUnavailable() || s.IsAlreadyPresent()) << failure_msg;
              break;
            case MasterErrorPB::TABLE_NOT_FOUND:
              CHECK(s.IsNotFound()) << failure_msg;
              break;
            default:
              FAIL() << failure_msg;
          }
        } else {
          // Renaming the table should only succeed once.
          CHECK(!rename_success.Exchange(true));
        }
      });
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  // At least one of rename or create should have failed; if both succeeded
  // there must be some sort of race.
  CHECK(!rename_success.Load() || !create_success.Load());

  unordered_set<string> live_tables;
  live_tables.insert(kNewName);
  if (create_success.Load()) {
    live_tables.insert(kOldName);
  }
  MasterMetadataVerifier verifier(live_tables, {});
  SysCatalogTable* sys_catalog =
      mini_master_->master()->catalog_manager()->sys_catalog();
  ASSERT_OK(sys_catalog->VisitTables(&verifier));
  ASSERT_OK(sys_catalog->VisitTablets(&verifier));
  ASSERT_OK(verifier.Verify());
}

TEST_F(MasterTest, TestConcurrentRenameAndDeleteOfSameTable) {
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);

  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  bool renamed = false;
  bool deleted = false;

  thread renamer([&] {
      AlterTableRequestPB req;
      AlterTableResponsePB resp;
      RpcController controller;

      req.mutable_table()->set_table_name(kTableName);
      req.set_new_table_name("testtb-renamed");
      CHECK_OK(proxy_->AlterTable(req, &resp, &controller));

      // There are two expected outcomes:
      //
      // 1. This thread won the race: no error.
      // 2. This thread lost the race: TABLE_NOT_FOUND error with NotFound status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code()) << failure_msg;
        CHECK(s.IsNotFound()) << failure_msg;
      } else {
        renamed = true;
      }
  });

  thread dropper([&] {
      DeleteTableRequestPB req;
      DeleteTableResponsePB resp;
      RpcController controller;

      req.mutable_table()->set_table_name(kTableName);
      CHECK_OK(proxy_->DeleteTable(req, &resp, &controller));

      // There are two expected outcomes:
      //
      // 1. This thread won the race: no error.
      // 2. This thread lost the race: TABLE_NOT_FOUND error with NotFound status.
      if (resp.has_error()) {
        Status s = StatusFromPB(resp.error().status());
        string failure_msg = Substitute("Unexpected response: $0",
                                        SecureDebugString(resp));
        CHECK_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code()) << failure_msg;
        CHECK(s.IsNotFound()) << failure_msg;
      } else {
        deleted = true;
      }
  });

  renamer.join();
  dropper.join();

  ASSERT_TRUE(renamed ^ deleted);
}

// Unit tests for the ConnectToMaster() RPC:
// should issue authentication tokens and the master CA cert.
TEST_F(MasterTest, TestConnectToMaster) {
  ConnectToMasterRequestPB req;
  ConnectToMasterResponsePB resp;
  RpcController rpc;
  ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
  SCOPED_TRACE(resp.DebugString());

  EXPECT_EQ(consensus::LEADER, resp.role()) << "should be leader";
  ASSERT_EQ(1, resp.ca_cert_der_size()) << "should have one cert";
  EXPECT_GT(resp.ca_cert_der(0).size(), 100) << "CA cert should be at least 100 bytes";
  ASSERT_TRUE(resp.has_authn_token()) << "should return an authn token";
  // Using 512 bit RSA key and SHA256 digest results in 64 byte signature. If
  // large keys are used, we use 2048 bit RSA keys so the signature is 256
  // bytes.
  int signature_size = UseLargeKeys() ? 256 : 64;
  EXPECT_EQ(signature_size, resp.authn_token().signature().size());
  ASSERT_TRUE(resp.authn_token().has_signing_key_seq_num());
  EXPECT_GT(resp.authn_token().signing_key_seq_num(), -1);

  security::TokenPB token;
  ASSERT_TRUE(token.ParseFromString(resp.authn_token().token_data()));
  ASSERT_TRUE(token.authn().has_username());

  ASSERT_EQ(1, resp.master_addrs_size());
  ASSERT_EQ("127.0.0.1", resp.master_addrs(0).host());
  ASSERT_NE(0, resp.master_addrs(0).port());

  // The returned location should be empty because no location mapping command
  // is defined.
  ASSERT_TRUE(resp.client_location().empty());

  // The cluster ID should match the masters cluster ID.
  string cluster_id;
  const std::shared_ptr<MasterServiceProxy> master_proxy = std::move(proxy_);
  ASSERT_OK(GetClusterId(master_proxy, MonoDelta::FromSeconds(30), &cluster_id));
  ASSERT_TRUE(!cluster_id.empty());
  ASSERT_EQ(cluster_id, resp.cluster_id());
}

TEST_F(MasterTest, TestConnectToMasterAndAssignLocation) {
  // Test first with a valid location mapping command.
  const string kLocationCmdPath = JoinPathSegments(GetTestExecutableDirectory(),
                                                   "testdata/first_argument.sh");
  const string location = "/foo";
  FLAGS_location_mapping_cmd = Substitute("$0 $1", kLocationCmdPath, location);
  FLAGS_master_client_location_assignment_enabled = true;
  {
    // Restarting the master to take into account the new setting for the
    // --location_mapping_cmd flag.
    mini_master_->Shutdown();
    ASSERT_OK(mini_master_->Restart());
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(location, resp.client_location());
  }

  // Now try again with an invalid command. The RPC should succeed but no
  // location should be assigned.
  FLAGS_location_mapping_cmd = "false";
  {
    // Restarting the master to take into account the new setting for the
    // --location_mapping_cmd flag.
    mini_master_->Shutdown();
    ASSERT_OK(mini_master_->Restart());
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.client_location().empty());
  }

  // Finally, use a command returning a different location.
  const string new_location = "/bar";
  FLAGS_location_mapping_cmd = Substitute("$0 $1", kLocationCmdPath, new_location);
  {
    // Restarting the master to take into account the new setting for the
    // --location_mapping_cmd flag.
    mini_master_->Shutdown();
    ASSERT_OK(mini_master_->Restart());
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(new_location, resp.client_location());
  }
}

// Test that the master signs its on server certificate when it becomes the leader,
// and also that it loads TSKs into the messenger's verifier.
TEST_F(MasterTest, TestSignOwnCertAndLoadTSKs) {
  ASSERT_EVENTUALLY([&]() {
      ASSERT_TRUE(master_->tls_context().has_signed_cert());
      ASSERT_GT(master_->messenger()->token_verifier().GetMaxKnownKeySequenceNumber(), -1);
    });
}

TEST_F(MasterTest, TestTableIdentifierWithIdAndName) {
  const char *kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);

  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  ListTablesResponsePB tables;
  NO_FATALS(DoListAllTables(&tables));
  ASSERT_EQ(1, tables.tables_size());
  ASSERT_EQ(kTableName, tables.tables(0).name());
  string table_id = tables.tables(0).id();
  ASSERT_FALSE(table_id.empty());

  // Delete the table with an invalid ID.
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    req.mutable_table()->set_table_id("abc123");
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code());
  }

  // Delete the table with an invalid name.
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name("abc123");
    req.mutable_table()->set_table_id(table_id);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code());
  }

  // Delete the table with an invalid ID and name.
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name("abc123");
    req.mutable_table()->set_table_id("abc123");
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(MasterErrorPB::TABLE_NOT_FOUND, resp.error().code());
  }

  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    req.mutable_table()->set_table_id(table_id);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
}

TEST_F(MasterTest, TestDuplicateRequest) {
  const char* const kTsUUID = "my-ts-uuid";
  TSToMasterCommonPB common;
  common.mutable_ts_instance()->set_permanent_uuid(kTsUUID);
  common.mutable_ts_instance()->set_instance_seqno(1);

  // Register the fake TS, without sending any tablet report.
  ServerRegistrationPB fake_reg;
  MakeHostPortPB("localhost", 1000, fake_reg.add_rpc_addresses());
  MakeHostPortPB("localhost", 2000, fake_reg.add_http_addresses());
  fake_reg.set_software_version(VersionInfo::GetVersionInfo());
  fake_reg.set_start_time(10000);

  // Information on replica management scheme.
  ReplicaManagementInfoPB rmi;
  rmi.set_replacement_scheme(ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION);

  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    req.mutable_registration()->CopyFrom(fake_reg);
    req.mutable_replica_management_info()->CopyFrom(rmi);
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));

    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.leader_master());
    ASSERT_FALSE(resp.needs_reregister());
    ASSERT_FALSE(resp.needs_full_tablet_report());
    ASSERT_FALSE(resp.has_tablet_report());
  }

  vector<shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);
  ASSERT_EQ(1, descs.size()) << "Should have registered the TS";
  ServerRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  ASSERT_EQ(SecureDebugString(fake_reg), SecureDebugString(reg))
      << "Master got different registration";
  shared_ptr<TSDescriptor> ts_desc;
  ASSERT_TRUE(master_->ts_manager()->LookupTSByUUID(kTsUUID, &ts_desc));
  ASSERT_EQ(ts_desc, descs[0]);

  // Create a table with three tablets.
  const char *kTableName = "test_table";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  vector<scoped_refptr<TableInfo>> tables;
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(1, tables.size());
  }

  scoped_refptr<TableInfo> table = tables[0];
  vector<scoped_refptr<TabletInfo>> tablets;
  table->GetAllTablets(&tablets);
  ASSERT_EQ(tablets.size(), 3);

  // Delete the table.
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // The table has no pending task.
  // The master would not send DeleteTablet requests for no consensus state for tablets.
  vector<scoped_refptr<MonitoredTask>> task_list;
  tables[0]->GetTaskList(&task_list);
  ASSERT_EQ(task_list.size(), 0);

  // Now the tserver send a full report with a deleted tablet.
  // The master will process it and send 'DeleteTablet' request to the tserver.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(false);
    tr->set_sequence_number(0);
    tr->add_updated_tablets()->set_tablet_id(tablets[0]->id());
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_tablet_report());
  }

  // The 'DeleteTablet' task is running for the master will continue
  // retrying to connect to the fake TS.
  tables[0]->GetTaskList(&task_list);
  ASSERT_EQ(task_list.size(), 1);
  ASSERT_EQ(task_list[0]->state(), MonitoredTask::kStateRunning);

  // Now the tserver send a full report with two deleted tablets.
  // The master will not send duplicate DeleteTablet request to the tserver.
  {
    TSHeartbeatRequestPB req;
    TSHeartbeatResponsePB resp;
    RpcController rpc;
    req.mutable_common()->CopyFrom(common);
    TabletReportPB* tr = req.mutable_tablet_report();
    tr->set_is_incremental(true);
    tr->set_sequence_number(0);
    tr->add_updated_tablets()->set_tablet_id(tablets[0]->id());
    tr->add_updated_tablets()->set_tablet_id(tablets[1]->id());
    ASSERT_OK(proxy_->TSHeartbeat(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_tablet_report());
  }

  tables[0]->GetTaskList(&task_list);
  ASSERT_EQ(task_list.size(), 2);
}

TEST_F(MasterTest, TestHideLiveRowCountInTableMetrics) {
  const char* kTableName = "testtable";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  vector<scoped_refptr<TableInfo>> tables;
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(1, tables.size());
  }
  vector<scoped_refptr<TabletInfo>> tablets;
  tables[0]->GetAllTablets(&tablets);

  const auto call_update_metrics = [&] (
      scoped_refptr<TabletInfo>& tablet,
      bool support_live_row_count) {
    tablet::ReportedTabletStatsPB old_stats;
    tablet::ReportedTabletStatsPB new_stats;
    new_stats.set_on_disk_size(1);
    if (support_live_row_count) {
      old_stats.set_live_row_count(1);
      new_stats.set_live_row_count(1);
    }
    tables[0]->UpdateMetrics(tablet->id(), old_stats, new_stats);
  };

  // Trigger to cause 'live_row_count' invisible.
  {
    for (int i = 0; i < 100; ++i) {
      for (int j = 0; j < tablets.size(); ++j) {
        NO_FATALS(call_update_metrics(tablets[j], (tablets.size() - 1 != j)));
      }
    }

    EasyCurl c;
    faststring buf;
    ASSERT_OK(c.FetchURL(Substitute("http://$0/metrics?ids=$1",
                                    mini_master_->bound_http_addr().ToString(),
                                    tables[0]->id()),
                         &buf));
    string raw = buf.ToString();
    ASSERT_STR_CONTAINS(raw, kTableName);
    ASSERT_STR_CONTAINS(raw, "on_disk_size");
    ASSERT_STR_NOT_CONTAINS(raw, "live_row_count");
  }

  // Trigger to cause 'live_row_count' visible.
  {
    for (int i = 0; i < 100; ++i) {
      for (int j = 0; j < tablets.size(); ++j) {
        NO_FATALS(call_update_metrics(tablets[j], true));
      }
    }
    EasyCurl c;
    faststring buf;
    ASSERT_OK(c.FetchURL(Substitute("http://$0/metrics?ids=$1",
                                    mini_master_->bound_http_addr().ToString(),
                                    tables[0]->id()),
                         &buf));
    string raw = buf.ToString();
    ASSERT_STR_CONTAINS(raw, kTableName);
    ASSERT_STR_CONTAINS(raw, "on_disk_size");
    ASSERT_STR_CONTAINS(raw, "live_row_count");
  }
}

TEST_F(MasterTest, TestTableStatisticsWithOldVersion) {
  const char* kTableName = "testtable";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  vector<scoped_refptr<TableInfo>> tables;
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(1, tables.size());
  }
  const auto& table = tables[0];
  vector<scoped_refptr<TabletInfo>> tablets;
  table->GetAllTablets(&tablets);
  ASSERT_FALSE(tablets.empty());

  {
    table->InvalidateMetrics(tablets.back()->id());
    ASSERT_FALSE(table->GetMetrics()->TableSupportsOnDiskSize());
    ASSERT_FALSE(table->GetMetrics()->TableSupportsLiveRowCount());
  }
  {
    tablet::ReportedTabletStatsPB old_stats;
    tablet::ReportedTabletStatsPB new_stats;
    new_stats.set_on_disk_size(1024);
    new_stats.set_live_row_count(1000);
    table->UpdateMetrics(tablets.back()->id(), old_stats, new_stats);
    ASSERT_TRUE(table->GetMetrics()->TableSupportsOnDiskSize());
    ASSERT_TRUE(table->GetMetrics()->TableSupportsLiveRowCount());
    ASSERT_EQ(1024, table->GetMetrics()->on_disk_size->value());
    ASSERT_EQ(1000, table->GetMetrics()->live_row_count->value());
  }
}

TEST_F(MasterTest, TestDeletedTablesAndTabletsCleanup) {
  FLAGS_enable_deleted_tables_and_tablets_cleanup = true;
  FLAGS_deleted_table_and_tablet_reserved_secs = 1;
  FLAGS_catalog_manager_bg_task_wait_ms = 10;

  const char* kTableName = "testtable";
  const Schema kTableSchema({ColumnSchema("key", INT32)}, 1);
  ASSERT_OK(CreateTable(kTableName, kTableSchema));

  vector<scoped_refptr<TableInfo>> tables;
  vector<scoped_refptr<TabletInfo>> tablets;
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(1, tables.size());
    NO_FATALS(master_->catalog_manager()->GetAllTablets(&tablets));
    ASSERT_EQ(3, tablets.size());
  }

  // Replace a tablet.
  {
    const string& tablet_id = tablets[0]->id();
    ReplaceTabletRequestPB req;
    ReplaceTabletResponsePB resp;
    req.set_tablet_id(tablet_id);
    RpcController controller;
    ASSERT_OK(proxy_->ReplaceTablet(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // The replaced tablet would be cleaned up if expired.
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTablets(&tablets));
    ASSERT_EQ(4, tablets.size());
  }
  SleepFor(MonoDelta::FromSeconds(FLAGS_deleted_table_and_tablet_reserved_secs + 1));
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTablets(&tablets));
    ASSERT_EQ(3, tablets.size());
  }

  // Delete the table.
  {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(kTableName);
    ASSERT_OK(proxy_->DeleteTable(req, &resp, &controller));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());
  }

  // The deleted table and tablets would be cleaned up if expired.
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(1, tables.size());
    NO_FATALS(master_->catalog_manager()->GetAllTablets(&tablets));
    ASSERT_EQ(3, tablets.size());
  }
  SleepFor(MonoDelta::FromSeconds(FLAGS_deleted_table_and_tablet_reserved_secs + 1));
  {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    NO_FATALS(master_->catalog_manager()->GetAllTables(&tables));
    ASSERT_EQ(0, tables.size());
    NO_FATALS(master_->catalog_manager()->GetAllTablets(&tablets));
    ASSERT_EQ(0, tablets.size());
  }
}

class AuthzTokenMasterTest : public MasterTest,
                             public ::testing::WithParamInterface<bool> {};

// Some basic verifications that we get authz tokens when we expect.
TEST_P(AuthzTokenMasterTest, TestGenerateAuthzTokens) {
  bool supports_authz = GetParam();
  FLAGS_master_support_authz_tokens = supports_authz;
  const char* kTableName = "testtb";
  const Schema kTableSchema({ ColumnSchema("key", INT32) }, 1);
  const auto send_req = [&] (GetTableSchemaResponsePB* resp) {
    RpcController rpc;
    GetTableSchemaRequestPB req;
    req.mutable_table()->set_table_name(kTableName);
    return proxy_->GetTableSchema(req, resp, &rpc);
  };
  // Send a request for which there is no table. Whether or not authz tokens are
  // supported, the response should have an error.
  {
    GetTableSchemaResponsePB resp;
    ASSERT_OK(send_req(&resp));
    ASSERT_TRUE(resp.has_error());
    const Status s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
  // Now create the table and check that we only get tokens when we expect.
  ASSERT_OK(CreateTable(kTableName, kTableSchema));
  {
    GetTableSchemaResponsePB resp;
    ASSERT_OK(send_req(&resp));
    ASSERT_EQ(supports_authz, resp.has_authz_token());
  }
}

INSTANTIATE_TEST_SUITE_P(SupportsAuthzTokens, AuthzTokenMasterTest, ::testing::Bool());

} // namespace master
} // namespace kudu
