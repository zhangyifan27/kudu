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
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/rebalance/rebalancer.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/rebalancer_tool.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/tool_replica_util.h"
#include "kudu/util/status.h"
#include "kudu/util/version_util.h"

using kudu::rebalance::Rebalancer;
using std::cout;
using std::endl;
using std::make_tuple;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

DECLARE_string(tables);
DECLARE_string(tablets);

DEFINE_string(ignored_tservers, "",
              "UUIDs of tablet servers to ignore while rebalancing the cluster "
              "(comma-separated list). If specified, the tablet servers are "
              "effectively ignored by the rebalancer tool, they are not considered "
              "as a part of the cluster as well as the replicas on them. "
              "If not specified, the rebalancer tool will run on all the tablet servers "
              "in the cluster.");

DEFINE_string(sections, "*",
              "Sections to print (comma-separated list of sections, "
              "available sections are: MASTER_SUMMARIES, TSERVER_SUMMARIES, "
              "VERSION_SUMMARIES, TABLET_SUMMARIES, TABLE_SUMMARIES, "
              "CHECKSUM_RESULTS and TOTAL_COUNT.) "
              "If not specified, print all sections.");

DEFINE_uint32(max_moves_per_server, 5,
              "Maximum number of replica moves to perform concurrently on one "
              "tablet server: 'move from' and 'move to' are counted "
              "as separate move operations.");

DEFINE_uint32(max_staleness_interval_sec, 300,
              "Maximum duration of the 'staleness' interval, when the "
              "rebalancer cannot make any progress in scheduling new moves and "
              "no prior scheduled moves are left, even if re-synchronizing "
              "against the cluster's state again and again. Such a staleness "
              "usually happens in case of a persistent problem with the "
              "cluster or when some unexpected concurrent activity is "
              "present (such as automatic recovery of failed replicas, etc.)");

DEFINE_int64(max_run_time_sec, 0,
             "Maximum time to run the rebalancing, in seconds. Specifying 0 "
             "means not imposing any limit on the rebalancing run time.");

DEFINE_string(move_single_replicas, "auto",
              "Whether to move single replica tablets (i.e. replicas of tablets "
              "of replication factor 1). Acceptable values are: "
              "'auto', 'enabled', 'disabled'. The value of 'auto' means "
              "turn it on/off depending on the replica management scheme "
              "and Kudu version.");

DEFINE_bool(output_replica_distribution_details, false,
            "Whether to output details on per-table and per-server "
            "replica distribution");

DEFINE_bool(report_only, false,
            "Whether to report on table- and cluster-wide replica distribution "
            "skew and exit without doing any actual rebalancing");

DEFINE_bool(disable_policy_fixer, false,
            "In case of multi-location cluster, whether to detect and fix "
            "placement policy violations. Fixing placement policy violations "
            "involves moving tablet replicas across different locations "
            "of the cluster. "
            "This setting is applicable to multi-location clusters only.");

DEFINE_bool(disable_cross_location_rebalancing, false,
            "In case of multi-location cluster, whether to move tablet "
            "replicas between locations in attempt to spread tablet replicas "
            "among location evenly (equalizing loads of locations throughout "
            "the cluster). "
            "This setting is applicable to multi-location clusters only.");

DEFINE_bool(disable_intra_location_rebalancing, false,
            "In case of multi-location cluster, whether to rebalance tablet "
            "replica distribution within each location. "
            "This setting is applicable to multi-location clusters only.");

DEFINE_bool(move_replicas_from_ignored_tservers, false,
            "Whether to move replicas from the specified 'ignored_tservers' to other "
            "servers when the source tablet server is healthy. "
            "This setting is effective only if the '--ignored_tservers' flag "
            "is specified as well. "
            "If set true, then all ignored tablet servers must be placed into "
            "the 'maintenance mode'.");

DEFINE_double(load_imbalance_threshold,
              kudu::rebalance::Rebalancer::Config::kLoadImbalanceThreshold,
              "The threshold for the per-table location load imbalance. "
              "The threshold is used during the cross-location rebalancing "
              "phase. If the measured cross-location load imbalance for a "
              "table is greater than the specified threshold, the rebalancer "
              "tries to move table's replicas to reduce the imbalance. "
              "The recommended range for the threshold is [0.5, ...) with the "
              "default value of 1.0. The threshold represents a policy "
              "wrt what to prefer: either ideal balance of the cross-location "
              "load on per-table basis (lower threshold value) or minimum "
              "number of replica movements between locations "
              "(greater threshold value). The default value is empirically "
              "proven to be a good choice between 'ideal' and 'good enough' "
              "replica distributions.");

static bool ValidateMoveSingleReplicas(const char* flag_name,
                                       const string& flag_value) {
  const vector<string> allowed_values = { "auto", "enabled", "disabled" };
  if (std::find_if(allowed_values.begin(), allowed_values.end(),
                   [&](const string& allowed_value) {
                     return boost::iequals(allowed_value, flag_value);
                   }) != allowed_values.end()) {
    return true;
  }

  std::ostringstream ss;
  ss << "'" << flag_value << "': unsupported value for --" << flag_name
     << " flag; should be one of ";
  copy(allowed_values.begin(), allowed_values.end(),
       std::ostream_iterator<string>(ss, " "));
  LOG(ERROR) << ss.str();

  return false;
}
DEFINE_validator(move_single_replicas, &ValidateMoveSingleReplicas);

namespace kudu {
namespace tools {

namespace {

Status RunKsck(const RunnerContext& context) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK_PREPEND(RemoteKsckCluster::Build(master_addresses, &cluster),
                        "unable to build KsckCluster");
  cluster->set_table_filters(Split(FLAGS_tables, ",", strings::SkipEmpty()));
  cluster->set_tablet_id_filters(Split(FLAGS_tablets, ",", strings::SkipEmpty()));
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  ksck->set_print_sections(Split(FLAGS_sections, ",", strings::SkipEmpty()));

  return ksck->RunAndPrintResults();
}

// Does the version in 'version_str' support movement of single replicas?
// The minimum version required is 1.7.1.
bool VersionSupportsRF1Movement(const string& version_str) {
  Version v;
  if (!ParseVersion(version_str, &v).ok()) {
    return false;
  }
  return make_tuple(v.major, v.minor, v.maintenance) >= make_tuple(1, 7, 1);
}

// Whether it make sense to move replicas of single-replica tablets.
// The output parameter 'move_single_replicas' cannot be null.
//
// Moving replicas of tablets with replication factor one (a.k.a. non-replicated
// tablets) is tricky because of the following:
//
//   * The sequence of Raft configuration updates when moving tablet replica
//     in case of the 3-2-3 replica management scheme.
//
//   * KUDU-2443: even with the 3-4-3 replica management scheme, moving of
//     non-replicated tablets is not possible for the versions prior to the fix.
//
Status EvaluateMoveSingleReplicasFlag(const vector<string>& master_addresses,
                                      bool* move_single_replicas) {
  DCHECK(move_single_replicas);
  if (!boost::iequals(FLAGS_move_single_replicas, "auto")) {
    if (boost::iequals(FLAGS_move_single_replicas, "enabled")) {
      *move_single_replicas = true;
    } else {
      DCHECK(boost::iequals(FLAGS_move_single_replicas, "disabled"));
      *move_single_replicas = false;
    }
    return Status::OK();
  }

  shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK_PREPEND(RemoteKsckCluster::Build(master_addresses, &cluster),
                        "unable to build KsckCluster");
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  // Ignoring the result of the Ksck::Run() method: it's possible the cluster
  // is not completely healthy but rebalancing can proceed; for example,
  // if a leader election is occurring.
  ignore_result(ksck->Run());
  const auto& ksck_results = ksck->results();

  for (const auto& summaries : { ksck_results.cluster_status.tserver_summaries,
                                 ksck_results.cluster_status.master_summaries }) {
    for (const auto& summary : summaries) {
      if (summary.version) {
        if (!VersionSupportsRF1Movement(*summary.version)) {
          LOG(INFO) << "found Kudu server of version '" << *summary.version
                    << "'; not rebalancing single-replica tablets as a result";
          *move_single_replicas = false;
          return Status::OK();
        }
      } else {
        LOG(INFO) << "no version information from some servers; "
                  << "not rebalancing single-replica tablets as the result";
        *move_single_replicas = false;
        return Status::OK();
      }
    }
  }

  // Now check for the replica management scheme. If it's the 3-2-3 scheme,
  // don't move replicas of non-replicated (a.k.a. RF=1) tablets. The reasoning
  // is simple: in Raft it's necessary to get acknowledgement from the majority
  // of voter replicas to commit a write operation. In case of the 3-2-3 scheme
  // and non-replicated tablets the majority is two out of two tablet replicas
  // because the destination replica is added as a voter. In case of huge amount
  // of data in the tablet or frequent updates, it might take a long time for the
  // destination replica to catch up. During that time the tablet would not be
  // available. The idea is to reduce the risk of unintended unavailability
  // unless it's explicitly requested by the operator.
  boost::optional<string> tid;
  if (!ksck_results.cluster_status.tablet_summaries.empty()) {
    tid = ksck_results.cluster_status.tablet_summaries.front().id;
  }
  bool is_343_scheme = false;
  auto s = Is343SchemeCluster(master_addresses, tid, &is_343_scheme);
  if (!s.ok()) {
    LOG(WARNING) << s.ToString() << ": failed to get information "
        "on the replica management scheme; not rebalancing "
        "single-replica tablets as the result";
    *move_single_replicas = false;
    return Status::OK();
  }

  *move_single_replicas = is_343_scheme;
  return Status::OK();
}

// Rebalance the cluster. The process is run step-by-step, where at each step
// a new batch of move operations is output by the algorithm. As many as
// possible replica movements from one batch are performed concurrently, while
// running not more than the specified number of concurrent replica movements
// at each tablet server. In other words, at every moment a single tablet server
// can be the source and the destination of no more than the specified number of
// move operations.
Status RunRebalance(const RunnerContext& context) {
  const vector<string> ignored_tservers =
      Split(FLAGS_ignored_tservers, ",", strings::SkipEmpty());
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  const vector<string> table_filters =
      Split(FLAGS_tables, ",", strings::SkipEmpty());

  // Evaluate --move_single_replicas flag: decide whether enable to disable
  // moving of single-replica tablets based on the reported version of the
  // Kudu components.
  bool move_single_replicas = false;
  RETURN_NOT_OK(EvaluateMoveSingleReplicasFlag(master_addresses,
                                               &move_single_replicas));
  RebalancerTool rebalancer(Rebalancer::Config(
      ignored_tservers,
      master_addresses,
      table_filters,
      FLAGS_max_moves_per_server,
      FLAGS_max_staleness_interval_sec,
      FLAGS_max_run_time_sec,
      FLAGS_move_replicas_from_ignored_tservers,
      move_single_replicas,
      FLAGS_output_replica_distribution_details,
      !FLAGS_disable_policy_fixer,
      !FLAGS_disable_cross_location_rebalancing,
      !FLAGS_disable_intra_location_rebalancing,
      FLAGS_load_imbalance_threshold));

  // Print info on pre-rebalance distribution of replicas.
  RETURN_NOT_OK(rebalancer.PrintStats(cout));

  if (FLAGS_report_only) {
    return Status::OK();
  }

  RebalancerTool::RunStatus result_status;
  size_t moves_count;
  RETURN_NOT_OK(rebalancer.Run(&result_status, &moves_count));

  const string msg_template = "rebalancing is complete: $0 (moved $1 replicas)";
  string msg_result_status;
  switch (result_status) {
    case RebalancerTool::RunStatus::CLUSTER_IS_BALANCED:
      msg_result_status = "cluster is balanced";
      break;
    case RebalancerTool::RunStatus::TIMED_OUT:
      msg_result_status = "time is up";
      break;
    default:
      msg_result_status = "unexpected rebalancer status";
      DCHECK(false) << msg_result_status;
      break;
  }
  cout << endl << Substitute(msg_template, msg_result_status, moves_count) << endl;

  if (moves_count != 0) {
    // Print info on post-rebalance distribution of replicas, if any moves
    // were performed at all.
    RETURN_NOT_OK(rebalancer.PrintStats(cout));
  }

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildClusterMode() {
  ModeBuilder builder("cluster");
  builder.Description("Operate on a Kudu cluster");

  {
    constexpr auto desc = "Check the health of a Kudu cluster";
    constexpr auto extra_desc = "By default, ksck checks that master and "
        "tablet server processes are running, and that table metadata is "
        "consistent. Use the 'checksum' flag to check that tablet data is "
        "consistent (also see the 'tables' and 'tablets' flags). Use the "
        "'checksum_snapshot' along with 'checksum' if the table or tablets "
        "are actively receiving inserts or updates. Use the 'verbose' flag to "
        "output detailed information on cluster status even if no "
        "inconsistency is found in metadata.";

    unique_ptr<Action> ksck = ActionBuilder("ksck", &RunKsck)
        .Description(desc)
        .ExtraDescription(extra_desc)
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("checksum_cache_blocks")
        .AddOptionalParameter("checksum_scan")
        .AddOptionalParameter("checksum_scan_concurrency")
        .AddOptionalParameter("checksum_snapshot")
        .AddOptionalParameter("checksum_timeout_sec")
        .AddOptionalParameter("color")
        .AddOptionalParameter("consensus")
        .AddOptionalParameter("fetch_info_concurrency")
        .AddOptionalParameter("ksck_format")
        .AddOptionalParameter("sections")
        .AddOptionalParameter("tables")
        .AddOptionalParameter("tablets")
        .Build();
    builder.AddAction(std::move(ksck));
  }

  {
    constexpr auto desc = "Move tablet replicas between tablet servers to "
        "balance replica counts for each table and for the cluster as a whole.";
    constexpr auto extra_desc = "The rebalancing tool moves tablet replicas "
        "between tablet servers, in the same manner as the "
        "'kudu tablet change_config move_replica' command, attempting to "
        "balance the count of replicas per table on each tablet server, "
        "and after that attempting to balance the total number of replicas "
        "per tablet server.";
    unique_ptr<Action> rebalance = ActionBuilder("rebalance", &RunRebalance)
        .Description(desc)
        .ExtraDescription(extra_desc)
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("disable_policy_fixer")
        .AddOptionalParameter("disable_cross_location_rebalancing")
        .AddOptionalParameter("disable_intra_location_rebalancing")
        .AddOptionalParameter("fetch_info_concurrency")
        .AddOptionalParameter("ignored_tservers")
        .AddOptionalParameter("load_imbalance_threshold")
        .AddOptionalParameter("max_moves_per_server")
        .AddOptionalParameter("max_run_time_sec")
        .AddOptionalParameter("max_staleness_interval_sec")
        .AddOptionalParameter("move_replicas_from_ignored_tservers")
        .AddOptionalParameter("move_single_replicas")
        .AddOptionalParameter("output_replica_distribution_details")
        .AddOptionalParameter("report_only")
        .AddOptionalParameter("tables")
        .Build();
    builder.AddAction(std::move(rebalance));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

