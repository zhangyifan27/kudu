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

#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex> // IWYU pragma: keep
#include <string>
#include <type_traits>  // IWYU pragma: keep
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class DnsResolver;
class Sockaddr;

namespace consensus {
class ConsensusServiceProxy;
}

namespace rpc {
class Messenger;
}

namespace tserver {
class TabletServerAdminServiceProxy;
}

namespace master {

class TSInfoPB;

// Map of dimension -> number of tablets.
typedef std::unordered_map<std::string, int32_t> TabletNumByDimensionMap;

// For a table, a map of range start key -> number of tablets for that range.
typedef std::unordered_map<std::string, int32_t> TabletNumByRangeMap;

// Map of table id -> number of tablets by each range for that table.
typedef std::unordered_map<std::string, TabletNumByRangeMap> TabletNumByRangePerTableMap;

// For a table, a pair of map of range start key -> number of times this range has
// had a replica selected on this tablet server and the number of times the table
// has had a replica selected on this tablet server.
typedef std::pair<std::unordered_map<std::string, double>, double> RecentReplicaByRangesPerTable;

// Map of table id -> number of times each range in the table and
// the table itself has had a replica selected on this tablet server.
typedef std::unordered_map<std::string, RecentReplicaByRangesPerTable> RecentReplicasByTable;

// For a table, a pair of map of range start key -> decay since last replica of this range
// has had a replica selected on this tablet server and the decay since last replica of the
// table has had a replica selected on this tablet server.
typedef std::pair<std::unordered_map<std::string, MonoTime>, MonoTime>
    LastReplicaDecayByRangesPerTable;

// Map of table id -> decay since last replica of each range in the table and
// the table itself has had a replica selected on this tablet server.
typedef std::unordered_map<std::string, LastReplicaDecayByRangesPerTable> LastReplicaDecayByTable;

// Master-side view of a single tablet server.
//
// Tracks the last heartbeat, status, instance identifier, location, etc.
// This class is thread-safe.
class TSDescriptor : public enable_make_shared<TSDescriptor> {
 public:
  static Status RegisterNew(const NodeInstancePB& instance,
                            const ServerRegistrationPB& registration,
                            const std::optional<std::string>& location,
                            DnsResolver* dns_resolver,
                            std::shared_ptr<TSDescriptor>* desc);

  virtual ~TSDescriptor() = default;

  // Set the last-heartbeat time to now.
  void UpdateHeartbeatTime();

  // Set whether a full tablet report is needed.
  void UpdateNeedsFullTabletReport(bool needs_report);

  // Whether a full tablet report is needed from this tablet server.
  bool needs_full_report() const;

  // Return the amount of time since the last heartbeat received
  // from this TS.
  MonoDelta TimeSinceHeartbeat() const;

  // Return whether this server is presumed dead based on last heartbeat time.
  bool PresumedDead() const;

  // Register this tablet server.
  Status Register(const NodeInstancePB& instance,
                  const ServerRegistrationPB& registration,
                  const std::optional<std::string>& location,
                  DnsResolver* dns_resolver);

  const std::string &permanent_uuid() const { return permanent_uuid_; }
  int64_t latest_seqno() const;

  // Copy the current registration info into the given PB object.
  // A safe copy is returned because the internal Registration object
  // may be mutated at any point if the tablet server re-registers.
  // If 'use_external_addr' is 'true', return information targeted to clients
  // in the external networks from where RPCs are proxied into the cluster's
  // internal network.
  // Return Status::OK() if the registration has been successfully filled in,
  // non-OK if misconfiguration related to the usage of external addresses
  // has been detected.
  Status GetRegistration(ServerRegistrationPB* reg,
                         bool use_external_addr = false) const;

  void GetTSInfoPB(TSInfoPB* tsinfo_pb, bool use_external_addr) const;

  void GetNodeInstancePB(NodeInstancePB* instance_pb) const;

  // Return an RPC proxy to the tablet server admin service.
  Status GetTSAdminProxy(const std::shared_ptr<rpc::Messenger>& messenger,
                         std::shared_ptr<tserver::TabletServerAdminServiceProxy>* proxy);

  // Return an RPC proxy to the consensus service.
  Status GetConsensusProxy(const std::shared_ptr<rpc::Messenger>& messenger,
                           std::shared_ptr<consensus::ConsensusServiceProxy>* proxy);

  // Increment the accounting of the number of replicas recently created on this
  // server. This value will automatically decay over time.
  void IncrementRecentReplicaCreations();

  // Increment the accounting of the number of replicas from the range 'range_key_start'
  // from the table 'table_id' recently created on this server. Also increments the accounting of
  // the number of replicas from 'table_id' recently created on this server.
  // These values will automatically decay over time.
  void IncrementRecentReplicaCreationsByRangeAndTable(const std::string& range_key_start,
                                                      const std::string& table_id);

  // Return the number of replicas which have recently been created on this
  // TS. This number is incremented when replicas are placed on the TS, and
  // then decayed over time. This method is not 'const' because each call
  // actually performs the time-based decay.
  double RecentReplicaCreations();

  // Return the number of replicas from the range identified by 'range_key_start' from the
  // table 'table_id' which have recently been created on this TS. This number is incremented
  // when replicas from this range are placed on the TS, and then decayed over time. This method
  // is not 'const' because each call actually performs the time-based decay.
  double RecentReplicaCreationsByRange(const std::string& range_key_start,
                                       const std::string& table_id);

  // Return the number of replicas from the table identified by
  // 'table_id' which have recently been created on this TS. This number is
  // incremented when replicas from this table are placed on the TS, and then
  // decayed over time. This method is not 'const' because
  // each call actually performs the time-based decay.
  double RecentReplicaCreationsByTable(const std::string& table_id);

  // Set the number of live replicas (i.e. running or bootstrapping).
  void set_num_live_replicas(int n) {
    DCHECK_GE(n, 0);
    std::lock_guard l(lock_);
    num_live_replicas_ = n;
  }

  // Set the number of live replicas in each dimension.
  void set_num_live_replicas_by_dimension(TabletNumByDimensionMap num_live_tablets_by_dimension) {
    std::lock_guard l(lock_);
    num_live_tablets_by_dimension_ = std::move(num_live_tablets_by_dimension);
  }

  // Set the number of live replicas per range for each table.
  void set_num_live_replicas_by_range_per_table(
      std::string table_id,
      TabletNumByRangeMap num_live_tablets_by_table) {
    std::lock_guard l(lock_);
    num_live_tablets_by_range_per_table_.emplace(table_id, num_live_tablets_by_table);
  }

  // Return the number of live replicas (i.e. running or bootstrapping).
  // If dimension is none, return the total number of replicas in the tablet server.
  // Otherwise, return the number of replicas in the dimension.
  int num_live_replicas(const std::optional<std::string>& dimension = std::nullopt) const {
    std::shared_lock l(lock_);
    if (dimension) {
      int32_t num_live_tablets = 0;
      if (num_live_tablets_by_dimension_) {
        ignore_result(FindCopy(*num_live_tablets_by_dimension_, *dimension, &num_live_tablets));
      }
      return num_live_tablets;
    }
    return num_live_replicas_;
  }

  // Return the number of live replicas (i.e. running or bootstrapping)
  // in the given range for the given table.
  int num_live_replicas_by_range(const std::string& range_key, const std::string& table_id) const {
    std::shared_lock l(lock_);
    int32_t num_live_tablets_by_range = 0;
    if (const auto* ranges = FindOrNull(
          num_live_tablets_by_range_per_table_, table_id); ranges != nullptr) {
      ignore_result(FindCopy(*ranges, range_key, &num_live_tablets_by_range));
    }
    return num_live_tablets_by_range;
  }

  // Return the number of live replicas (i.e. running or bootstrapping) in the given table.
  int num_live_replicas_by_table(const std::string& table_id) const {
    std::shared_lock l(lock_);
    int32_t num_live_tablets_by_table = 0;
    if (ContainsKey(num_live_tablets_by_range_per_table_, table_id)) {
      auto ranges = FindOrDie(num_live_tablets_by_range_per_table_, table_id);
      for (const auto& range : ranges) {
        num_live_tablets_by_table += range.second;
      }
    }
    return num_live_tablets_by_table;
  }

  // Return the location of the tablet server. This returns a safe copy
  // since the location could change at any time if the tablet server
  // re-registers.
  std::optional<std::string> location() const {
    std::shared_lock l(lock_);
    return location_;
  }

  // Return a string form of this TS, suitable for printing.
  // Includes the UUID as well as last known host/port.
  std::string ToString() const;

 protected:
  explicit TSDescriptor(std::string perm_id);

 private:
  FRIEND_TEST(TestTSDescriptor, TestReplicaCreationsDecay);
  friend class AutoRebalancerTest;
  friend class PlacementPolicyTest;

  // Uses DNS to resolve registered hosts to a single Sockaddr.
  // Returns the resolved address as well as the hostname associated with it
  // in 'addr' and 'host'.
  Status ResolveSockaddr(Sockaddr* addr, std::string* host) const;

  void DecayRecentReplicaCreationsUnlocked();

  void DecayRecentReplicaCreationsByRangeUnlocked(const std::string& range_start_key,
                                                  const std::string& table_id);

  void DecayRecentReplicaCreationsByTableUnlocked(const std::string& range_start_key);

  void AssignLocationForTesting(std::string loc) {
    location_ = std::move(loc);
  }

  mutable rw_spinlock lock_;

  const std::string permanent_uuid_;
  int64_t latest_seqno_;

  // The last time a heartbeat was received for this node.
  MonoTime last_heartbeat_;

  // Whether the tablet server needs to send a full report.
  bool needs_full_report_;

  // The number of times this tablet server has recently been selected to create a
  // tablet replica. This value decays back to 0 over time.
  double recent_replica_creations_;
  MonoTime last_replica_creations_decay_;

  // A map that contains the number of times this tablet server has recently been selected to
  // create a tablet replica per range and table. This value decays back to 0 over time.
  RecentReplicasByTable recent_replicas_by_range_;
  LastReplicaDecayByTable last_replica_decay_by_range_;

  // The time this tablet server was started, used to calculate decay for
  // recent replica creations by range and table.
  MonoTime init_time_;

  // The number of live replicas on this host, from the last heartbeat.
  int num_live_replicas_;

  // The number of live replicas in each dimension, from the last heartbeat.
  std::optional<TabletNumByDimensionMap> num_live_tablets_by_dimension_;

  // The number of live replicas for each range for each table on this host from the last heartbeat.
  TabletNumByRangePerTableMap num_live_tablets_by_range_per_table_;

  // The tablet server's location, as determined by the master at registration.
  std::optional<std::string> location_;

  std::unique_ptr<ServerRegistrationPB> registration_;

  std::shared_ptr<tserver::TabletServerAdminServiceProxy> ts_admin_proxy_;
  std::shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;
  DnsResolver* dns_resolver_;

  DISALLOW_COPY_AND_ASSIGN(TSDescriptor);
};

// Alias for a vector of tablet server descriptors.
typedef std::vector<std::shared_ptr<TSDescriptor>> TSDescriptorVector;

} // namespace master
} // namespace kudu
