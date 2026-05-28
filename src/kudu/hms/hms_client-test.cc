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

#include "kudu/hms/hms_client.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/thrift/client.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::optional;
using kudu::rpc::SaslProtection;
using std::make_pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace hms {

// This class is parameterized by a tuple/pair:
//   * SASL protection type
//   * TLS on/off
class HmsClientTest : public KuduTest,
                      public ::testing::WithParamInterface<
                          std::tuple<optional<SaslProtection::Type>, bool>> {
 public:

  static Status CreateTable(HmsClient* client,
                     const string& database_name,
                     const string& table_name,
                     const string& table_id,
                     const string& cluster_id) {
    hive::Table table;
    table.dbName = database_name;
    table.tableName = table_name;
    table.tableType = HmsClient::kManagedTable;
    table.__set_parameters({
        make_pair(HmsClient::kKuduTableIdKey, table_id),
        make_pair(HmsClient::kKuduTableNameKey, table_name),
        make_pair(HmsClient::kKuduClusterIdKey, cluster_id),
        make_pair(HmsClient::kKuduMasterAddrsKey, string("TODO")),
        make_pair(HmsClient::kStorageHandlerKey, HmsClient::kKuduStorageHandler)
    });
    table.sd.inputFormat = HmsClient::kKuduInputFormat;
    table.sd.outputFormat = HmsClient::kKuduOutputFormat;
    table.sd.serdeInfo.serializationLib = HmsClient::kKuduSerDeLib;

    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ make_pair(HmsClient::kKuduMasterEventKey, "true") });

    return client->CreateTable(table, env_ctx);
  }

  Status DropTable(HmsClient* client,
                   const string& database_name,
                   const string& table_name,
                   const string& table_id) {
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ make_pair(HmsClient::kKuduTableIdKey, table_id) });
    return client->DropTable(database_name, table_name, env_ctx);
  }

  void CheckMiniHmsTlsConfig(bool enable_tls) {
    MiniHms hms;
    const string data_root = GetTestPath(Substitute("hms-$0", enable_tls ? "tls" : "plain"));
    ASSERT_OK(env_->CreateDir(data_root));
    hms.SetDataRoot(data_root);
    hms.EnableKuduPlugin(false);
    hms.EnableTls(enable_tls);

    ASSERT_OK(hms.Start());
    SCOPED_CLEANUP({ WARN_NOT_OK(hms.Stop(), "failed to stop MiniHms"); });

    faststring hive_site;
    ASSERT_OK(ReadFileToString(env_, JoinPathSegments(data_root, "hive-site.xml"), &hive_site));
    if (enable_tls) {
      ASSERT_STR_CONTAINS(
          hive_site.ToString(),
          "<name>hive.metastore.use.SSL</name>\n    <value>true</value>");
    } else {
      ASSERT_STR_NOT_CONTAINS(hive_site.ToString(), "hive.metastore.use.SSL");
    }
    ASSERT_EQ(enable_tls, env_->FileExists(JoinPathSegments(data_root, "server-keystore.p12")));
  }

  static Status StartHmsNoCluster(MiniHms* hms, bool tls_enabled) {
    DCHECK(hms);
    hms->EnableTls(tls_enabled);

    // Set the `KUDU_HMS_SYNC_ENABLED` environment variable in the
    // HMS environment to manually enable HMS synchronization checks.
    // This means we don't need to stand up a Kudu Cluster for this test.
    hms->AddEnvVar("KUDU_HMS_SYNC_ENABLED", "1");
    return hms->Start();
  }
};

INSTANTIATE_TEST_SUITE_P(ProtectionTypes,
                         HmsClientTest,
                         ::testing::Combine(
                             ::testing::Values(std::nullopt
                                             , SaslProtection::kIntegrity
// On macos, krb5 has issues repeatedly spinning up new KDCs ('unable to reach
// any KDC in realm KRBTEST.COM, tried 1 KDC'). Integrity protection gives us
// good coverage, so we disable the other variants.
#ifndef __APPLE__
                                             , SaslProtection::kAuthentication
                                             , SaslProtection::kPrivacy
#endif
                             ),
                             ::testing::Values(false, true)));

TEST_P(HmsClientTest, TestHmsOperations) {
  const auto protection = std::get<0>(GetParam());
  const auto enable_tls = std::get<1>(GetParam());

  MiniHms hms;
  hms.SetDataRoot(GetTestDataDirectory());
  hms.EnableTls(enable_tls);

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = enable_tls;
  if (enable_tls) {
    hms_client_opts.tls_trusted_ca_cert_file = hms.ca_cert_file_path();
  }

  MiniKdc kdc;
  if (protection) {
    ASSERT_OK(kdc.Start());

    string spn = "hive/127.0.0.1";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab(spn, &ktpath));

    ASSERT_OK(rpc::SaslInit());
    hms.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"],
                       spn,
                       ktpath,
                       *protection);

    ASSERT_OK(kdc.CreateUserPrincipal("alice"));
    ASSERT_OK(kdc.Kinit("alice"));
    ASSERT_OK(kdc.SetKrb5Environment());
    hms_client_opts.enable_kerberos = true;
    hms_client_opts.service_principal = "hive";
  }

  // Set the `KUDU_HMS_SYNC_ENABLED` environment variable in the
  // HMS environment to manually enable HMS synchronization checks.
  // This means we don't need to stand up a Kudu Cluster for this test.
  hms.AddEnvVar("KUDU_HMS_SYNC_ENABLED", "1");

  ASSERT_OK(hms.Start());

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_FALSE(client->IsConnected());
  ASSERT_OK(client->Start());

  // TTransport::isOpen() returns false for TLS-enabled connections because of
  // how TSSLSocket::isOpen() is implemented. It checks the 'TSSLSocket::ssl_'
  // field, and the field is populated by the very first TLS handshake.
  // TTransport::open() doesn't start a TLS handshake on its own.  Instead,
  // the TLS handshake runs on the very first attempt to read or write from the
  // underlying socket. For a TLS-protected connection, client.IsConnected()
  // should return 'true' after the very first I/O on the underlying socket
  // before it's shut down.
  ASSERT_TRUE(enable_tls || client->IsConnected());

  // Create a database.
  string database_name = "my_db";
  hive::Database db;
  db.name = database_name;
  ASSERT_OK(client->CreateDatabase(db));
  ASSERT_TRUE(client->IsConnected());
  ASSERT_TRUE(client->CreateDatabase(db).IsAlreadyPresent());

  // Get all databases.
  vector<string> databases;
  ASSERT_OK(client->GetAllDatabases(&databases));
  std::sort(databases.begin(), databases.end());
  EXPECT_EQ(vector<string>({ "default", database_name }), databases) << "Databases: " << databases;

  // Get a specific database.
  hive::Database my_db;
  ASSERT_OK(client->GetDatabase(database_name, &my_db));
  EXPECT_EQ(database_name, my_db.name) << "my_db: " << my_db;

  string table_name = "my_table";
  string table_id = "table-id";
  string cluster_id = "cluster-id";

  // Check that the HMS rejects Kudu tables without a table ID.
  ASSERT_STR_CONTAINS(
    CreateTable(client.get(), database_name, table_name, "", cluster_id).ToString(),
    "Kudu table entry must contain a table ID");

  // Create a table.
  ASSERT_OK(CreateTable(client.get(), database_name, table_name, table_id, cluster_id));
  ASSERT_TRUE(CreateTable(client.get(), database_name, table_name,
      table_id, cluster_id).IsAlreadyPresent());

  // Retrieve a table.
  hive::Table my_table;
  ASSERT_OK(client->GetTable(database_name, table_name, &my_table));
  EXPECT_EQ(database_name, my_table.dbName) << "my_table: " << my_table;
  EXPECT_EQ(table_name, my_table.tableName);
  EXPECT_EQ(table_id, my_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(cluster_id, my_table.parameters[HmsClient::kKuduClusterIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler, my_table.parameters[HmsClient::kStorageHandlerKey]);
  EXPECT_EQ(HmsClient::kManagedTable, my_table.tableType);
  EXPECT_EQ(HmsClient::kKuduInputFormat, my_table.sd.inputFormat);
  EXPECT_EQ(HmsClient::kKuduOutputFormat, my_table.sd.outputFormat);
  EXPECT_EQ(HmsClient::kKuduSerDeLib, my_table.sd.serdeInfo.serializationLib);

  string new_table_name = "my_altered_table";

  // Renaming the table with an incorrect table ID should fail.
  hive::Table altered_table(my_table);
  altered_table.tableName = new_table_name;
  altered_table.parameters[HmsClient::kKuduTableIdKey] = "bogus-table-id";
  ASSERT_TRUE(client->AlterTable(database_name, table_name, altered_table).IsRemoteError());

  // Rename the table.
  altered_table.parameters[HmsClient::kKuduTableIdKey] = table_id;
  ASSERT_OK(client->AlterTable(database_name, table_name, altered_table));

  // Original table is gone.
  ASSERT_TRUE(client->AlterTable(database_name, table_name, altered_table).IsIllegalState());

  // Check that the altered table's properties are intact.
  hive::Table renamed_table;
  ASSERT_OK(client->GetTable(database_name, new_table_name, &renamed_table));
  EXPECT_EQ(database_name, renamed_table.dbName);
  EXPECT_EQ(new_table_name, renamed_table.tableName);
  EXPECT_EQ(table_id, renamed_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler,
            renamed_table.parameters[HmsClient::kStorageHandlerKey]);
  EXPECT_EQ(HmsClient::kManagedTable, renamed_table.tableType);
  EXPECT_EQ(HmsClient::kKuduInputFormat, renamed_table.sd.inputFormat);
  EXPECT_EQ(HmsClient::kKuduOutputFormat, renamed_table.sd.outputFormat);
  EXPECT_EQ(HmsClient::kKuduSerDeLib, renamed_table.sd.serdeInfo.serializationLib);

  // Create a table with an uppercase name.
  string uppercase_table_name = "my_UPPERCASE_Table";
  ASSERT_OK(CreateTable(client.get(), database_name, uppercase_table_name,
      "uppercase-table-id", cluster_id));

  // Create a table with an illegal utf-8 name.
  ASSERT_TRUE(CreateTable(client.get(), database_name, "☃ sculptures 😉",
      table_id, cluster_id).IsInvalidArgument());

  // Create a non-Kudu table.
  hive::Table non_kudu_table;
  non_kudu_table.dbName = database_name;
  non_kudu_table.tableName = "non_kudu_table";
  non_kudu_table.parameters[HmsClient::kStorageHandlerKey] = "bogus.storage.Handler";
  ASSERT_OK(client->CreateTable(non_kudu_table));

  // Get all table names.
  vector<string> table_names;
  ASSERT_OK(client->GetTableNames(database_name, &table_names));
  std::sort(table_names.begin(), table_names.end());
  EXPECT_EQ(vector<string>({ new_table_name, "my_uppercase_table", "non_kudu_table" }), table_names)
      << "table names: " << table_names;

  // Get filtered table names.
  // NOTE: LIKE filters are used instead of = filters due to HIVE-21614
  table_names.clear();
  string filter = Substitute(
      "$0$1 LIKE \"$2\"", HmsClient::kHiveFilterFieldParams,
      HmsClient::kStorageHandlerKey, HmsClient::kKuduStorageHandler);
  ASSERT_OK(client->GetTableNames(database_name, filter, &table_names));
  std::sort(table_names.begin(), table_names.end());
  EXPECT_EQ(vector<string>({ new_table_name, "my_uppercase_table" }), table_names)
      << "table names: " << table_names;

  // Get multiple tables.
  vector<hive::Table> tables;
  ASSERT_OK(client->GetTables(database_name, table_names, &tables));
  ASSERT_EQ(2, tables.size());
  EXPECT_EQ(new_table_name, tables[0].tableName);
  EXPECT_EQ("my_uppercase_table", tables[1].tableName);

  // Check that the HMS rejects Kudu table drops with a bogus table ID.
  ASSERT_TRUE(DropTable(client.get(), database_name, new_table_name, "bogus-table-id").IsRemoteError());
  // Check that the HMS rejects non-existent table drops.
  ASSERT_TRUE(DropTable(client.get(), database_name, "foo-bar", "bogus-table-id").IsNotFound());

  // Drop a table.
  ASSERT_OK(DropTable(client.get(), database_name, new_table_name, table_id));

  // Drop the database.
  ASSERT_TRUE(client->DropDatabase(database_name).IsIllegalState());
  // TODO(HIVE-17008)
  // ASSERT_OK(client->DropDatabase(database_name, Cascade::kTrue));
  // TODO(HIVE-17008)
  // ASSERT_TRUE(client->DropDatabase(database_name).IsNotFound());

  int64_t event_id;
  ASSERT_OK(client->GetCurrentNotificationEventId(&event_id));

  // Retrieve the notification log and spot-check that the results look sensible.
  vector<hive::NotificationEvent> events;
  ASSERT_OK(client->GetNotificationEvents(-1, 100, &events));

  ASSERT_EQ(6, events.size());
  EXPECT_EQ("CREATE_DATABASE", events[0].eventType);
  EXPECT_EQ("CREATE_TABLE", events[1].eventType);
  EXPECT_EQ("ALTER_TABLE", events[2].eventType);
  EXPECT_EQ("CREATE_TABLE", events[3].eventType);
  EXPECT_EQ("CREATE_TABLE", events[4].eventType);
  EXPECT_EQ("DROP_TABLE", events[5].eventType);
  // TODO(HIVE-17008)
  //EXPECT_EQ("DROP_TABLE", events[6].eventType);
  //EXPECT_EQ("DROP_DATABASE", events[7].eventType);

  // Retrieve a specific notification log.
  events.clear();
  ASSERT_OK(client->GetNotificationEvents(2, 1, &events));
  ASSERT_EQ(1, events.size()) << "events: " << events;
  EXPECT_EQ("ALTER_TABLE", events[0].eventType);
  ASSERT_OK(client->Stop());
  ASSERT_FALSE(client->IsConnected());
}

TEST_P(HmsClientTest, TestLargeObjects) {
  const auto protection = std::get<0>(GetParam());
  const auto enable_tls = std::get<1>(GetParam());

  MiniHms hms;
  hms.SetDataRoot(GetTestDataDirectory());
  hms.EnableTls(enable_tls);

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = enable_tls;
  if (enable_tls) {
    hms_client_opts.tls_trusted_ca_cert_file = hms.ca_cert_file_path();
  }

  MiniKdc kdc;
  if (protection) {
    ASSERT_OK(kdc.Start());

    // Try a non-standard service principal to ensure it works correctly.
    string spn = "hive_alternate_sp/127.0.0.1";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab(spn, &ktpath));

    ASSERT_OK(rpc::SaslInit());
    hms.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"],
                       spn,
                       ktpath,
                       *protection);

    ASSERT_OK(kdc.CreateUserPrincipal("alice"));
    ASSERT_OK(kdc.Kinit("alice"));
    ASSERT_OK(kdc.SetKrb5Environment());
    hms_client_opts.enable_kerberos = true;
    hms_client_opts.service_principal = "hive_alternate_sp";
  }

  ASSERT_OK(hms.Start());

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  string database_name = "default";
  string table_name = "big_table";

  hive::Table table;
  table.dbName = database_name;
  table.tableName = table_name;
  table.tableType = HmsClient::kManagedTable;
  hive::FieldSchema partition_key;
  partition_key.name = "c1";
  partition_key.type = "int";
  table.partitionKeys.emplace_back(std::move(partition_key));
  table.sd.inputFormat = HmsClient::kKuduInputFormat;
  table.sd.outputFormat = HmsClient::kKuduOutputFormat;
  table.sd.serdeInfo.serializationLib = HmsClient::kKuduSerDeLib;

  ASSERT_OK(client->CreateTable(table));

  // Add a bunch of partitions to the table. This ensures we can send and
  // receive really large thrift objects. We have to add the partitions in small
  // batches, otherwise Derby chokes.
  const int kBatches = 40;
  const int kPartitionsPerBatch = 25;

  for (int batch_idx = 0; batch_idx < kBatches; batch_idx++) {
    vector<hive::Partition> partitions;
    for (int partition_idx = 0; partition_idx < kPartitionsPerBatch; partition_idx++) {
      hive::Partition partition;
      partition.dbName = database_name;
      partition.tableName = table_name;
      partition.values = { std::to_string(batch_idx * kPartitionsPerBatch + partition_idx) };
      partitions.emplace_back(std::move(partition));
    }

    ASSERT_OK(client->AddPartitions(database_name, table_name, std::move(partitions)));
  }

  ASSERT_OK(client->GetTable(database_name, table_name, &table));

  vector<hive::Partition> partitions;
  ASSERT_OK(client->GetPartitions(database_name, table_name, &partitions));
  ASSERT_EQ(kBatches * kPartitionsPerBatch, partitions.size());
}

TEST_F(HmsClientTest, TestHmsFaultHandling) {
  MiniHms hms;
  ASSERT_OK(hms.Start());

  thrift::ClientOptions options;
  options.recv_timeout = MonoDelta::FromMilliseconds(500),
  options.send_timeout = MonoDelta::FromMilliseconds(500);
  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), options, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Get a specific database.
  hive::Database my_db;
  ASSERT_OK(client->GetDatabase("default", &my_db));

  // Shutdown the HMS.
  ASSERT_OK(hms.Stop());
  ASSERT_TRUE(client->GetDatabase("default", &my_db).IsNetworkError());
  ASSERT_OK(client->Stop());

  // Restart the HMS and ensure the client can connect.
  ASSERT_OK(hms.Start());
  ASSERT_OK(client->Start());
  ASSERT_OK(client->GetDatabase("default", &my_db));

  // Pause the HMS and ensure the client times-out appropriately.
  ASSERT_OK(hms.Pause());
  ASSERT_TRUE(client->GetDatabase("default", &my_db).IsTimedOut());

  // Unpause the HMS and ensure the client can continue.
  ASSERT_OK(hms.Resume());
  ASSERT_OK(client->GetDatabase("default", &my_db));
}

// Verify the Hive Metastore's config file and the presence of related artifacts
// depending whether TLS is enabled. Make sure the Hive Metastore is able to
// start with the generated config files.
TEST_F(HmsClientTest, TestMiniHmsTlsConfig) {
  CheckMiniHmsTlsConfig(false);
  CheckMiniHmsTlsConfig(true);
}

// Test connecting the HMS client to TCP sockets in various invalid states.
TEST_F(HmsClientTest, TestHmsConnect) {
  Sockaddr loopback;
  ASSERT_OK(loopback.ParseString("127.0.0.1", 0));

  thrift::ClientOptions options;
  options.recv_timeout = MonoDelta::FromMilliseconds(100),
  options.send_timeout = MonoDelta::FromMilliseconds(100);
  options.conn_timeout = MonoDelta::FromMilliseconds(100);

  // This test will attempt to connect and transfer data upon starting the
  // client.
  options.verify_service_config = true;

  auto start_client = [&options] (const Sockaddr& addr) {
    unique_ptr<HmsClient> client;
    RETURN_NOT_OK(HmsClient::New(HostPort(addr), options, &client));
    return client->Start();
  };

  // Listening, but not accepting socket.
  Sockaddr listening;
  Socket listening_socket;
  ASSERT_OK(listening_socket.Init(loopback.family(), 0));
  ASSERT_OK(listening_socket.BindAndListen(loopback, 1));
  ASSERT_OK(listening_socket.GetSocketAddress(&listening));
  ASSERT_TRUE(start_client(listening).IsTimedOut());

  // Bound, but not listening socket.
  Sockaddr bound;
  Socket bound_socket;
  ASSERT_OK(bound_socket.Init(loopback.family(), 0));
  ASSERT_OK(bound_socket.Bind(loopback));
  ASSERT_OK(bound_socket.GetSocketAddress(&bound));
  ASSERT_TRUE(start_client(bound).IsNetworkError());

  // Unbound socket.
  Sockaddr unbound;
  Socket unbound_socket;
  ASSERT_OK(unbound_socket.Init(loopback.family(), 0));
  ASSERT_OK(unbound_socket.Bind(loopback));
  ASSERT_OK(unbound_socket.GetSocketAddress(&unbound));
  ASSERT_OK(unbound_socket.Close());
  ASSERT_TRUE(start_client(unbound).IsNetworkError());
}

TEST_F(HmsClientTest, TestDeserializeJsonTable) {
  string json = R"#({"1":{"str":"table_name"},"2":{"str":"database_name"}})#";
  hive::Table table;
  ASSERT_OK(HmsClient::DeserializeJsonTable(json, &table));
  ASSERT_EQ("table_name", table.tableName);
  ASSERT_EQ("database_name", table.dbName);
}

TEST_F(HmsClientTest, TestCaseSensitivity) {
  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/false));

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), {}, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Create a database.
  hive::Database db;
  db.name = "my_db";
  ASSERT_OK(client->CreateDatabase(db));

  // Create a table.
  ASSERT_OK(CreateTable(client.get(), "my_db", "Foo", "abc123", "Bar"));

  hive::Table table;
  ASSERT_OK(client->GetTable("my_db", "Foo", &table));
  ASSERT_EQ(table.tableName, "foo");

  ASSERT_OK(client->GetTable("my_db", "foo", &table));
  ASSERT_EQ(table.tableName, "foo");

  ASSERT_OK(client->GetTable("MY_DB", "FOO", &table));
  ASSERT_EQ(table.dbName, "my_db");
  ASSERT_EQ(table.tableName, "foo");
}

TEST_F(HmsClientTest, TlsEnabledOnlyOnServer) {
  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/true));

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = false;

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Try sending a request to the HMS.
  string uuid;
  const auto s = client->GetUuid(&uuid);
  // As expected, Thrift communication fails: the server expects TLS handshake
  // to start, but the client expects regular message exchange as it's done
  // over a non-TLS connection. The client misinterprets the first bytes
  // received back from the server as a header with a huge message size.
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to get HMS DB UUID");
  ASSERT_STR_CONTAINS(s.ToString(), "MaxMessageSize reached");

  // The client side is able to close the connection without an issue.
  ASSERT_TRUE(client->IsConnected());
  ASSERT_OK(client->Stop());
}

TEST_F(HmsClientTest, TlsEnabledOnlyOnClient) {
  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/false));

  // Enable TLS support in HmsClient, but don't provide information on trusted
  // CA certificates -- it's not needed since the communication should fail
  // before the verification of the server's certificate.
  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = true;

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Try sending a request to the HMS: it should fail with an error from the
  // TLS layer.
  string uuid;
  const auto s = client->GetUuid(&uuid);
  ASSERT_TRUE(s.IsNetworkError() || s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to get HMS DB UUID");
  ASSERT_STR_MATCHES(s.ToString(),
      "SSL_connect: (error code|unexpected eof while reading)");
}

// With TLS protection enabled and no trusted CA certificate information,
// the Thrift client implementation used by HmsClient rejects self-signed
// HMS server certificates.
TEST_F(HmsClientTest, TlsWithoutTrustedCACerts) {
  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/true));

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = true;

  // Intentionally not setting ClientOptions::tls_trusted_ca_cert_file field,
  // so there isn't trusted CA certificates provided to the Thrift client.
  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Try sending a request to the HMS: it should fail because the TLS handshake
  // should not accept a self-signed certificate without the signing CA's
  // certificate among the trusted ones.
  string uuid;
  const auto s = client->GetUuid(&uuid);
  ASSERT_TRUE(s.IsNetworkError() || s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to get HMS DB UUID");
  ASSERT_STR_CONTAINS(s.ToString(), "SSL_connect: certificate verify failed");
}

// With TLS protection enabled, provide path to a non-existent file and verify
// the failure to load the trusted CA by the HMS client is handled properly.
TEST_F(HmsClientTest, TlsNonExistentCACertsFile) {
  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = true;
  hms_client_opts.tls_trusted_ca_cert_file = JoinPathSegments(
      GetTestDataDirectory(), ".nonexistent");

  unique_ptr<HmsClient> client;
  const auto s = HmsClient::New({ "127.0.0.1", 0 }, hms_client_opts, &client);
  ASSERT_EQ(nullptr, client.get());
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not create HmsClient");
  ASSERT_STR_CONTAINS(s.ToString(), "SSL_CTX_load_verify_locations");
  ASSERT_STR_CONTAINS(s.ToString(), "no such file");
}

// The file expected to contain trusted CA certificates in PEM format does
// exist, but the data is in wrong format -- it's a PKCS#12 JKS keystore.
TEST_F(HmsClientTest, TlsCACertsFileWrongDataFormat) {
  // Start HMS with TLS support enabled to generate JKS keystore file.
  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/true));

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = true;
  // Point the CA cert file location to the HMS's CA keystore instead.
  // That's JSK file in PKCS#12 format, not a file in PEM format with
  // certificates.
  hms_client_opts.tls_trusted_ca_cert_file = hms.ca_keystore_path();

  unique_ptr<HmsClient> client;
  const auto s = HmsClient::New(hms.address(), hms_client_opts, &client);
  ASSERT_EQ(nullptr, client.get());
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not create HmsClient");
  ASSERT_STR_CONTAINS(s.ToString(), "SSL_CTX_load_verify_locations");
  ASSERT_STR_MATCHES(s.ToString(), "no certificate .* found");
  ASSERT_STR_MATCHES(s.ToString(), "no .* crl found");
}

// Provide a file with a CA certificate not matching the CA key that was used
// to sign the HMS server certificate -- the HMS client shouldn't be able
// to connect because of failed TLS negotiation.
TEST_F(HmsClientTest, TlsCACertsWrongCertificate) {
  const auto fpath = JoinPathSegments(GetTestDataDirectory(), "ca_cert_wrong.pem");
  ASSERT_OK(WriteStringToFile(Env::Default(), Slice(security::kCaCert), fpath));

  MiniHms hms;
  ASSERT_OK(StartHmsNoCluster(&hms, /*tls_enabled=*/true));

  thrift::ClientOptions hms_client_opts;
  hms_client_opts.enable_tls = true;
  hms_client_opts.tls_trusted_ca_cert_file = fpath;

  unique_ptr<HmsClient> client;
  ASSERT_OK(HmsClient::New(hms.address(), hms_client_opts, &client));
  ASSERT_NE(nullptr, client.get());
  ASSERT_OK(client->Start());

  // Try sending a request to the HMS: it should fail with corresponding
  // error from the OpenSSL library because of the real CA private key and the
  // provided CA certificate mismatch.
  string uuid;
  const auto s = client->GetUuid(&uuid);
  ASSERT_TRUE(s.IsNetworkError() || s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to get HMS DB UUID");
  ASSERT_STR_MATCHES(s.ToString(), "SSL_connect: certificate verify failed");
}

} // namespace hms
} // namespace kudu
