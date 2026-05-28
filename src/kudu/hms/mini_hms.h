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
#include <map>
#include <memory>
#include <string>

#include "kudu/rpc/sasl_common.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Subprocess;

namespace hms {

class MiniHms {
 public:

  // Build path to the file with trusted CA certificates for the specified
  // data root.
  static std::string BuildCaCertFilePath(const std::string& data_root);

  MiniHms();

  ~MiniHms();

  // Configures the mini HMS to use Kerberos.
  void EnableKerberos(std::string krb5_conf,
                      std::string service_principal,
                      std::string keytab_file,
                      rpc::SaslProtection::Type protection);

  // Configures whether the mini HMS uses SSL/TLS for its Thrift interface.
  void EnableTls(bool enable);

  // Configures the mini HMS to enable or disable the Kudu plugin.
  void EnableKuduPlugin(bool enable);

  // Add extra environment variables to the HMS environment.
  void AddEnvVar(const std::string& key, const std::string& value);

  // Configures the mini HMS to store its data in the provided path. If not set,
  // it uses a test-only temporary directory.
  void SetDataRoot(std::string data_root);

  // Starts the mini Hive metastore.
  //
  // If the MiniHms has already been started and stopped, it will be restarted
  // using the same listening port.
  Status Start();

  // Stops the mini Hive metastore.
  Status Stop();

  // Pause the Hive metastore process.
  Status Pause();

  // Unpause the Hive metastore process.
  Status Resume();

  // Delete the HMS database directory.
  Status DeleteDatabaseDir();

  // Returns the address of the Hive metastore. Should only be called after the
  // metastore is started.
  HostPort address() const {
    return HostPort("127.0.0.1", port_);
  }

  // Returns the Metastore URIs, in the format that the Hive
  // hive.metastore.uris configuration expects.
  std::string uris() const;

  // Returns true when Kerberos is enabled.
  bool IsKerberosEnabled() const {
    return !keytab_file_.empty();
  }

  // Returns true when SSL/TLS is enabled.
  bool IsTlsEnabled() const {
    return tls_enabled_;
  }

  // Returns absolute path to the file with the CA certificate (PEM format)
  // that the Thrift server's TLS certificate is signed with.
  std::string ca_cert_file_path() const;

  // Returns absolute path to HMS's JKS CA keystore location.
  std::string ca_keystore_path() const;

 private:

  // Creates a security.properties file for use via `-Djava.security.properties` in the mini HMS.
  Status CreateSecurityProperties() const;

  // Creates a hive-site.xml for the mini HMS.
  Status CreateHiveSite() const;

  // Creates a core-site.xml for the mini HMS.
  Status CreateCoreSite() const;

  // Creates a log4j2 configuration properties file for the mini HMS.
  Status CreateLogConfig() const;

  // Creates the keystore files required for the mini HMS's SSL/TLS endpoint.
  Status CreateTlsKeyStore(const std::string& java_home);

  std::unique_ptr<Subprocess> hms_process_;
  MonoDelta notification_log_ttl_ = MonoDelta::FromSeconds(86400);
  uint16_t port_ = 0;

  std::string data_root_;
  // Sub-directory under 'data_root_' where Derby keeps its database.
  std::string metadb_subdir_ = "metadb";

  // Kerberos configuration
  std::string krb5_conf_;
  std::string service_principal_;
  std::string keytab_file_;
  rpc::SaslProtection::Type protection_ = rpc::SaslProtection::kAuthentication;

  // TLS/SSL and keystore-related parameters required to enable TLS-protected
  // Thrift connections.
  bool tls_enabled_ = false;
  std::string key_store_path_;
  std::string key_store_password_;

  // Whether to enable the Kudu listener plugin.
  bool enable_kudu_plugin_ = true;

  std::map<std::string, std::string> extra_env_vars_ = {};
};

} // namespace hms
} // namespace kudu
