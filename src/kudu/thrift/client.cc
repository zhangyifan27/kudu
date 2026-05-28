// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/thrift/client.h"

#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>

#include <glog/logging.h>
#include <openssl/x509.h>
#include <thrift/TOutput.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TSocket.h>

#include "kudu/thrift/sasl_client_transport.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace apache {
namespace thrift {
namespace transport {
class TTransport;
}  // namespace transport
}  // namespace thrift
}  // namespace apache

using apache::thrift::TOutput;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::SSLProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TSSLSocketFactory;
using apache::thrift::transport::TTransport;
using std::make_shared;
using std::shared_ptr;

namespace kudu {
namespace thrift {

namespace {
// A logging callback for Thrift.
void ThriftOutputFunction(const char* output) {
  LOG(INFO) << output;
}
} // anonymous namespace

shared_ptr<TProtocol> CreateClientProtocol(const HostPort& address, const ClientOptions& options) {
  static std::once_flag set_thrift_logging_callback;
  std::call_once(set_thrift_logging_callback, [] {
    // Initialize the global Thrift logging callback.
    TOutput::instance().setOutputFunction(ThriftOutputFunction);

    // Kudu initializes the OpenSSL library on itself long before this helper
    // is being called and needs the library to be operational after all
    // TSSLSocketFactory and TSSLSocket instances are gone. That's the reason
    // to enable the manual mode for all TSSLSocketFactory instances. It's
    // assumed (and it's indeed so at time of writing) this is the only place
    // in a Kudu process that sets OpenSSL initialization mode for the Thrift
    // library before any instance of TSSLSocket is created, so it's enough
    // to call it here once.
    TSSLSocketFactory::setManualOpenSSLInitialization(true);
  });

  shared_ptr<TSocket> socket;

  // With manual initialization of TSSLSocketFactory it's possible to have no
  // restrictions on the lifespan for its instances: they don't have to outlive
  // TSSLSocket objects created by them.
  if (options.enable_tls) {
    // When TLS is enabled, load trusted CA certificates from the explicitly
    // provided options.tls_trusted_ca_cert_file. Otherwise, load them from
    // the system-wide CA root certificate bundle.
    TSSLSocketFactory sf;
    sf.authenticate(true);  // always require valid certificates from TLS peers
    if (!options.tls_trusted_ca_cert_file.empty()) {
      // Load the explicitly provided CA certificates to trust.
      sf.loadTrustedCertificates(options.tls_trusted_ca_cert_file.c_str());
    } else {
      // Custom CA certificates to trust aren't provided: load the system-wide
      // CA root certificate bundle.
      const auto* const env_var_name = X509_get_default_cert_file_env();
      DCHECK(env_var_name);
      const auto* default_cert_file = getenv(env_var_name);
      if (!default_cert_file) {
        // Environment override isn't present, use the built-in location.
        default_cert_file = X509_get_default_cert_file();
      }
      if (default_cert_file && Env::Default()->FileExists(default_cert_file)) {
        sf.loadTrustedCertificates(default_cert_file);
      } else {
        // Misconfigured OpenSSL library or environment (e.g., Ubuntu 18).
        KLOG_EVERY_N_SECS(WARNING, 60) <<
            "trusted CA certificates aren't provided explicitly and the "
            "system-wide CA root certificate bundle location isn't available: "
            "HMS client won't be able to verify peer TLS certificates";
      }
    }
    socket = sf.createSocket(address.host(), address.port());
  } else {
    socket = make_shared<TSocket>(address.host(), address.port());
  }
  socket->setSendTimeout(options.send_timeout.ToMilliseconds());
  socket->setRecvTimeout(options.recv_timeout.ToMilliseconds());
  socket->setConnTimeout(options.conn_timeout.ToMilliseconds());

  shared_ptr<TTransport> transport;
  if (options.enable_kerberos) {
    DCHECK(!options.service_principal.empty());
    transport = make_shared<SaslClientTransport>(options.service_principal,
                                                 address.host(),
                                                 std::move(socket),
                                                 options.max_buf_size);
  } else {
    transport = make_shared<TBufferedTransport>(std::move(socket));
  }

  return make_shared<TBinaryProtocol>(std::move(transport));
}

bool IsFatalError(const Status& error) {
  // Whitelist of errors which are not fatal. This errs on the side of
  // considering an error fatal since the consequences are low; just an
  // unnecessary reconnect. If a fatal error is not recognized it could cause
  // another RPC to fail, since there is no way to check the status of the
  // connection before sending an RPC.
  return !(error.IsAlreadyPresent()
        || error.IsNotFound()
        || error.IsInvalidArgument()
        || error.IsIllegalState()
        || error.IsNotSupported()
        || error.IsRemoteError());
}
} // namespace thrift
} // namespace kudu
