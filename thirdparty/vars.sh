#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [ -z "$TP_DIR" ]; then
   echo "TP_DIR variable not set, check your scripts"
   exit 1
fi

TP_SOURCE_DIR="$TP_DIR/src"
TP_BUILD_DIR="$TP_DIR/build"

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=https://d3dr9sfxru4sde.cloudfront.net

# Third party dependency downloading URL, default to the CloudFront
# Distribution URL.
DEPENDENCY_URL=${DEPENDENCY_URL:-$CLOUDFRONT_URL_PREFIX}

PREFIX_COMMON=$TP_DIR/installed/common
PREFIX_DEPS=$TP_DIR/installed/uninstrumented
PREFIX_DEPS_TSAN=$TP_DIR/installed/tsan

GFLAGS_VERSION=2.2.2
GFLAGS_NAME=gflags-$GFLAGS_VERSION
GFLAGS_SOURCE=$TP_SOURCE_DIR/$GFLAGS_NAME

GLOG_VERSION=0.6.0
GLOG_NAME=glog-$GLOG_VERSION
GLOG_SOURCE=$TP_SOURCE_DIR/$GLOG_NAME

GMOCK_VERSION=1.12.1
GMOCK_NAME=googletest-release-$GMOCK_VERSION
GMOCK_SOURCE=$TP_SOURCE_DIR/$GMOCK_NAME

GPERFTOOLS_VERSION=2.8.1
GPERFTOOLS_NAME=gperftools-$GPERFTOOLS_VERSION
GPERFTOOLS_SOURCE=$TP_SOURCE_DIR/$GPERFTOOLS_NAME

PROTOBUF_VERSION=3.14.0
PROTOBUF_NAME=protobuf-$PROTOBUF_VERSION
PROTOBUF_SOURCE=$TP_SOURCE_DIR/$PROTOBUF_NAME

# Note: CMake gets patched on SLES12SP0. When changing the CMake version, please check if
# cmake-issue-15873-dont-use-select.patch needs to be updated.
CMAKE_VERSION=3.19.1
CMAKE_NAME=cmake-$CMAKE_VERSION
CMAKE_SOURCE=$TP_SOURCE_DIR/$CMAKE_NAME

SNAPPY_VERSION=1.1.8
SNAPPY_NAME=snappy-$SNAPPY_VERSION
SNAPPY_SOURCE=$TP_SOURCE_DIR/$SNAPPY_NAME

LZ4_VERSION=1.9.3
LZ4_NAME=lz4-$LZ4_VERSION
LZ4_SOURCE=$TP_SOURCE_DIR/$LZ4_NAME

# from https://github.com/kiyo-masui/bitshuffle
BITSHUFFLE_VERSION=0.3.5
BITSHUFFLE_NAME=bitshuffle-$BITSHUFFLE_VERSION
BITSHUFFLE_SOURCE=$TP_SOURCE_DIR/$BITSHUFFLE_NAME

ZLIB_VERSION=1.2.11
ZLIB_NAME=zlib-$ZLIB_VERSION
ZLIB_SOURCE=$TP_SOURCE_DIR/$ZLIB_NAME

LIBEV_VERSION=4.20
LIBEV_NAME=libev-$LIBEV_VERSION
LIBEV_SOURCE=$TP_SOURCE_DIR/$LIBEV_NAME

RAPIDJSON_VERSION=1.1.0
RAPIDJSON_NAME=rapidjson-$RAPIDJSON_VERSION
RAPIDJSON_SOURCE=$TP_SOURCE_DIR/$RAPIDJSON_NAME

# Hash of the squeasel git revision to use.
# (from http://github.com/cloudera/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SQUEASEL_VERSION=d83cf6d9af0e2c98c16467a6a035ae0d7ca21cb1
SQUEASEL_NAME=squeasel-$SQUEASEL_VERSION
SQUEASEL_SOURCE=$TP_SOURCE_DIR/$SQUEASEL_NAME

# Hash of the mustache git revision to use.
# (from https://github.com/henryr/cpp-mustache)
#
# To re-build this tarball use the following in the mustache repo:
#  export NAME=mustache-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
MUSTACHE_VERSION=b290952d8eb93d085214d8c8c9eab8559df9f606
MUSTACHE_NAME=mustache-$MUSTACHE_VERSION
MUSTACHE_SOURCE=$TP_SOURCE_DIR/$MUSTACHE_NAME

# git revision of google style guide:
# https://github.com/google/styleguide
# git archive --prefix=google-styleguide-$(git rev-parse HEAD)/ -o /tmp/google-styleguide-$(git rev-parse HEAD).tgz HEAD
GSG_VERSION=7a179d1ac2e08a5cc1622bec900d1e0452776713
GSG_NAME=google-styleguide-$GSG_VERSION
GSG_SOURCE=$TP_SOURCE_DIR/$GSG_NAME

GCOVR_VERSION=3.0
GCOVR_NAME=gcovr-$GCOVR_VERSION
GCOVR_SOURCE=$TP_SOURCE_DIR/$GCOVR_NAME

CURL_VERSION=7.68.0
CURL_NAME=curl-$CURL_VERSION
CURL_SOURCE=$TP_SOURCE_DIR/$CURL_NAME

# Hash of the crcutil git revision to use.
# (from http://github.com/cloudera/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=2903870057d2f1f109b245650be29e856dc8b646
CRCUTIL_NAME=crcutil-$CRCUTIL_VERSION
CRCUTIL_SOURCE=$TP_SOURCE_DIR/$CRCUTIL_NAME

LIBUNWIND_VERSION=1.5.0
LIBUNWIND_NAME=libunwind-$LIBUNWIND_VERSION
LIBUNWIND_SOURCE=$TP_SOURCE_DIR/$LIBUNWIND_NAME

# See package-llvm.sh for details on the LLVM tarball.
LLVM_VERSION=11.0.0
LLVM_NAME=llvm-$LLVM_VERSION.src
LLVM_SOURCE=$TP_SOURCE_DIR/$LLVM_NAME

# The include-what-you-use is built along with LLVM in its source tree.
IWYU_VERSION=0.15

# Python is required to build LLVM 3.6+ because it uses
# llvm/utils/llvm-build/llvmbuild script. It is only built and installed if
# the system Python version is less than 2.7.
PYTHON_VERSION=2.7.13
PYTHON_NAME=python-$PYTHON_VERSION
PYTHON_SOURCE=$TP_SOURCE_DIR/$PYTHON_NAME

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found in the 'kudu' branch of https://github.com/cloudera/catapult
# and built with "tracing/kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=21d76f8350fea2da2aa25cb6fd512703497d0c11
TRACE_VIEWER_NAME=kudu-trace-viewer-$TRACE_VIEWER_VERSION
TRACE_VIEWER_SOURCE=$TP_SOURCE_DIR/$TRACE_VIEWER_NAME

BOOST_VERSION=1_74_0
BOOST_NAME=boost_$BOOST_VERSION
BOOST_SOURCE=$TP_SOURCE_DIR/$BOOST_NAME

# The breakpad source artifact is created using the script found in
# scripts/make-breakpad-src-archive.sh
BREAKPAD_VERSION=9eac2058b70615519b2c4d8c6bdbfca1bd079e39
BREAKPAD_NAME=breakpad-$BREAKPAD_VERSION
BREAKPAD_SOURCE=$TP_SOURCE_DIR/$BREAKPAD_NAME

# Hash of the sparsehash-c11 git revision to use.
# (from http://github.com/sparsehash/sparsehash-c11)
#
# To re-build this tarball use the following in the sparsehash-c11 repo:
#  export NAME=sparsehash-c11-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SPARSEHASH_VERSION=cf0bffaa456f23bc4174462a789b90f8b6f5f42f
SPARSEHASH_NAME=sparsehash-c11-$SPARSEHASH_VERSION
SPARSEHASH_SOURCE=$TP_SOURCE_DIR/$SPARSEHASH_NAME

SPARSEPP_VERSION=1.22
SPARSEPP_NAME=sparsepp-$SPARSEPP_VERSION
SPARSEPP_SOURCE=$TP_SOURCE_DIR/$SPARSEPP_NAME

THRIFT_VERSION=0.11.0
THRIFT_NAME=thrift-$THRIFT_VERSION
THRIFT_SOURCE=$TP_SOURCE_DIR/$THRIFT_NAME

BISON_VERSION=3.5.4
BISON_NAME=bison-$BISON_VERSION
BISON_SOURCE=$TP_SOURCE_DIR/$BISON_NAME

# Note: The Hive release binary tarball is stripped of unnecessary jars before
# being uploaded. See thirdparty/package-hive.sh for details.
# ./thirdparty/package-hive.sh -d -r -v 3.1.2 apache-hive-3.1.2-bin
HIVE_VERSION=3.1.2
HIVE_NAME=hive-$HIVE_VERSION
HIVE_SOURCE=$TP_SOURCE_DIR/$HIVE_NAME

# Note: The Hadoop release tarball is stripped of unnecessary jars before being
# uploaded. See thirdparty/package-hadoop.sh for details.
HADOOP_VERSION=3.2.0
HADOOP_NAME=hadoop-$HADOOP_VERSION
HADOOP_SOURCE=$TP_SOURCE_DIR/$HADOOP_NAME

YAML_VERSION=0.6.2
YAML_NAME=yaml-cpp-yaml-cpp-$YAML_VERSION
YAML_SOURCE=$TP_SOURCE_DIR/$YAML_NAME

CHRONY_VERSION=3.5
CHRONY_NAME=chrony-$CHRONY_VERSION
CHRONY_SOURCE=$TP_SOURCE_DIR/$CHRONY_NAME

# Hash of the gumbo-parser git revision to use.
# (from https://github.com/google/gumbo-parser)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-parser-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_PARSER_VERSION=aa91b27b02c0c80c482e24348a457ed7c3c088e0
GUMBO_PARSER_NAME=gumbo-parser-$GUMBO_PARSER_VERSION
GUMBO_PARSER_SOURCE=$TP_SOURCE_DIR/$GUMBO_PARSER_NAME

# Hash of the gumbo-query git revision to use.
# (from https://github.com/lazytiger/gumbo-query)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-query-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_QUERY_VERSION=c9f10880b645afccf4fbcd11d2f62a7c01222d2e
GUMBO_QUERY_NAME=gumbo-query-$GUMBO_QUERY_VERSION
GUMBO_QUERY_SOURCE=$TP_SOURCE_DIR/$GUMBO_QUERY_NAME

POSTGRES_VERSION=12.2
POSTGRES_NAME=postgresql-$POSTGRES_VERSION
POSTGRES_SOURCE=$TP_SOURCE_DIR/$POSTGRES_NAME

POSTGRES_JDBC_VERSION=42.2.10
POSTGRES_JDBC_NAME=postgresql-$POSTGRES_JDBC_VERSION
POSTGRES_JDBC_SOURCE=$TP_SOURCE_DIR/$POSTGRES_JDBC_NAME

# If you need to rebuild the tarball for a specific hash instead of a release,
# run the following commands:
# mvn versions:set -DnewVersion=$(git rev-parse HEAD)
# mvn versions:update-child-modules
# mvn package -DskipTests
RANGER_VERSION=2.1.0
RANGER_NAME=ranger-$RANGER_VERSION-admin
RANGER_SOURCE=$TP_SOURCE_DIR/$RANGER_NAME

OATPP_VERSION=1.2.5
OATPP_NAME=oatpp-$OATPP_VERSION
OATPP_SOURCE=$TP_SOURCE_DIR/$OATPP_NAME

OATPP_SWAGGER_VERSION=1.2.5
OATPP_SWAGGER_NAME=oatpp-swagger-$OATPP_SWAGGER_VERSION
OATPP_SWAGGER_SOURCE=$TP_SOURCE_DIR/$OATPP_SWAGGER_NAME

JWT_CPP_VERSION=3bd600762a70faccc7ec1c2dacb999cba6c6ef5e
JWT_CPP=jwt-cpp
JWT_CPP_NAME=$JWT_CPP-$JWT_CPP_VERSION
JWT_CPP_SOURCE=$TP_SOURCE_DIR/$JWT_CPP_NAME
