#!/bin/bash

########################################################################
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
#
########################################################################

BUILDDIR="$PWD"
KUDU_HOME="$BUILDDIR/../.."
WEBSERVER_DOC_ROOT="$BUILDDIR/../../www"
KUDU_BUILD_TYPE="release"
KUDU_VERSION="1.16.x"
KUDU_PACKAGE_NAME="apache_kudu_${KUDU_VERSION}_${KUDU_BUILD_TYPE}"

# build kudu from source
../../build-support/enable_devtoolset.sh \
../../thirdparty/installed/common/bin/cmake \
-DCMAKE_BUILD_TYPE=${KUDU_BUILD_TYPE} -DNO_TESTS=1 ../..
make -j4

if [ -d "${KUDU_HOME}/${KUDU_PACKAGE_NAME}/" ]; then
  rm -rf ${KUDU_HOME}/${KUDU_PACKAGE_NAME}/
fi
mkdir ${KUDU_HOME}/${KUDU_PACKAGE_NAME}

# install binaries and libraries
make DESTDIR=${KUDU_HOME}/${KUDU_PACKAGE_NAME} install

# pack_server
mv ${KUDU_HOME}/${KUDU_PACKAGE_NAME}/usr/local/* ${KUDU_HOME}/${KUDU_PACKAGE_NAME}
rm -rf ${KUDU_HOME}/${KUDU_PACKAGE_NAME}/usr/
cp -r ${WEBSERVER_DOC_ROOT} /${KUDU_HOME}/${KUDU_PACKAGE_NAME}
tar -zcvf ${KUDU_HOME}/${KUDU_PACKAGE_NAME}.tar.gz ${KUDU_HOME}/${KUDU_PACKAGE_NAME}
