#!/bin/bash
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "LicensBK_STREAM_STORAGE_ROOT_PATHe");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# set -e

BOOKIE_PORT=${bookiePort:-${BOOKIE_PORT}}
BOOKIE_PORT=${BOOKIE_PORT:-3181}
BK_zkServers=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/;/,/g')
ZK_URL=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/,/;/g')
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}/ledgers"
BK_DIR="/bk"

export BOOKIE_PORT=${BOOKIE_PORT}
export BK_zkServers=${BK_zkServers}
export BK_metadataServiceUri=zk://${ZK_URL}${BK_LEDGERS_PATH}
export BK_journalDirectories=${BK_journalDirectories:-${BK_DIR}/journal}
export BK_ledgerDirectories=${BK_ledgerDirectories:-${BK_DIR}/ledgers}
export BK_indexDirectories=${BK_DIR}/index
export BK_CLUSTER_ROOT_PATH=/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}

export BK_tlsProvider=OpenSSL
export BK_tlsKeyStoreType=JKS
export BK_tlsKeyStore=/var/private/tls/bookie.keystore.jks
export BK_tlsKeyStorePasswordPath=/var/private/tls/bookie.keystore.passwd
export BK_tlsTrustStoreType=JKS
export BK_tlsTrustStore=/var/private/tls/bookie.truststore.jks
export BK_tlsTrustStorePasswordPath=/var/private/tls/bookie.truststore.passwd

export BK_STREAM_STORAGE_ROOT_PATH=${BK_STREAM_STORAGE_ROOT_PATH:-"/stream"}

# To create directories for multiple ledgers and journals if specified
create_bookie_dirs() {
  IFS=',' read -ra directories <<< $1
  for i in "${directories[@]}"
  do
      mkdir -p $i
      if [ "$(id -u)" = '0' ]; then
          chown -R "${BK_USER}:${BK_USER}" $i
      fi
  done
}

wait_for_zookeeper() {
    echo "Waiting for zookeeper"
    until zk-shell --run-once "ls /" ${BK_zkServers}; do sleep 5; done
    echo "Done waiting for Zookeeper"
}


create_zk_root() {
    if [ "x${BK_CLUSTER_ROOT_PATH}" != "x" ]; then
        echo "Creating the zk root dir '${BK_CLUSTER_ROOT_PATH}' at '${BK_zkServers}'"
        # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} create ${BK_CLUSTER_ROOT_PATH}
        zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH} '' false false true" ${BK_zkServers}
        zk-shell --run-once "create ${BK_STREAM_STORAGE_ROOT_PATH} '' false false true" ${BK_zkServers}
        echo "Done creating the zk root dir"
    fi
}

configure_bk() {
    # We need to update the metadata endpoint and Bookie ID before attempting to delete the cookie
    sed -i "s|.*metadataServiceUri=.*\$|metadataServiceUri=${BK_metadataServiceUri}|" /opt/bookkeeper/conf/bk_server.conf
    if [ ! -z "$BK_useHostNameAsBookieID" ]; then
      sed -i "s|.*useHostNameAsBookieID=.*\$|useHostNameAsBookieID=${BK_useHostNameAsBookieID}|" ${BK_HOME}/conf/bk_server.conf
    fi
}

format_bookie() {
    if [ `find $BK_journalDirectory $BK_ledgerDirectories $BK_indexDirectories -type f 2> /dev/null | wc -l` -gt 0 ]; then
      # The container already contains data in BK directories. This is probably because
      # the container has been restarted; or, if running on Kubernetes, it has probably been
      # updated or evacuated without losing its persistent volumes.
      echo "Data available in bookkeeper directories; not formatting the bookie"
    else


      # The container does not contain any BK data, it is probably a new
      # bookie. We will format any pre-existent data and metadata before starting
      # the bookie to avoid potential conflicts.
      echo "Formatting bookie data and metadata"
      /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie || true
    fi
}

format_zk_metadata() {
    export BOOKIE_CONF=/opt/bookkeeper/conf/bk_server.conf
    export SERVICE_PORT=$BOOKIE_PORT
    echo "Formatting zk metadata. Will ignore any errors if the formatting is already done."
    /opt/bookkeeper/bin/bookkeeper shell metaformat -nonInteractive || true
    echo "Done formatting zk metadata "
}

# Init the cluster if required znodes not exist in Zookeeper.
# Use ephemeral zk node as lock to keep initialize atomic.
function init_cluster() {
    if [ "x${BK_STREAM_STORAGE_ROOT_PATH}" == "x" ]; then
        echo "BK_STREAM_STORAGE_ROOT_PATH is not set. fail fast."
        exit -1
    fi

    # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}
    echo "Executing 'zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}'"
    zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}
    #if [ $? -eq 0 ]; then
     #   echo "Metadata of cluster already exists, no need to init"
    # else
        # create ephemeral zk node bkInitLock, initiator who this node, then do init; other initiators will wait.
        # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} create -e ${BK_CLUSTER_ROOT_PATH}/bkInitLock
        echo "Executing 'zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock '' true false false" ${BK_zkServers}'"
        zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock '' true false false" ${BK_zkServers}
        if [ $? -eq 0 ]; then
            # bkInitLock created success, this is the successor to do znode init
            echo "Initializing bookkeeper cluster at service uri ${BK_metadataServiceUri}."
            /opt/bookkeeper/bin/bkctl --service-uri ${BK_metadataServiceUri} cluster init
            if [ $? -eq 0 ]; then
                echo "Successfully initialized bookkeeper cluster at service uri ${BK_metadataServiceUri}."
            else
                echo "Failed to initialize bookkeeper cluster at service uri ${BK_metadataServiceUri}. please check the reason."
                exit
            fi
        else
            echo "Other docker instance is doing initialize at the same time, will wait in this instance."
            tenSeconds=1
            while [ ${tenSeconds} -lt 10 ]
            do
                sleep 10
                # echo "run '/opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}'"
                # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}
                echo "zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}"
                zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}
                if [ $? -eq 0 ]; then
                    echo "Waited $tenSeconds * 10 seconds, bookkeeper inited"
                    break
                else
                    echo "Waited $tenSeconds * 10 seconds, still not init"
                    (( tenSeconds++ ))
                    continue
                fi
            done

            if [ ${tenSeconds} -eq 10 ]; then
                echo "Waited 100 seconds for bookkeeper cluster init, something wrong, please check"
                exit
            fi
        fi
    # fi
}

echo "Creating directories for journal and ledgers"
create_bookie_dirs "${BK_journalDirectories}"
create_bookie_dirs "${BK_ledgerDirectories}"

echo "Waiting for Zookeeper to come up"
wait_for_zookeeper

echo "Creating Zookeeper root"
create_zk_root

echo "Creating Zookeeper metadata"
# format_zk_metadata

configure_bk

# echo "Formatting bookie if necessary"
# format_bookie

echo "Initializing Cluster"
# init_cluster

echo "Starting bookie"
#/bin/sh
#/opt/bookkeeper/scripts/entrypoint.sh bookie
# /opt/bookkeeper/bin/bookkeeper bookie
tail -f /dev/null
