/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.test.common.SecurityConfigDefaults;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ZkTlsEnabledInProcPravegaClusterTest {

    @SneakyThrows
    @Test
    public void runInProcPravegaClusterWithZkTlsEnabled() {
        InProcPravegaCluster inProcCluster = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .secureZK(true)
                .zkUrl("localhost:" + 4001)
                .zkPort(4001)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .enableRestServer(true)
                .restServerPort(9092)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableTls(true)
                .enableAuth(true)
                .keyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                .certFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                .jksKeyFile(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_PATH)
                .jksTrustFile(SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH)
                .keyPasswordFile(SecurityConfigDefaults.TLS_PASSWORD_PATH)
                .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD)
                .passwdFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                .build();

        inProcCluster.setControllerPorts(new int[]{9090});
        inProcCluster.setSegmentStorePorts(new int[]{6000});

        log.info("Starting in-proc Cluster...");
        inProcCluster.start();
        log.info("Done starting in-proc Cluster.");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown Hook is running...");
                try {
                    inProcCluster.close();
                } catch (Exception e) {
                    log.warn("Failed to close the cluster", e);
                }
            }
        });
        log.info("Application terminating...");
    }
}
