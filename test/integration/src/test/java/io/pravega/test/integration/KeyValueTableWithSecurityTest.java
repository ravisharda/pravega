/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.test.integration.utils.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class KeyValueTableWithSecurityTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(200);

    @Test
    public void putAnEntryWithAuthEnabled() {
        String username = "allPowerfulUser";
        String pwd = "super-secret";
        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put(username, "prn::*,READ_UPDATE");

        @Cleanup
        final ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tokenSigningKeyBasis("secret").tokenTtlInSeconds(600)
                .rgWritesWithReadPermEnabled(true)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, pwd))
                .build();
        cluster.initialize();

        String scopeName = "testScope" + Math.random();
        String tableName = "testTable";
        String keyFamily = "profile";
        String key = "firstName";
        String value = "john";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(pwd, username))

                .build();
        TestUtils.createScope(clientConfig, scopeName);
        log.debug("Created the scope {}", scopeName);

        KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);
        log.debug("Created the KeyValueTableManager");

        boolean result = keyValueTableManager.createKeyValueTable(scopeName, tableName,
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        log.debug("Created the table {}", tableName);
        assertTrue(result);

        KeyValueTableFactory factory = KeyValueTableFactory.withScope(scopeName, clientConfig);
        KeyValueTable<String, String> table =
                factory.forKeyValueTable(tableName, new UTF8StringSerializer(), new UTF8StringSerializer(),
                        KeyValueTableClientConfiguration.builder().build());
        log.debug("Created table {}", tableName);

        table.put(keyFamily, key, value).join();
        log.debug("Added an entry to the table", tableName);

        TableEntry<String, String> entry = table.get(keyFamily, key).join();
        log.debug("Retrieved an entry from the table", tableName);

        assertNotNull(entry);
        assertEquals(value, entry.getValue());
    }

}
