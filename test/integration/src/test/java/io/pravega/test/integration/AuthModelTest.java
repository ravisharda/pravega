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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.test.integration.utils.PasswordAuthHandlerInput;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Slf4j
public class AuthModelTest {

    @SneakyThrows
    @Test
    public void readingWithAccountWithReadWritePermissions() {
        List<String> messages = Arrays.asList("message 1", "message 2");
        List<String> readMessages = new ArrayList<>();

        String encryptedPassword = StrongPasswordProcessor.builder().build().encryptPassword("1111_aaaa");
        List<PasswordAuthHandlerInput.Entry> entries = Arrays.asList(
                PasswordAuthHandlerInput.Entry.of("superUser", encryptedPassword, "*,READ_UPDATE;"),
                PasswordAuthHandlerInput.Entry.of("streamUser", encryptedPassword, "testscope,READ_UPDATE;testscope/*,READ_UPDATE;")
        );

        try (ClusterWrapper pravegaCluster = new ClusterWrapper(true, "secret",
                600, entries, 4)) {
            pravegaCluster.initialize();

            String scopeName = "testscope";
            String streamName = "teststream";
            int numSegments = 1;

            ClientConfig adminClientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "superUser"))
                    .build();

            this.writeEvents(adminClientConfig, scopeName, streamName, numSegments, messages);

            ClientConfig streamUserClientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "streamUser"))
                    .build();

            String readerGroup = UUID.randomUUID().toString().replace("-", "");
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scopeName, streamName))
                    .disableAutomaticCheckpoints()
                    .build();

            @Cleanup
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, streamUserClientConfig);
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

            @Cleanup final EventStreamClientFactory clientFactory =
                    EventStreamClientFactory.withScope(scopeName, streamUserClientConfig);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(
                    "readerId", readerGroup,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());

            EventRead<String> event = null;
            do {
                event = reader.readNextEvent(2000);
                if (event.getEvent() != null) {
                    readMessages.add(event.getEvent());
                }
            } while (event.getEvent() != null);
        }
        assertSame(messages.size(), readMessages.size());
        assertEquals(messages.get(0), readMessages.get(0));
    }

    @SneakyThrows
    @Test
    public void readingWithAccountWithReadOnlyPermissions() {
        List<String> messages = Arrays.asList("message 1", "message 2");
        List<String> readMessages = new ArrayList<>();

        String encryptedPassword = StrongPasswordProcessor.builder().build().encryptPassword("1111_aaaa");
        List<PasswordAuthHandlerInput.Entry> entries = Arrays.asList(
                PasswordAuthHandlerInput.Entry.of("superUser", encryptedPassword, "*,READ_UPDATE;"),
                PasswordAuthHandlerInput.Entry.of("streamUser", encryptedPassword, "testscope,READ_UPDATE;testscope/*,READ;")
        );

        try (ClusterWrapper pravegaCluster = new ClusterWrapper(true, "secret",
                600, entries, 4)) {
            pravegaCluster.initialize();

            String scopeName = "testscope";
            String streamName = "teststream";
            int numSegments = 1;

            ClientConfig adminClientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "superUser"))
                    .build();
            this.writeEvents(adminClientConfig, scopeName, streamName, numSegments, messages);
            log.debug("Wrote events using user {}", "superUser");

            ClientConfig streamUserClientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "streamUser"))
                    .build();

            String readerGroup = UUID.randomUUID().toString().replace("-", "");
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scopeName, streamName))
                    .disableAutomaticCheckpoints()
                    .build();

            @Cleanup
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, streamUserClientConfig);
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

            @Cleanup final EventStreamClientFactory clientFactory =
                    EventStreamClientFactory.withScope(scopeName, streamUserClientConfig);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(
                    "readerId", readerGroup,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());

            EventRead<String> event = null;
            do {
                event = reader.readNextEvent(2000);
                if (event.getEvent() != null) {
                    readMessages.add(event.getEvent());
                }
            } while (event.getEvent() != null);
        }
        assertSame(messages.size(), readMessages.size());
        assertEquals(messages.get(0), readMessages.get(0));

    }

    private void writeEvents(ClientConfig clientConfig, String scopeName,
                             String streamName, int numSegments, List<String> messages) {
        createScopeStream(scopeName, streamName, numSegments, clientConfig);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);

        //@Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());

        messages.stream().forEach(message -> {
            writer.writeEvent(message).join();
            log.debug("Wrote a message '{}' to stream '{} / {}'", message, scopeName, streamName);
        });
    }

    private void createScopeStream(String scopeName, String streamName, int numSegments, ClientConfig clientConfig) {
        @Cleanup final StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);
        log.debug("Created stream manager.");

        boolean isScopeCreated = streamManager.createScope(scopeName);
        assertTrue("Failed to create scope", isScopeCreated);
        log.debug("Created the scope {}", scopeName);

        boolean isStreamCreated = streamManager.createStream(scopeName, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy
                        .fixed(numSegments))
                .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);
        log.debug("Created the stream {} in scope {}", streamName, scopeName);
    }
}


