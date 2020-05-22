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

import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
public class BookkeeperDataLogWriteTest {
    private static final int WRITE_MAX_LENGTH = 200;
    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT = 500;
    private static final int BOOKIE_COUNT = 1;
    private static final int THREAD_POOL_SIZE = 3;
    private static final int MAX_WRITE_ATTEMPTS = 3;
    private static final int MAX_LEDGER_SIZE = WRITE_MAX_LENGTH * Math.max(10, WRITE_COUNT / 20);
    private static final int ZOOKEEPER_PORT = TestUtils.getAvailableListenPort();
    private static final Duration TIMEOUT = Duration.ofMillis(60 * 1000);
    private static final int WRITE_MIN_LENGTH = 20;

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static BookKeeperServiceRunner BOOKIES_RUNNER;

    private final Random random = new Random(0);
    private final AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
    private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private final AtomicReference<DurableDataLogFactory> factory = new AtomicReference<>();

    private ScheduledExecutorService executorService = null;

    @SneakyThrows
    @BeforeClass
    public static void setupEnv() {
        log.debug("setupEnv entry");
        List<Integer> bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        BOOKIES_RUNNER = BookKeeperServiceRunner.builder()
                .startZk(true)
                .zkPort(ZOOKEEPER_PORT)
                .ledgersPath("/pravega/bookkeeper/ledgers")
                .secureBK(false)
                .secureZK(false)
                .bookiePorts(bookiePorts)
                .build();

        BOOKIES_RUNNER.startAll();
        log.debug("setupEnv exit");
    }

    /**
     * Before each test, we create a new namespace; this ensures that data created from a previous test does not leak
     * into the current one (namespaces cannot be deleted (at least not through the API)).
     */
    @Before
    public void setup() throws Exception {
        log.debug("setup entry");
        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + ZOOKEEPER_PORT)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build());
        this.zkClient.get().start();

        // Setup bkConfig to use the port and namespace.
        this.bkConfig.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + ZOOKEEPER_PORT)
                .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, MAX_LEDGER_SIZE)
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                .build());

        this.executorService = executorService();

        // Create default factory.
        DurableDataLogFactory factory = new BookKeeperLogFactory(this.bkConfig.get(), this.zkClient.get(),
                this.executorService);
        factory.initialize();
        this.factory.set(factory);

        log.debug("setup exit");
    }

    private ScheduledExecutorService executorService() {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }

    @SneakyThrows
    @After
    public void tearDown() {
        log.debug("teardown entry");
        this.executorService.shutdownNow();
        this.executorService.awaitTermination(5, TimeUnit.SECONDS);

        DurableDataLogFactory dataLogFactory = this.factory.getAndSet(null);
        if (dataLogFactory != null) {
            dataLogFactory.close();
        }

        val zkClient = this.zkClient.getAndSet(null);
        if (zkClient != null) {
            zkClient.close();
        }
    }

    @SneakyThrows
    @Test
    public void readMultipleItems() {
        TreeMap<LogAddress, byte[]> writeData;
        try (DurableDataLog dataLog = this.factory.get().createDurableDataLog(CONTAINER_ID)) {
            // dataLog.enable();
            dataLog.initialize(TIMEOUT);
            writeData = this.populate(dataLog, 10);
        }

        try (DurableDataLog dataLog = this.factory.get().createDurableDataLog(CONTAINER_ID)) {
            // dataLog.enable();
            dataLog.initialize(TIMEOUT);
            verifyReads(dataLog, writeData);
        }
    }

    private void verifyReads(DurableDataLog log, TreeMap<LogAddress, byte[]> writeData) throws Exception {
        @Cleanup
        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader();
        Iterator<Map.Entry<LogAddress, byte[]>> expectedIterator = writeData.entrySet().iterator();
        while (true) {
            DurableDataLog.ReadItem nextItem = reader.getNext();
            if (nextItem == null) {
                Assert.assertFalse("Reader reached the end but there were still items to be read.", expectedIterator.hasNext());
                break;
            }

            Assert.assertTrue("Reader has more items but there should not be any more items to be read.", expectedIterator.hasNext());

            // Verify sequence number, as well as payload.
            val expected = expectedIterator.next();
            Assert.assertEquals("Unexpected sequence number.", expected.getKey().getSequence(), nextItem.getAddress().getSequence());
            val actualPayload = StreamHelpers.readAll(nextItem.getPayload(), nextItem.getLength());
            Assert.assertArrayEquals("Unexpected payload for sequence number " + expected.getKey(), expected.getValue(), actualPayload);
        }
    }


    @SneakyThrows
    @Test
    public void appendASingleItemThenRead() {
        try (DurableDataLog writeDataLog = this.factory.get().createDurableDataLog(CONTAINER_ID)) {
            // dataLog.enable();
            writeDataLog.initialize(TIMEOUT);

            CompletableFuture<LogAddress> addressFuture =
                    writeDataLog.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT);
            long sequenceNum = addressFuture.join().getSequence();
            assertTrue(sequenceNum > 1);
            log.debug("Append sequence number: {}", sequenceNum);
        }

        try(DurableDataLog readDataLog = this.factory.get().createDurableDataLog(CONTAINER_ID)) {
            readDataLog.initialize(TIMEOUT);
            CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = readDataLog.getReader();
            DurableDataLog.ReadItem readItem = null;
            int readCount = 0;
            while ((readItem = reader.getNext()) != null) {
                log.debug("Read an item");
                readCount++;
            }
            assertEquals("Unexpected number of entries/ledgers read.", 1, readCount);
        }
    }

    protected TreeMap<LogAddress, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        val data = new ArrayList<byte[]>();
        val futures = new ArrayList<CompletableFuture<LogAddress>>();
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = getWriteData();
            futures.add(log.append(new CompositeByteArraySegment(writeData), TIMEOUT));
            data.add(writeData);
        }

        val addresses = Futures.allOfWithResults(futures).join();
        for (int i = 0; i < data.size(); i++) {
            writtenData.put(addresses.get(i), data.get(i));
        }

        return writtenData;
    }

    private byte[] getWriteData() {
        int length = WRITE_MIN_LENGTH + random.nextInt(WRITE_MAX_LENGTH - WRITE_MIN_LENGTH);
        byte[] data = new byte[length];
        this.random.nextBytes(data);
        return data;
    }

    @SneakyThrows
    @AfterClass
    public static void tearDownEnv() {
        log.debug("tearDownEnv entry");
        if (BOOKIES_RUNNER != null) {
            BOOKIES_RUNNER.close();
        }
    }
}
