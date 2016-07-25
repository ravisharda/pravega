/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.serverhost.benchmark;

import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.service.ServiceBuilder;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Base class for a benchmark.
 */
public abstract class Benchmark {
    //region Members

    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    protected final Supplier<ServiceBuilder> serviceBuilderProvider;
    protected static final int ONE_MB = 1024 * 1024;
    protected static final int ONE_KB = 1024;

    //endregion

    //region Constructor

    protected Benchmark(Supplier<ServiceBuilder> serviceBuilderProvider) {
        Preconditions.checkNotNull(serviceBuilderProvider, "serviceBuilderProvider");
        this.serviceBuilderProvider = serviceBuilderProvider;
    }

    //endregion

    //region Abstract Members

    /**
     * Executes the benchmark.
     */
    public abstract void run();

    /**
     * Gets a value indicating the name of the test. Used for logging purposes.
     *
     * @return
     */
    protected abstract String getTestName();

    //endregion

    //region Helpers

    protected List<String> createStreamSegments(StreamSegmentStore store, int segmentCount) {
        List<CompletableFuture<Void>> results = new ArrayList<>();
        List<String> result = new ArrayList<>();

        for (int i = 0; i < segmentCount; i++) {
            String name = String.format("StreamSegment_%d", i);
            result.add(name);
            results.add(store.createStreamSegment(name, TIMEOUT));
        }

        FutureHelpers.allOf(results).join();
        return result;
    }

    protected void log(String messageTemplate, Object... args) {
        System.out.println(getTestName() + ": " + String.format(messageTemplate, args));
    }


    protected void printResultLine(Object... args) {
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof Double) {
                System.out.print(String.format("%.1f", (double)arg));
            } else {
                System.out.print(arg);
            }

            if (i < args.length - 1) {
                System.out.print(", ");
            }
        }
        System.out.println();
    }

    //endregion
}
