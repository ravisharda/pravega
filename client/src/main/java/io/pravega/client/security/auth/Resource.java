/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import io.pravega.common.Exceptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A utility class to represent a resource.
 */
@RequiredArgsConstructor
public class Resource {

    enum Type {
        KEY_VALUE_TABLE,
        READER_GROUP,
        STREAM
    }

    @Getter
    private final String scope;

    @Getter
    private final String name;

    @Getter
    private final Type type;

    public static Resource of(String scopeName, String streamName) {
        return Resource.of(scopeName, streamName, Type.STREAM);
    }

    public static Resource of(String scopeName, String resourceName, @NonNull Type resourceType) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(resourceName, "resourceName");
        return new Resource(scopeName, resourceName, resourceType);
    }
}
