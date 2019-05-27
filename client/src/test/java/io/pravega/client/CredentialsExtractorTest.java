/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import io.pravega.client.stream.impl.Credentials;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CredentialsExtractorTest {

    @Test
    public void extractsCredentialsFromProperties() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void explicitlySpecifiedCredentialsIsNotOverridden() {

        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new Credentials() {
                    @Override
                    public String getAuthenticationType() {
                        return "typeSpecifiedViaExplicitObject";
                    }

                    @Override
                    public String getAuthenticationToken() {
                        return "tokenSpecifiedViaExplicitObject";
                    }
                }).extractCredentials(properties, null)
                .build();

        assertEquals("Explicitly set credentials should not be overridden", "typeSpecifiedViaExplicitObject",
                clientConfig.getCredentials().getAuthenticationType());

        assertEquals("Explicitly set credentials should not be overridden", "tokenSpecifiedViaExplicitObject",
                clientConfig.getCredentials().getAuthenticationToken());
    }

    @Test
    public void loadsAGenericCredentialsObjectFromPropertiesIfLoadDynamicIsFalse() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "false");
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void doesNotLoadCredentialsOfNonExistentClassIfLoadDynamicIsTrue() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "true");
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();

        // Expecting a null because there is no Credentials implementation in the classpath that registers an
        // authentication type "amethod".
        assertNull(clientConfig.getCredentials());
    }

    @Test
    public void loadsCredentialsOfExistentClassIfLoadDynamicIsTrue() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "true");
        properties.setProperty("pravega.client.auth.method", "Bearer");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull("Credentials is null", credentials);
        assertNotNull(DynamicallyLoadedCreds.class.getName(), credentials.getClass());
        assertEquals("Expected a different authentication type", "Bearer",
                credentials.getAuthenticationType());
    }

    /**
     * A class representing Credentials. It is dynamically loaded using a {@link java.util.ServiceLoader} by
     * the code under test, in the enclosing test class. For ServiceLoader to find it, it is configured in
     * META-INF/services/io.pravega.client.stream.impl.Credentials.
     */
    public static class DynamicallyLoadedCreds implements Credentials {

        @Override
        public String getAuthenticationType() {
            return "Bearer";
        }

        @Override
        public String getAuthenticationToken() {
            return "SomeToken";
        }
    }

    public static class DynamicallyLoadedCredsSecond implements Credentials {

        @Override
        public String getAuthenticationType() {
            return "DynamicallyLoadedCredsSecond";
        }

        @Override
        public String getAuthenticationToken() {
            return "DynamicallyLoadedCredsSecond";
        }
    }
}