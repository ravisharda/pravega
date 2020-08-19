/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth.handler.impl;

import io.pravega.auth.AuthHandler;
import io.pravega.controller.server.security.auth.AuthorizationResource;
import io.pravega.controller.server.security.auth.LegacyAuthorizationResourceImpl;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class LegacyAclAuthorizerImplTest {
    private final static String DUMMY_ENCRYPTED_PWD = "Dummy encrypted value";
    private AuthorizationResource resource = new LegacyAuthorizationResourceImpl();

    @Test
    public void testSuperUserAcl() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("*", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();

        // Root resource
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScopes()));

        // Specific resources
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScope("testScope")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofStreamInScope("testScope", "testStream")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofReaderGroupInScope("testScope", "testRgt")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofKeyValueTableInScope("testScope", "testKvt")));
    }

    @Test
    public void testUserWithCreateScopeAccess() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("/", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScopes()));
    }

    @Test
    public void testUserWithAccessToASpecificScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("testscope", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScope("testscope")));
    }

    @Test
    public void testUserWithAccessToAllScopes() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("/*", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScope("testscope1")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, resource.ofScope("testscope2")));
    }

    @Test
    public void testUserWithAccessToAllDirectAndIndirectChildrenOfScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("abcscope/*", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();

        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl, resource.ofScope("abcscope")));
        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl, resource.ofScope("xyzscope")));

        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "stream123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "stream456")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofReaderGroupInScope("abcscope", "rg123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofKeyValueTableInScope("abcscope", "kvt123")));

        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl,
                resource.ofStreamInScope("xyzscope", "stream123")));
    }

    @Test
    public void testUserWithAccessToStreamPatternsInASpecificScope() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("abcscope/*", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "str123")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "streaMMMMMMMMMMM")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "str")));
    }

    @Test
    public void testUserWithAccessToStreamPatternsInAnyScopeDoesnotWork() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry("*/mystream", AuthHandler.Permissions.READ_UPDATE, true)
        ));

        LegacyAclAuthorizerImpl authorizer = new LegacyAclAuthorizerImpl();
        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl,
                resource.ofStreamInScope("abcscope", "mystream")));
        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl,
                resource.ofStreamInScope("mnoscope", "mystream")));
        assertEquals(AuthHandler.Permissions.NONE, authorizer.authorize(acl,
                resource.ofStreamInScope("xyzscope", "mystream")));
    }
}
