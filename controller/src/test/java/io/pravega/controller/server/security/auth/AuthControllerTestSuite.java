/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth;

import io.pravega.controller.rest.v1.SecureStreamMetaDataTests;

import io.pravega.controller.rest.v1.UserSecureStreamMetaDataTests;
import io.pravega.controller.server.rpc.auth.PravegaAuthManagerTest;
// import io.pravega.controller.server.security.auth.handler.impl.AccessControlEntryTest;
import io.pravega.controller.server.security.auth.handler.impl.LegacyAclAuthorizerImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import io.pravega.controller.rest.v1.StreamMetaDataTests;
import io.pravega.controller.rest.v1.StreamMetaDataAuthFocusedTests;
import io.pravega.controller.server.rpc.auth.ControllerGrpcAuthFocusedTest;
import io.pravega.controller.server.rpc.auth.RESTAuthHelperTest;
import io.pravega.controller.server.security.auth.handler.impl.AclAuthorizerImplTest;
import io.pravega.controller.server.security.auth.handler.impl.PasswordAuthHandlerTest;
import io.pravega.controller.server.v1.InMemoryControllerServiceImplTest;
import io.pravega.controller.server.v1.ZKControllerServiceImplTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        StreamMetaDataAuthFocusedTests.class,
        ControllerGrpcAuthFocusedTest.class,
        StreamMetaDataTests.class,
        SecureStreamMetaDataTests.class,
        UserSecureStreamMetaDataTests.class,
        InMemoryControllerServiceImplTest.class,
        ZKControllerServiceImplTest.class,
        RESTAuthHelperTest.class,
        PravegaAuthManagerTest.class,
        AuthorizationResourceImplTest.class,
        LegacyAuthorizationResourceImplTest.class,
        PasswordAuthHandlerTest.class,
        AclAuthorizerImplTest.class,
        LegacyAclAuthorizerImplTest.class,
        // AccessControlEntryTest.class
})

public class AuthControllerTestSuite {

}
