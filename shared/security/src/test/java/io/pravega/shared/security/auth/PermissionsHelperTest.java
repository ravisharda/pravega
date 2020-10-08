/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.auth;

import io.pravega.auth.AuthHandler;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PermissionsHelperTest {

    @Test
    public void translatesValidPermissions() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.toAuthHandlerPermissions(AccessOperation.READ));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.toAuthHandlerPermissions(AccessOperation.WRITE));
    }

    @Test
    public void throwsExceptionForNotUnderstoodPermissions() {
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> PermissionsHelper.toAuthHandlerPermissions(AccessOperation.ANY));
    }

    @Test
    public void parsesEmptyPermissionStringToDefault() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.parse("", AuthHandler.Permissions.READ));
    }

    @Test
    public void parsesNonEmptyPermissionStrings() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.parse(AccessOperation.READ.toString(),
                AuthHandler.Permissions.NONE));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.parse(AccessOperation.WRITE.toString(),
                AuthHandler.Permissions.NONE));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.parse(AccessOperation.ANY.toString(),
                AuthHandler.Permissions.READ_UPDATE));
    }
}
