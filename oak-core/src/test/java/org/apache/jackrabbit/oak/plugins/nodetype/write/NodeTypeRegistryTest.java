/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

public class NodeTypeRegistryTest {
    
    static void registerNodeType(@Nonnull String resourceName) {
        checkArgument(!Strings.isNullOrEmpty(resourceName));
        
    }
    
    @Before
    public void setUp() throws LoginException, NoSuchWorkspaceException {
    }
    
    @After
    public void tearDown() throws IOException {
    }
    
    @Test
    public void oakIndexable() throws IOException, LoginException, NoSuchWorkspaceException {
        ContentRepository repository = null;
        Root root;
        ContentSession session = null;

        try {
            repository = new Oak().with(new InitialContent())
                .createContentRepository();
            session = repository.login(null, null);
            root = session.getLatestRoot();

            InputStream stream = NodeTypeRegistryTest.class.getResourceAsStream("oak3725-1.cnd");
            
            System.out.println(stream);

        } finally {
            if (session != null) {
                session.close();
            }
            if (repository instanceof Closeable) {
                ((Closeable) repository).close();
            }
            repository = null;
        }

    }
}
