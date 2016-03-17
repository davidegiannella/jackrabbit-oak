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
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.api.Type.NAME;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
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
    public void oakIndexable() throws IOException, LoginException, NoSuchWorkspaceException, CommitFailedException {
        ContentRepository repository = null;
        Root root;
        ContentSession session = null;
        InputStream stream = null;
        
        try {
            repository = new Oak().with(new InitialContent()).with(new OpenSecurityProvider())
                .createContentRepository();
            session = repository.login(null, null);
            root = session.getLatestRoot();

            stream = NodeTypeRegistryTest.class.getResourceAsStream("oak3725-1.cnd");
            
            NodeTypeRegistry.register(root, stream, "oak3625-1");
            
            Tree test = root.getTree("/").addChild("test");
            test.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, NAME);
            root.commit();
        } finally {
            if (stream != null) {
                stream.close();
            }
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
