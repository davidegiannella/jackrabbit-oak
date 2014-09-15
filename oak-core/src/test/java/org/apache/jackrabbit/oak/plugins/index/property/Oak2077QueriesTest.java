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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.junit.Assert.fail;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Test;

public class Oak2077QueriesTest extends BasicOrderedPropertyIndexQueryTest {
    private NodeStore nodestore;
    
    @Override
    protected ContentRepository createRepository() {
        nodestore = new MemoryNodeStore();
        return new Oak(nodestore).with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new OrderedPropertyIndexEditorProvider())
            .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        // leaving it empty. Prefer to create the index definition in each method
    }
    
    @Test @Ignore
    public void queryNotNullAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryNotNullDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryEqualsAscending() {
        fail();
    }

    @Test @Ignore
    public void queryEqualsDescending() {
        fail();
    }

    @Test @Ignore
    public void queryGreaterThanAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThenDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThanEqualAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThanEqualDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanEqualAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanEqualDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenWithBoundariesAscending() {
        fail();
    }

    @Test @Ignore
    public void queryBetweenWithBoundariesDescending() {
        fail();
    }
}
