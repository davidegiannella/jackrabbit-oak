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
package org.apache.jackrabbit.oak.jcr;

import java.util.Set;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicCounterTest extends AbstractRepositoryTest {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    
    public AtomicCounterTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @BeforeClass
    public static void assumptions() {
        // run only on the below fixtures
        Assume.assumeTrue(FIXTURES.contains(Fixture.SEGMENT_MK));
    }
    
    @Test
    public void dumb() throws RepositoryException {
        Session session = getAdminSession();
        
        Node root = session.getRootNode();
        root.addNode("no");
        root.addNode("yes").addMixin(NodeTypeConstants.MIX_ATOMIC_COUNTER);
        
        // TODO complete this stub
    }
}
