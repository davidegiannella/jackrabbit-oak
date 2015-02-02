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

import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Set;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicCounterTest extends AbstractRepositoryTest {
    /**
     * set of fixtures provided by the environment
     */
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
        
    public AtomicCounterTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @BeforeClass
    public static void assumptions() {
        // run only on the below fixtures
        assumeTrue(FIXTURES.contains(Fixture.SEGMENT_MK));
    }
    
    @Test
    public void increment() throws RepositoryException {
        
        assumeTrue(NodeStoreFixture.SEGMENT_MK.equals(fixture));
        
        Session session = getAdminSession();

        try {
            Node root = session.getRootNode();
            Node node = root.addNode("normal node");
            session.save();

            node.setProperty(PROP_INCREMENT, 1L);
            session.save();

            assertTrue("for normal nodes we expect the increment property to be treated as normal",
                node.hasProperty(PROP_INCREMENT));

            node = root.addNode("counterNode");
            node.addMixin(MIX_ATOMIC_COUNTER);
            session.save();

            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(0, node.getProperty(PROP_COUNTER).getLong());

            node.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(1, node.getProperty(PROP_COUNTER).getLong());
            assertFalse(node.hasProperty(PROP_INCREMENT));

            // increment again the same node
            node.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(2, node.getProperty(PROP_COUNTER).getLong());
            assertFalse(node.hasProperty(PROP_INCREMENT));

            // decrease the counter by 2
            node.setProperty(PROP_INCREMENT, -2L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(0, node.getProperty(PROP_COUNTER).getLong());
            assertFalse(node.hasProperty(PROP_INCREMENT));

            // increase by 5
            node.setProperty(PROP_INCREMENT, 5L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(5, node.getProperty(PROP_COUNTER).getLong());
            assertFalse(node.hasProperty(PROP_INCREMENT));
        } finally {
            session.logout();
        }
    }
    
    // FIXME add test case on non root node
}
