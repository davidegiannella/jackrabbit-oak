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

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.jcr.NodeStoreFixture.SEGMENT_MK;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.apache.jackrabbit.oak.spi.commit.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.spi.commit.AtomicCounterEditor.PROP_INCREMENT;
import static org.junit.Assert.assertEquals;
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
    
    /**
     * used to ignore individual tests in case of running with multiple fixtures and only one (or
     * some) of those is desired
     */
    private static final Set<NodeStoreFixture> ALLOWED_FIXTURES = of(SEGMENT_MK);
    
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
        
        assumeTrue(ALLOWED_FIXTURES.contains(fixture));
        
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

            // increment again the same node
            node.getProperty(PROP_INCREMENT).remove();
            session.save();
            node.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(2, node.getProperty(PROP_COUNTER).getLong());

            // decrease the counter by 2
            node.getProperty(PROP_INCREMENT).remove();
            session.save();
            node.setProperty(PROP_INCREMENT, -2L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(0, node.getProperty(PROP_COUNTER).getLong());

            // increase by 5
            node.getProperty(PROP_INCREMENT).remove();
            session.save();
            node.setProperty(PROP_INCREMENT, 5L);
            session.save();
            assertTrue(node.hasProperty(PROP_COUNTER));
            assertEquals(5, node.getProperty(PROP_COUNTER).getLong());
        } finally {
            session.logout();
        }
    }
}
