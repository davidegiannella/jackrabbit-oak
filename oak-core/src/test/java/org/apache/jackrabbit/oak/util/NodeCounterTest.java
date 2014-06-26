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
package org.apache.jackrabbit.oak.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.util.NodeCounter.APPROX_MIN_RESOLUTION;
import static org.apache.jackrabbit.oak.util.NodeCounter.PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Deque;
import java.util.Random;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class NodeCounterTest {
    public static class MockRandom extends Random {
        private static final long serialVersionUID = -2051060190069357480L;
        
        /**
         * if passed into the {@link #setNextInt(int)} will instruct the class to use the original
         * {@code Random.nextInt(int)}
         */
        public static final int SUPER = Integer.MIN_VALUE;
        
        private int nextInt = SUPER;
        
        public MockRandom() {
            super(37);
        }
        
        /**
         * if previously set to {@link #SUPER} using the {@link #setNextInt(int)} it will work as
         * {@code Random.nextInt(int)} otherwise it will return the previous value set via
         * {@link #setNextInt(int)}
         */
        @Override
        public int nextInt(int n) {
            if (nextInt == SUPER) {
                return super.nextInt(n);
            } else {
                return nextInt;
            }
            
        }
        
        /**
         * set what value should be returned with future calls of {@link #nextInt(int)}. See
         * {@link #nextInt(int)} for details.
         * 
         * @param nextInt
         */
        public void setNextInt(int nextInt) {
            this.nextInt = nextInt;
        }
    }
    
    /**
     * used to mock the behaviour and being sure about the results. Refer to {@link MockRandom} for
     * details and reset according to needs at every use
     */
    private static final MockRandom RND = new MockRandom();
    
    @Test
    public void getApproxAdded() {
        NodeBuilder node; 
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        assertEquals("if the node has no properties for counting NO_PROPERTIES is expected",
            NodeCounter.NO_PROPERTIES, NodeCounter.getApproxAdded(node));
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(PREFIX + "1", APPROX_MIN_RESOLUTION, Type.LONG);
        assertEquals(APPROX_MIN_RESOLUTION, NodeCounter.getApproxAdded(node));
        node.setProperty(PREFIX + "2", APPROX_MIN_RESOLUTION, Type.LONG);
        assertEquals(2000L, NodeCounter.getApproxAdded(node));
        node.setProperty(PREFIX + "3", 2000L, Type.LONG);
        assertEquals(4000L, NodeCounter.getApproxAdded(node));
    }
    
    @Test
    public void getApproxRemoved() {
        NodeBuilder node; 
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        assertEquals("if the node has no properties for counting NO_PROPERTIES is expected",
            NodeCounter.NO_PROPERTIES, NodeCounter.getApproxRemoved(node));
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(PREFIX + "1", -APPROX_MIN_RESOLUTION, Type.LONG);
        assertEquals(APPROX_MIN_RESOLUTION, NodeCounter.getApproxRemoved(node));
        node.setProperty(PREFIX + "2", -APPROX_MIN_RESOLUTION, Type.LONG);
        assertEquals(2000L, NodeCounter.getApproxRemoved(node));
        node.setProperty(PREFIX + "3", -2000L, Type.LONG);
        assertEquals(4000L, NodeCounter.getApproxRemoved(node));
    }
    
    @Test
    public void nodeAdded() {
        NodeBuilder node;
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(1000L), PREFIX);

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        RND.setNextInt(1);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(1000L, 1000L), PREFIX);
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        RND.setNextInt(1);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeAdded(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(1000L, 1000L, 2000L, 4000L), PREFIX);
    }
    
    @Test
    public void nodeRemoved() {
        NodeBuilder node;
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(-1000L), PREFIX);

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        RND.setNextInt(1);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(-1000L, -1000L), PREFIX);
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        RND.setNextInt(1);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        node = NodeCounter.nodeRemoved(RND, node);
        assertNotNull(node);
        assertProperties(node, ImmutableList.of(-1000L, -1000L, -2000L, -4000L), PREFIX);
    }
    
    /**
     * assert the correctness of the properties in the provided node towards the expected values.
     *  
     * @param node
     * @param expected
     */
    private static void assertProperties(final NodeBuilder node, final Iterable<Long> expected,
                                         final String prefix) {
        
        checkNotNull(node);
        checkNotNull(expected);
        checkNotNull(prefix);
        
        Iterable<? extends PropertyState> properties = node.getProperties();
        Deque<Long> exp = Lists.newLinkedList(expected);
        // asserting the results
        assertFalse("We should have some properties for count", Iterables.isEmpty(properties));
        for (PropertyState p : properties) {
            if (p.getName().startsWith(prefix)) {
                long v = p.getValue(Type.LONG);
                if (exp.contains(v)) {
                    exp.remove(v);
                } else {
                    fail("A count has been found that was not expected: " + v);
                }
            }
        }
        assertTrue("by now we should have cleared all the expected values", exp.isEmpty());
        
    }
    
    @Test
    public void getApproxCount() {
        NodeBuilder node;
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        assertEquals("With no properties for estimation a NO_PROPERTIES is expected",
            NodeCounter.NO_PROPERTIES, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        assertEquals(APPROX_MIN_RESOLUTION, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeAdded(RND, node);
        assertEquals(2000L, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        assertEquals(APPROX_MIN_RESOLUTION, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        assertEquals(APPROX_MIN_RESOLUTION, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        assertEquals(3000L, NodeCounter.getApproxCount(node));
        
        // double-checking in case we have more removed than added
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        NodeCounter.nodeAdded(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        NodeCounter.nodeRemoved(RND, node);
        assertEquals(500L, NodeCounter.getApproxCount(node));

        // checking with a custom resolution
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        long resolution = 100L;
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        assertEquals(100L, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        resolution = 100L;
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        assertEquals(200L, NodeCounter.getApproxCount(node));

        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        resolution = 100L;
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeRemoved(RND, node, PREFIX, resolution);
        assertEquals(100L, NodeCounter.getApproxCount(node));
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        RND.setNextInt(0);
        resolution = 100L;
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeAdded(RND, node, PREFIX, resolution);
        NodeCounter.nodeRemoved(RND, node, PREFIX, resolution);
        assertEquals(300L, NodeCounter.getApproxCount(node));
    }
}
