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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.util.NodeCounter.PREFIX;
import static org.apache.jackrabbit.oak.util.NodeCounter.APPROX_MIN_RESOLUTION;

public class NodeCounterTest {
    public static class MockRandom extends Random {
        private static final long serialVersionUID = -2051060190069357480L;
        
        /**
         * if passed into the {@link #setNextInt(int)} will instruct the class to use the original
         * {@code Random.nextInt(int)}
         */
        public static final int SUPER = Integer.MIN_VALUE;
        
        private int nextInt = SUPER;
        
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
        
        public void setNextInt(int nextInt) {
            this.nextInt = nextInt;
        }
    }
    
    static final MockRandom RND = new MockRandom();
    
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
}
