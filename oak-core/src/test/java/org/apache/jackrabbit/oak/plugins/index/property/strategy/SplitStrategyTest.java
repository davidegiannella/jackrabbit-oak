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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.Sets;

public class SplitStrategyTest {
    @Test
    public void insert() {
        SplitStrategy strategy = new SplitStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node;
        final String path = "/content/foo/bar";
        final String sha1Path = "acdd763534a786e0d21adb9d6c6b1565d5bd5211";
        Set<String> before = Sets.newHashSet();
        Set<String> after;
        
        after = Sets.newHashSet("app");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));
        
        after = Sets.newHashSet("app,les");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode("les");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));

        after = Sets.newHashSet("app,le,ts,:");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode("le");
        assertTrue(node.exists());
        node = node.getChildNode("ts");
        assertTrue(node.exists());
        node = node.getChildNode(":");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));
    }
}
